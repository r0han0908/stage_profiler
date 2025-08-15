#!/usr/bin/env python3
"""
Knative cold-start stage profiler (+ wake-from-zero + revision-aware naming)
----------------------------------------------------------------------------
• Profiles every new pod (robust LIST + WATCH).
• Derives all seven stages from the Kubernetes API:
    1) stage1-scheduling      (PodScheduled True)
    2) stage2-image-pull      (first containers Running; uses latest started_at across containers)
    3) stage3-container-init  (ContainersReady/Ready=True)
    4) stage4-proxy-warmup    (HTTP polling until first 200 OK)
    5) stage5-runtime-startup (queue-proxy container Ready)
    6) stage6-app-init        (Ready→first 200 OK window)
    7) stage7-first-request   (timing of the first measured request)
• If API-server closes the stream, it reconnects.
• If there are zero pods and wake-from-zero is enabled, it sends requests to `--url`
  (or http://<ksvc>.<ns>.svc.cluster.local) to trigger scale from zero, then profiles.

Structured logs:
  - logs/stage_timings.ndjson   (append-only history; one JSON per coldstart/reprobe)
  - logs/latest_and_best.json   ({"current": ..., "best_overall": ...})

Run naming / revision-aware / interval reset:
• Each profiling run gets a human label: "profiling-<n>".
• If the run is the first one for a newly detected Knative Revision, it is labeled "revisioned-<n>".
• When a new Revision is detected, the periodic cold-start trigger schedule is reset immediately.
"""

import os, json, logging, argparse, threading, time, pathlib, random
from collections import OrderedDict
from datetime import datetime, timezone
from urllib.parse import urlparse, urlencode
from typing import Optional, Tuple, Dict

import requests
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# ────────────  Logging  ────────────
level = os.getenv("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, level, logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("stage-profiler")

def _utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
def _ms(sec: float) -> str:
    return f"{sec*1e3:.2f} ms"
def _iso(dt: datetime) -> str:
    return _utc(dt).isoformat()

def _sanitize(endpoint: str) -> str:
    endpoint = (endpoint or "").rstrip("/v1/traces").rstrip("/v1/metrics").strip()
    return urlparse(endpoint).netloc if endpoint.startswith(("http://", "https://")) else endpoint

# ────── OpenTelemetry for profiler’s own spans ──────
collector_ep = _sanitize(os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") or
                         os.getenv("OTEL_COLLECTOR_ENDPOINT") or
                         "localhost:4317")
tp = TracerProvider(resource=Resource({"service.name": "stage-profiler"}))
tp.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=collector_ep, insecure=True)))
trace.set_tracer_provider(tp)
tracer = trace.get_tracer(__name__)

# ────────────  Global state  ────────────
pod_state: Dict[str, dict] = {}
pod_timings: Dict[str, dict] = {}
pod_lock = threading.Lock()
ready_event = threading.Event()
ready_uid: Optional[str] = None
processed = set()

service_by_uid: Dict[str, str] = {}
pod_name_by_uid: Dict[str, str] = {}
namespace_by_uid: Dict[str, str] = {}
revision_by_uid: Dict[str, str] = {}

# Run naming + revision tracking
run_lock = threading.Lock()
run_counter = 0
rev_run_counter = 0
run_name_by_uid: Dict[str, str] = {}
known_revisions: set = set()
revision_profiled_once: set = set()   # only the first profile for a new revision is "revisioned-*"
last_revision_seen: Optional[str] = None

# Periodic scheduler control (resettable on new revision)
periodic_lock = threading.Lock()
periodic_next_at = 0.0

# Single-flight guard for wake attempts (prevents interleaved wake hits)
wake_sf_lock = threading.Lock()

STAGES = OrderedDict([
    ("stage1-scheduling",      "Pod Scheduling"),
    ("stage2-image-pull",      "Image Pull"),
    ("stage3-container-init",  "Container Init"),
    ("stage5-runtime-startup", "Runtime Startup"),
    ("stage4-proxy-warmup",    "Proxy Warm-up"),
    ("stage6-app-init",        "App Init"),
    ("stage7-first-request",   "First Request"),
])

v1: client.CoreV1Api  # set later

# ────────────  File outputs  ────────────
HISTORY_LOG = None          # logs/stage_timings.ndjson
BEST_CURRENT_JSON = None    # logs/latest_and_best.json

def _ensure_logs_dir(logs_dir: str):
    path = pathlib.Path(logs_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path

def _write_line(path: str, obj: dict):
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception as e:
        log.warning("Failed writing %s: %s", path, e)

def _write_json(path: str, obj: dict):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning("Failed writing %s: %s", path, e)

_best_lock = threading.Lock()
_best_overall = None  # cached best by lowest total_ms (coldstarts only)

def _update_latest_and_best(current_record: dict):
    global _best_overall
    with _best_lock:
        cur_total = current_record.get("total_ms", float("inf"))
        if (current_record.get("kind") == "coldstart" and
            (_best_overall is None or cur_total < _best_overall.get("total_ms", float("inf")))):
            _best_overall = current_record
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "current": current_record,
            "best_overall": _best_overall,
        }
        _write_json(BEST_CURRENT_JSON, payload)

# ────────────  Span + timing helpers  ────────────
def emit_span(stage: str, start: datetime, end: datetime, uid: str):
    dur_ms = (end - start).total_seconds()*1000
    with pod_lock:
        pod_timings.setdefault(uid, {})[stage] = (start, end, dur_ms)
    span = tracer.start_span(stage, start_time=int(_utc(start).timestamp()*1e9))
    svc = service_by_uid.get(uid, "unknown")
    ns  = namespace_by_uid.get(uid, "unknown")
    rev = revision_by_uid.get(uid, "unknown")
    span.set_attribute("k8s.namespace.name", ns)
    span.set_attribute("knative.service",     svc)
    span.set_attribute("knative.revision",    rev)
    span.set_attribute("prof.stage_id",       stage)
    span.end(end_time=int(_utc(end).timestamp()*1e9))
    log.info("%s [%s]: %s", STAGES.get(stage, stage), uid[:6], _ms(dur_ms/1000))

def print_table(uid: str):
    timings = pod_timings.get(uid)
    if not timings:
        return
    svc = service_by_uid.get(uid, "unknown")
    total = (timings["stage7-first-request"][1] - timings["stage1-scheduling"][0]).total_seconds()*1000
    run_name = run_name_by_uid.get(uid, "profiling-0")
    print("\n\u250f" + "━"*70 + "┓")
    print(f"┃ {'COLD START PROFILE ' + '(' + run_name + ')':^68} ┃")
    print(f"┃ Pod {uid[:8]}  Service {svc:<20} {'':>24} ┃")
    print("┣" + "━"*70 + "┫")
    print(f"┃ {'Stage':^30} │ {'Duration':^18} │ {'% Total':^15} ┃")
    print("┣" + "━"*30 + "╋" + "━"*18 + "╋" + "━"*15 + "┫")
    for sid, label in STAGES.items():
        if sid in timings:
            _, _, ms = timings[sid]
            pct = ms/total*100 if total else 0
            print(f"┃ {label:30} │ {_ms(ms/1000):18} │ {pct:13.1f}% ┃")
    print("┗" + "━"*70 + "┛\n")

# ───────  Stage-5 monitor (queue-proxy)  ───────
def monitor_qproxy(ns: str, pod_name: str, uid: str, sched_ts: datetime):
    # quick pre-check
    try:
        pod = v1.read_namespaced_pod(pod_name, ns)
        for cs in (pod.status.container_statuses or []):
            if cs.name == "queue-proxy" and cs.ready:
                emit_span("stage5-runtime-startup", sched_ts, datetime.now(timezone.utc), uid)
                return
    except Exception:
        pass
    for _ in range(120):
        try:
            pod = v1.read_namespaced_pod(pod_name, ns)
            for cs in (pod.status.container_statuses or []):
                if cs.name == "queue-proxy" and cs.ready:
                    emit_span("stage5-runtime-startup", sched_ts, datetime.now(timezone.utc), uid)
                    return
        except Exception:
            pass
        time.sleep(1)
    log.warning("queue-proxy never Ready for %s; skipping stage5", uid[:6])

# ───────  Pod event processing  ───────
def _svc_name(pod) -> Optional[str]:
    lbl = pod.metadata.labels or {}
    return (lbl.get("serving.knative.dev/service")
            or lbl.get("app.kubernetes.io/name")
            or lbl.get("app"))

def _rev_name(pod) -> Optional[str]:
    lbl = pod.metadata.labels or {}
    return lbl.get("serving.knative.dev/revision")

def process_pod(pod):
    global ready_uid, last_revision_seen
    uid, name, ns = pod.metadata.uid, pod.metadata.name, pod.metadata.namespace
    if uid in processed:
        return

    pod_name_by_uid[uid] = name
    namespace_by_uid[uid] = ns
    if (rev := _rev_name(pod)):
        revision_by_uid[uid] = rev
        # detect first time we see this revision
        if rev not in known_revisions:
            known_revisions.add(rev)
            last_revision_seen = rev
            log.info("[revision] New revision detected: %s", rev)
            with periodic_lock:
                global periodic_next_at
                periodic_next_at = time.time()  # force periodic trigger schedule reset

    log.info("Saw pod %s in %s (phase=%s)", name, ns, pod.status.phase)

    st = pod_state.setdefault(uid, {"namespace": ns, "name": name})
    st.setdefault("submit", pod.metadata.creation_timestamp)

    # Stage 1: PodScheduled -> True
    for c in (pod.status.conditions or []):
        if c.type == "PodScheduled" and c.status == "True" and "scheduled" not in st:
            st["scheduled"] = c.last_transition_time
            emit_span("stage1-scheduling", st["submit"], c.last_transition_time, uid)
            if (svc := _svc_name(pod)):
                service_by_uid[uid] = svc

    # Stage 2: choose latest started_at across containers
    if "scheduled" in st and "running" not in st:
        latest = None
        for cs in (pod.status.container_statuses or []):
            if cs.state and cs.state.running and cs.state.running.started_at:
                ts = cs.state.running.started_at
                latest = ts if (latest is None or ts > latest) else latest
        if latest:
            st["running"] = latest
            emit_span("stage2-image-pull", st["scheduled"], st["running"], uid)

    # Stage 3: Pod Ready / ContainersReady
    for c in (pod.status.conditions or []):
        if c.type in ("Ready", "ContainersReady") and c.status == "True":
            if "ready" not in st and "running" in st:
                st["ready"] = c.last_transition_time
                emit_span("stage3-container-init", st["running"], c.last_transition_time, uid)
                threading.Thread(target=monitor_qproxy,
                                 args=(ns, name, uid, st["scheduled"]),
                                 daemon=True).start()
                ready_uid = uid
                ready_event.set()
            break

# ───────  Kubernetes watch loop (robust)  ───────
def setup_k8s():
    global v1
    try:
        config.load_incluster_config()
        log.info("Loaded in-cluster config")
    except Exception:
        config.load_kube_config()
        log.info("Loaded kubeconfig")
    v1 = client.CoreV1Api()

def list_matching_pods(ns: str, selector: str):
    return v1.list_namespaced_pod(ns, label_selector=selector or "")

def watch_pods(ns: str, selector: str):
    resp = list_matching_pods(ns, selector)
    log.info("Initial list: %d pod(s) matched selector '%s'", len(resp.items), selector or "<all>")
    for p in resp.items:
        process_pod(p)
    rv = resp.metadata.resource_version
    # WATCH
    while True:
        w = watch.Watch()
        try:
            for ev in w.stream(
                v1.list_namespaced_pod,
                namespace=ns,
                label_selector=selector or "",
                resource_version=rv,
                timeout_seconds=300,
                allow_watch_bookmarks=True,
            ):
                et = ev.get("type")
                obj = ev.get("object")
                # Guard against Status/timeouts that are dicts or lack .metadata
                if not hasattr(obj, "metadata"):
                    continue
                if et == "BOOKMARK":
                    rv = obj.metadata.resource_version
                    continue
                rv = obj.metadata.resource_version
                if et in ("ADDED", "MODIFIED"):
                    process_pod(obj)
        except Exception as exc:
            log.warning("pod watch closed (%s) – reconnect in 2 s", exc)
            time.sleep(2)
        finally:
            w.stop()

# ───────  Probe helpers (Stages 4/6/7 + wake)  ───────
def probe(url: str, uid: str, ready_ts: datetime, kind: str = "coldstart"):
    # Stage 4 proxy warm-up (poll service until first 200)
    t0 = datetime.now(timezone.utc)
    for _ in range(60):
        try:
            if requests.get(url, timeout=2).status_code == 200:
                break
        except requests.RequestException:
            pass
        time.sleep(0.5)
    t4_end = datetime.now(timezone.utc)
    emit_span("stage4-proxy-warmup", t0, t4_end, uid)
    emit_span("stage6-app-init", ready_ts, t4_end, uid)

    # Stage 7 first-request timing (one more request)
    t7 = datetime.now(timezone.utc)
    try:
        status = requests.get(url, timeout=5).status_code
    except requests.RequestException:
        status = -1
    emit_span("stage7-first-request", t7, datetime.now(timezone.utc), uid)
    log.info("[%s] First request HTTP status: %s", kind, status)

def _extract_knative_service_from_selector(selector: str) -> Optional[str]:
    if not selector:
        return None
    parts = [p.strip() for p in selector.split(",")]
    for p in parts:
        if p.startswith("serving.knative.dev/service="):
            return p.split("=", 1)[1]
    return None

def _build_probe_url(ns: str, selector: str, explicit_url: Optional[str]) -> Optional[str]:
    if explicit_url:
        return explicit_url
    svc = _extract_knative_service_from_selector(selector)
    if svc:
        return f"http://{svc}.{ns}.svc.cluster.local"
    return None

# ★ Single-flight wake with early cancel
def wake_service(url: Optional[str], retries: int, interval_s: int,
                 ns: Optional[str] = None, selector: Optional[str] = None):
    """
    Send a few HTTP GETs to trigger scale-from-zero, but:
    - single-flight: only one thread wakes at a time
    - cancel early: stop as soon as any matching pod appears
    """
    if not url:
        log.info("[wake] no URL to wake")
        return

    if not wake_sf_lock.acquire(blocking=False):
        log.debug("[wake] wake already in progress; skipping")
        return

    def pods_exist() -> bool:
        if not (ns and selector):
            return False
        try:
            return len(list_matching_pods(ns, selector).items) > 0
        except Exception:
            return False

    try:
        for i in range(max(1, retries)):
            if pods_exist():
                log.info("[wake] cancel: pod(s) detected; stopping further wake attempts")
                break

            try:
                q = {"wake": int(time.time()), "r": random.randint(0, 1_000_000)}
                sep = "&" if "?" in url else "?"
                target = f"{url}{sep}{urlencode(q)}"
                log.info("[wake] hitting %s (attempt %d/%d)", target, i+1, retries)
                requests.get(target, timeout=5)
            except requests.RequestException:
                pass

            if i < retries - 1:
                deadline = time.time() + interval_s
                while time.time() < deadline:
                    if pods_exist():
                        log.info("[wake] cancel during wait: pod(s) detected")
                        return
                    time.sleep(0.2)
    finally:
        wake_sf_lock.release()

# ───────  Periodic cold-start trigger (delete a pod or wake if zero)  ───────
def periodic_coldstart(ns: str, selector: str, period_sec: int, wake_url: Optional[str],
                       wake_retries: int, wake_interval_s: int, jitter_sec: int = 10):
    if period_sec <= 0:
        return
    with periodic_lock:
        global periodic_next_at
        periodic_next_at = time.time() + min(5, period_sec)

    while True:
        now = time.time()
        with periodic_lock:
            due = now >= periodic_next_at
        if due:
            try:
                pods = list_matching_pods(ns, selector)
                candidates = [p for p in pods.items if (p.status.phase in ("Running", "Pending"))]
                if not candidates:
                    log.info("[periodic] no pods to delete; likely scaled to zero — sending wake")
                    wake_service(wake_url, retries=wake_retries, interval_s=wake_interval_s, ns=ns, selector=selector)
                else:
                    victim = random.choice(candidates)
                    log.info("[periodic] Deleting pod %s to force cold start …", victim.metadata.name)
                    try:
                        v1.delete_namespaced_pod(victim.metadata.name, ns, grace_period_seconds=0)
                    except ApiException as e:
                        log.warning("[periodic] Failed to delete pod %s: %s", victim.metadata.name, e)
            except Exception as e:
                log.warning("[periodic] Error during periodic cold start: %s", e)

            with periodic_lock:
                jitter = random.randint(0, max(1, jitter_sec))
                periodic_next_at = time.time() + period_sec + jitter
        time.sleep(1)

# ───────  Zero-watcher: wake when there are zero pods  ───────
def zero_watcher(ns: str, selector: str, wake_url: Optional[str],
                 wake_enabled: bool, wake_interval_s: int, wake_retries: int):
    if not wake_enabled:
        return
    time.sleep(3)
    while True:
        try:
            pods = list_matching_pods(ns, selector)
            if len(pods.items) == 0:
                log.info("[zero] 0 pods for selector='%s' — sending wake", selector)
                wake_service(wake_url, retries=wake_retries, interval_s=wake_interval_s, ns=ns, selector=selector)
        except Exception as e:
            log.warning("[zero] error checking pod count: %s", e)
        time.sleep(wake_interval_s)

# ───────  JSON serialization ───────
def _serialize_record(uid: str, kind: str = "coldstart") -> dict:
    with pod_lock:
        timings = pod_timings.get(uid, {}).copy()
        name = pod_state.get(uid, {}).get("name", pod_name_by_uid.get(uid))
        ns = pod_state.get(uid, {}).get("namespace", namespace_by_uid.get(uid))
        svc = service_by_uid.get(uid, "unknown")
        rev = revision_by_uid.get(uid, "unknown")

    if not timings:
        return {}

    try:
        total_ms = (timings["stage7-first-request"][1] - timings["stage1-scheduling"][0]).total_seconds()*1000
    except Exception:
        total_ms = sum(v[2] for v in timings.values())

    stages = {}
    for sid, label in STAGES.items():
        if sid in timings:
            start, end, ms = timings[sid]
            stages[sid] = {
                "label": label,
                "start": _iso(start),
                "end": _iso(end),
                "duration_ms": round(ms, 3),
            }

    run_label = run_name_by_uid.get(uid, "profiling-0")

    return {
        "kind": kind,  # "coldstart" or "reprobe"
        "run_name": run_label,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "namespace": ns,
        "service": svc,
        "revision": rev,
        "pod_uid": uid,
        "pod_name": name,
        "total_ms": round(total_ms, 3),
        "stages": stages,
    }

# Run-name assignment helpers
def assign_run_name(uid: str):
    """Assign profiling-N by default; if this uid's revision is new and unprofiled → revisioned-N."""
    global run_counter, rev_run_counter
    rev = revision_by_uid.get(uid)
    is_revision_event = (rev is not None and rev in known_revisions and rev not in revision_profiled_once)
    with run_lock:
        if is_revision_event:
            rev_run_counter += 1
            run_name_by_uid[uid] = f"revisioned-{rev_run_counter}"
            revision_profiled_once.add(rev)
        else:
            run_counter += 1
            run_name_by_uid[uid] = f"profiling-{run_counter}"

# ───────  main  ───────
def main():
    ap = argparse.ArgumentParser("Cold-start profiler (every pod + periodic cold starts + wake-from-zero)")
    ap.add_argument("--namespace", required=True, help="Namespace to watch (e.g., default)")
    ap.add_argument("--selector", required=True, help="Label selector (e.g., 'serving.knative.dev/service=httpbin')")
    ap.add_argument("--url", required=False, help="Route URL to probe/wake; if omitted, tries 'http://<ksvc>.<ns>.svc.cluster.local'")
    ap.add_argument("--logs-dir", default="logs", help="Directory to write logs (default: ./logs)")
    ap.add_argument("--reprobe-every", type=int, default=0,
                    help="If >0, periodically re-probe the same URL (steady-state) and log entries as kind='reprobe' every N seconds (NOT a cold start).")
    # Defaults requested:
    ap.add_argument("--periodic-coldstart-every-minutes", type=int, default=20,
                    help="Every N minutes delete a current pod; if none, wake the service to trigger a new cold start. (default: 20)")
    ap.add_argument("--wake-when-zero", dest="wake_when_zero", action="store_true", default=True,
                    help="Send requests to wake the service when no pods are present (default: on)")
    ap.add_argument("--no-wake-when-zero", dest="wake_when_zero", action="store_false",
                    help="Disable wake-from-zero behavior")
    ap.add_argument("--wake-interval-seconds", type=int, default=20*60,
                    help="Seconds between wake attempts/checks when zero pods are present (default: 1200 = 20 minutes)")
    ap.add_argument("--wake-retries", type=int, default=3,
                    help="Number of wake HTTP attempts per wake cycle (default: 3)")
    args = ap.parse_args()

    # Prepare file paths
    logs_path = _ensure_logs_dir(args.logs_dir)
    global HISTORY_LOG, BEST_CURRENT_JSON
    HISTORY_LOG = str(logs_path / "stage_timings.ndjson")
    BEST_CURRENT_JSON = str(logs_path / "latest_and_best.json")

    # One-time client initialization
    setup_k8s()

    # Compute a wake/probe URL usable even when scaled to zero
    wake_probe_url = _build_probe_url(args.namespace, args.selector, args.url)
    if wake_probe_url:
        log.info("Wake/Probe URL: %s", wake_probe_url)
    else:
        log.warning("No wake/probe URL could be derived. Provide --url or include serving.knative.dev/service=<name> in --selector.")

    # Start the watch thread (always running; if no pods, we just wait)
    threading.Thread(target=watch_pods, args=(args.namespace, args.selector), daemon=True).start()

    # Start the periodic cold-start thread (delete or wake)
    period_sec = max(0, args.periodic_coldstart_every_minutes) * 60
    if period_sec > 0:
        threading.Thread(target=periodic_coldstart,
                         args=(args.namespace, args.selector, period_sec, wake_probe_url,
                               args.wake_retries, args.wake_interval_seconds),
                         daemon=True).start()
        log.info("Periodic cold-start enabled: every %d minutes", args.periodic_coldstart_every_minutes)
    else:
        log.info("Periodic cold-start disabled")

    # Start zero watcher (wake when 0 pods)
    threading.Thread(target=zero_watcher,
                     args=(args.namespace, args.selector, wake_probe_url,
                           args.wake_when_zero, args.wake_interval_seconds, args.wake_retries),
                     daemon=True).start()

    log.info("Profiler running in ns=%s selector='%s' (URL=%s)", args.namespace, args.selector, wake_probe_url or "<none>")

    # Optional steady-state re-probe loop state
    next_reprobe_at = 0.0
    last_probe_target: Optional[Tuple[str, str, datetime]] = None  # (url, uid, ready_ts)

    while True:
        # If a pod just became Ready, finish profiling it (coldstart)
        if ready_event.wait(timeout=1.0):
            uid = None
            try:
                uid = ready_uid
            finally:
                ready_event.clear()
            if uid and uid not in processed:
                processed.add(uid)

                st = pod_state.get(uid, {})
                ready_ts = st.get("ready") or st.get("scheduled") or datetime.now(timezone.utc)

                # Run naming (profiling-N vs revisioned-N)
                assign_run_name(uid)

                probe_url = wake_probe_url
                if probe_url:
                    try:
                        probe(probe_url, uid, ready_ts, kind="coldstart")
                    except Exception as e:
                        log.warning("Probe failed for %s: %s", uid[:6], e)

                # Wait up to 120s for stage 5 to be recorded (queue-proxy ready)
                deadline = time.time() + 120
                while ("stage5-runtime-startup" not in pod_timings.get(uid, {}) and time.time() < deadline):
                    time.sleep(0.5)

                print_table(uid)

                # Persist results — history + latest/best
                record = _serialize_record(uid, kind="coldstart")
                if record:
                    _write_line(HISTORY_LOG, record)
                    _update_latest_and_best(record)

                # Enable re-probe loop if wanted
                if args.reprobe_every > 0 and probe_url:
                    last_probe_target = (probe_url, uid, ready_ts)
                    next_reprobe_at = time.time() + args.reprobe_every

                log.info("Done with pod %s — waiting for next Ready pod …", uid[:6])

        # Steady-state re-probe (optional, NOT a cold start)
        if args.reprobe_every > 0 and last_probe_target and time.time() >= next_reprobe_at:
            probe_url, uid, ready_ts = last_probe_target
            alias_uid = f"{uid}-reprobe-{int(time.time())}"
            with pod_lock:
                pod_timings[alias_uid] = {}
                service_by_uid[alias_uid] = service_by_uid.get(uid, "unknown")
                namespace_by_uid[alias_uid] = namespace_by_uid.get(uid, "unknown")
                revision_by_uid[alias_uid] = revision_by_uid.get(uid, "unknown")
                pod_state[alias_uid] = pod_state.get(uid, {}).copy()

            # assign a profiling run name for reprobe as well (non-revisioned)
            with run_lock:
                global run_counter
                run_counter += 1
                run_name_by_uid[alias_uid] = f"profiling-{run_counter}"

            try:
                probe(probe_url, alias_uid, ready_ts, kind="reprobe")
            except Exception as e:
                log.warning("Re-probe failed: %s", e)

            record = _serialize_record(alias_uid, kind="reprobe")
            if record:
                _write_line(HISTORY_LOG, record)
                _update_latest_and_best(record)

            next_reprobe_at = time.time() + args.reprobe_every

if __name__ == "__main__":
    main()
