#!/usr/bin/env python3
"""
Knative cold-start stage profiler (15-min monitoring + wake-if-zero + revision-aware)
------------------------------------------------------------------------------------
• Profiles every new pod (robust LIST + WATCH).
• Derives seven stages from the Kubernetes API:
    1) stage1-scheduling      (PodScheduled=True)
    2) stage2-image-pull      (containers Running; latest started_at across containers)
    3) stage3-container-init  (ContainersReady/Ready=True)
    4) stage4-proxy-warmup    (HTTP polling until first 200 OK; requires --url or derivable URL)
    5) stage5-runtime-startup (queue-proxy container Ready)
    6) stage6-app-init        (Ready→first 200 OK window)
    7) stage7-first-request   (timing of the first measured request)
• If API-server closes the stream, it reconnects.

Behavior (exact per your spec):
- Every 15 minutes (fixed), run a monitoring cycle:
    * If the service currently has 0 pods **and minScale==0**, send a few HTTP requests to the
      service URL to trigger scale-from-zero. The watch thread then profiles that cold start.
    * If pods exist, perform a warm **reprobe** (no deletes) and log as kind="reprobe".
- Detects a **new Knative Revision** (label serving.knative.dev/revision); the first profile for a new
  revision is named 'revisioned-<n>' and **resets the 15-min timer** right after that run.
- No pod deletions. No continuous zero watcher — only the 15-min cadence.

Structured logs:
  - logs/stage_timings.ndjson   (append-only history; one JSON per run)
  - logs/latest_and_previous.json   ({"baseline": ..., "latest": ..., "previous": ...})

Usage:
  python stage_profiler.py --namespace default \
    --selector 'serving.knative.dev/service=httpbin' \
    --url http://httpbin.default.svc.cluster.local
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

# ────────────  Config  ────────────
REPROBE_EVERY_SEC = 15 * 60  # fixed 15 minutes

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
revision_profiled_once: set = set()   # first profile per revision → "revisioned-*"

# Periodic monitoring control
periodic_lock = threading.Lock()
periodic_next_at = 0.0
last_probe_target: Optional[Tuple[str, str, datetime]] = None  # (url, uid, ready_ts)

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
coapi: client.CustomObjectsApi  # set later

# ────────────  File outputs  ────────────
HISTORY_LOG = None                 # logs/stage_timings.ndjson
LATEST_PREVIOUS_JSON = None        # logs/latest_and_previous.json

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

# latest & previous tracker (+ baseline)
_lp_lock = threading.Lock()
_previous_record = None  # last run we wrote (any kind)
_baseline_record = None  # very first run ever (persisted)

def _update_latest_and_previous(current_record: dict):
    """
    Persist a small JSON doc keeping:
      - baseline: the very first recorded run ever (sticky across restarts)
      - latest: the current run
      - previous: the run immediately before the current one
    {
      "updated_at": "...",
      "baseline": {...first_record...},
      "latest":   {...current_record...},
      "previous": {...previous_record...} | None
    }
    """
    global _previous_record, _baseline_record

    with _lp_lock:
        # Bootstrap baseline/previous from existing file if present (for persistence across restarts)
        if _baseline_record is None:
            try:
                with open(LATEST_PREVIOUS_JSON, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                    _baseline_record = existing.get("baseline")
                    if _previous_record is None:
                        _previous_record = existing.get("latest")
            except Exception:
                # File may not exist yet or be unreadable; we'll set baseline below
                pass

        # If still no baseline, this is the very first record — set it now.
        if _baseline_record is None:
            _baseline_record = current_record

        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "baseline": _baseline_record,
            "latest": current_record,
            "previous": _previous_record,
        }

        _write_json(LATEST_PREVIOUS_JSON, payload)
        _previous_record = current_record

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
    total = (timings["stage7-first-request"][1] - timings["stage1-scheduling"][0]).total_seconds()*1000 \
            if ("stage7-first-request" in timings and "stage1-scheduling" in timings) else \
            sum(v[2] for v in timings.values())
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
    global ready_uid
    uid, name, ns = pod.metadata.uid, pod.metadata.name, pod.metadata.namespace
    if uid in processed:
        return

    pod_name_by_uid[uid] = name
    namespace_by_uid[uid] = ns
    if (rev := _rev_name(pod)):
        revision_by_uid[uid] = rev
        if rev not in known_revisions:
            known_revisions.add(rev)
            log.info("[revision] New revision detected: %s", rev)

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

    # Stage 2: latest started_at across containers
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
    global v1, coapi
    try:
        config.load_incluster_config()
        log.info("Loaded in-cluster config")
    except Exception:
        config.load_kube_config()
        log.info("Loaded kubeconfig")
    v1 = client.CoreV1Api()
    coapi = client.CustomObjectsApi()

def list_matching_pods(ns: str, selector: str):
    return v1.list_namespaced_pod(ns, label_selector=selector or "")

def watch_pods(ns: str, selector: str):
    resp = list_matching_pods(ns, selector)
    log.info("Initial list: %d pod(s) matched selector '%s'", len(resp.items), selector or "<all>")
    for p in resp.items:
        process_pod(p)
    rv = resp.metadata.resource_version
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
                if obj is None or not hasattr(obj, "metadata"):
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

# ───────  Knative helpers ───────
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

def get_ksvc(co: client.CustomObjectsApi, ns: str, name: str) -> Optional[dict]:
    try:
        return co.get_namespaced_custom_object(
            group="serving.knative.dev", version="v1",
            namespace=ns, plural="services", name=name
        )
    except Exception:
        return None

def get_min_scale(co: client.CustomObjectsApi, ns: str, name: str) -> Optional[int]:
    ksvc = get_ksvc(co, ns, name)
    if not ksvc:
        return None
    ann = (((ksvc.get("spec") or {}).get("template") or {}).get("metadata") or {}).get("annotations") or {}
    val = ann.get("autoscaling.knative.dev/minScale")
    try:
        return int(val) if val is not None else None
    except Exception:
        return None

# ───────  Probe + wake helpers (Stages 4/6/7 + optional wake)  ───────
def probe(url: str, uid: str, ready_ts: datetime, kind: str):
    # Stage 4: poll until first 200 (up to ~30s)
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

    # Stage 7: one more request
    t7 = datetime.now(timezone.utc)
    try:
        status = requests.get(url, timeout=5).status_code
    except requests.RequestException:
        status = -1
    emit_span("stage7-first-request", t7, datetime.now(timezone.utc), uid)
    log.info("[%s] First request HTTP status: %s", kind, status)

def wake_service(url: Optional[str], attempts: int = 3, interval_s: int = 5):
    if not url:
        log.info("[wake] no URL to wake")
        return
    for i in range(max(1, attempts)):
        try:
            q = {"wake": int(time.time()), "r": random.randint(0, 1_000_000)}
            sep = "&" if "?" in url else "?"
            target = f"{url}{sep}{urlencode(q)}"
            log.info("[wake] hitting %s (attempt %d/%d)", target, i+1, attempts)
            requests.get(target, timeout=5)
        except requests.RequestException:
            pass
        if i < attempts - 1:
            time.sleep(interval_s)

# ───────  JSON serialization ───────
def _serialize_record(uid: str, kind: str) -> dict:
    with pod_lock:
        timings = pod_timings.get(uid, {}).copy()
        name = pod_state.get(uid, {}).get("name", pod_name_by_uid.get(uid))
        ns = pod_state.get(uid, {}).get("namespace", namespace_by_uid.get(uid))
        svc = service_by_uid.get(uid, "unknown")
        rev = revision_by_uid.get(uid, "unknown")

    if not timings:
        return {}

    if ("stage7-first-request" in timings and "stage1-scheduling" in timings):
        total_ms = (timings["stage7-first-request"][1] - timings["stage1-scheduling"][0]).total_seconds()*1000
    else:
        total_ms = sum(v[2] for v in timings.values())

    stages = {}
    for sid, label in STAGES.items():
        if sid in timings:
            start, end, ms = timings[sid]
            stages[sid] = {"label": label, "start": _iso(start), "end": _iso(end), "duration_ms": round(ms, 3)}

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

def assign_run_name(uid: str, is_revision_first_run: bool):
    global run_counter, rev_run_counter
    with run_lock:
        if is_revision_first_run:
            rev_run_counter += 1
            run_name_by_uid[uid] = f"revisioned-{rev_run_counter}"
        else:
            run_counter += 1
            run_name_by_uid[uid] = f"profiling-{run_counter}"

# ───────  15-min monitoring thread  ───────
def periodic_monitor(ns: str, selector: str, probe_url: Optional[str], ksvc_name: Optional[str]):
    global periodic_next_at
    with periodic_lock:
        periodic_next_at = time.time() + REPROBE_EVERY_SEC
    while True:
        time.sleep(0.5)
        with periodic_lock:
            due = (time.time() >= periodic_next_at)
        if not due:
            continue

        # Determine current pod count
        pods = list_matching_pods(ns, selector)
        pod_cnt = len(pods.items) if pods and getattr(pods, "items", None) is not None else 0

        # Decide action based on minScale and pod count
        minscale = None
        if ksvc_name:
            try:
                minscale = get_min_scale(coapi, ns, ksvc_name)
            except Exception:
                minscale = None

        if pod_cnt == 0 and (minscale == 0) and probe_url:
            log.info("[monitor] 0 pods & minScale=0 — waking service to capture next cold start")
            wake_service(probe_url, attempts=3, interval_s=5)
            with periodic_lock:
                periodic_next_at = time.time() + REPROBE_EVERY_SEC
            continue

        # Otherwise, if we have a target pod and URL, perform a warm reprobe
        tgt = None
        with pod_lock:
            tgt = last_probe_target
        if not tgt or not probe_url:
            with periodic_lock:
                periodic_next_at = time.time() + REPROBE_EVERY_SEC
            continue

        reprobe_url, uid, ready_ts = tgt
        alias_uid = f"{uid}-reprobe-{int(time.time())}"
        with pod_lock:
            pod_timings[alias_uid] = {}
            service_by_uid[alias_uid] = service_by_uid.get(uid, "unknown")
            namespace_by_uid[alias_uid] = namespace_by_uid.get(uid, "unknown")
            revision_by_uid[alias_uid] = revision_by_uid.get(uid, "unknown")
            pod_state[alias_uid] = pod_state.get(uid, {}).copy()

        assign_run_name(alias_uid, is_revision_first_run=False)
        try:
            probe(reprobe_url, alias_uid, ready_ts, kind="reprobe")
        except Exception as e:
            log.warning("Re-probe failed: %s", e)

        record = _serialize_record(alias_uid, kind="reprobe")
        if record:
            _write_line(HISTORY_LOG, record)
            _update_latest_and_previous(record)

        with periodic_lock:
            periodic_next_at = time.time() + REPROBE_EVERY_SEC

# ───────  main  ───────
def main():
    ap = argparse.ArgumentParser("Cold-start profiler (15-min monitoring; wake-if-zero; revision-aware)")
    ap.add_argument("--namespace", required=True, help="Namespace to watch (e.g., default)")
    ap.add_argument("--selector", required=True, help="Label selector (e.g., 'serving.knative.dev/service=httpbin')")
    ap.add_argument("--url", required=False, help="Service URL to probe/wake; if omitted, tries 'http://<ksvc>.<ns>.svc.cluster.local'")
    ap.add_argument("--logs-dir", default="logs", help="Directory to write logs (default: ./logs)")
    args = ap.parse_args()

    # Prepare file paths
    logs_path = _ensure_logs_dir(args.logs_dir)
    global HISTORY_LOG, LATEST_PREVIOUS_JSON
    HISTORY_LOG = str(logs_path / "stage_timings.ndjson")
    LATEST_PREVIOUS_JSON = str(logs_path / "latest_and_previous.json")

    # Clients
    setup_k8s()

    # Compute a probe URL usable even when scaled to zero
    probe_url = _build_probe_url(args.namespace, args.selector, args.url)
    if probe_url:
        log.info("Probe/Wake URL: %s", probe_url)
    else:
        log.warning("No probe URL could be derived. Provide --url or include serving.knative.dev/service=<name> in --selector.")

    ksvc_name = _extract_knative_service_from_selector(args.selector)

    # Start the watch thread
    threading.Thread(target=watch_pods, args=(args.namespace, args.selector), daemon=True).start()

    # Start 15-min monitoring thread
    threading.Thread(target=periodic_monitor,
                     args=(args.namespace, args.selector, probe_url, ksvc_name),
                     daemon=True).start()

    log.info("Profiler running in ns=%s selector='%s' (URL=%s)", args.namespace, args.selector, probe_url or "<none>")

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

                # Determine if this is the first run for a newly seen revision
                rev = revision_by_uid.get(uid)
                is_revision_first_run = (rev is not None and rev in known_revisions and rev not in revision_profiled_once)
                assign_run_name(uid, is_revision_first_run=is_revision_first_run)
                if is_revision_first_run:
                    revision_profiled_once.add(rev)

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

                # Persist results — history + latest/previous (+ baseline)
                record = _serialize_record(uid, kind="coldstart")
                if record:
                    _write_line(HISTORY_LOG, record)
                    _update_latest_and_previous(record)

                # Remember target for warm reprobes
                if probe_url:
                    with pod_lock:
                        global last_probe_target
                        last_probe_target = (probe_url, uid, ready_ts)

                # Reset the 15-min schedule if this was a revisioned run
                if is_revision_first_run:
                    with periodic_lock:
                        global periodic_next_at
                        periodic_next_at = time.time() + REPROBE_EVERY_SEC

                log.info("Done with pod %s — waiting for next Ready pod …", uid[:6])

if __name__ == "__main__":
    main()
