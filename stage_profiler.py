#!/usr/bin/env python3
"""
Knative cold-start stage profiler
---------------------------------
• Profiles every new pod (robust LIST + WATCH).
• Derives all seven stages from the Kubernetes API.
• Stage 5 ends when the queue-proxy container becomes Ready.
• Prints a formatted table after each pod.
• If API-server closes the stream, it reconnects.

Structured logs:
• logs/stage_timings.ndjson            (append-only history; one JSON object per profile)
• logs/latest_and_previous.json        (always holds {"latest": ..., "previous": ...})

New:
• --periodic-coldstart-every-minutes N (default 20): every N minutes, delete one current
  pod matching --selector to force a new cold start, which will be profiled & logged.
"""

import os, json, logging, argparse, threading, time, pathlib, random
from collections import OrderedDict
from datetime import datetime, timezone
from urllib.parse import urlparse

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

def _utc(dt):
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
def _ms(sec): return f"{sec*1e3:.2f} ms"
def _iso(dt): return _utc(dt).isoformat()

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
pod_state, pod_timings = {}, {}
pod_lock = threading.Lock()
ready_event = threading.Event()
ready_uid = None  # type: str | None
processed = set()
service_by_uid = {}
pod_name_by_uid = {}
namespace_by_uid = {}

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
HISTORY_LOG = None               # logs/stage_timings.ndjson
LATEST_PREVIOUS_JSON = None      # logs/latest_and_previous.json

def _ensure_logs_dir(logs_dir: str):
    path = pathlib.Path(logs_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path

def _write_history(record: dict):
    try:
        with open(HISTORY_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        log.warning("Failed writing history log: %s", e)

_lp_lock = threading.Lock()
_previous_record = None  # last run we wrote (any kind)

def _update_latest_and_previous(current_record: dict):
    """
    Persist a small JSON doc keeping only the latest run and the immediately previous one.
    {
      "updated_at": "...",
      "latest":   {...current_record...},
      "previous": {...previous_record...} | null
    }
    """
    global _previous_record
    with _lp_lock:
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "latest": current_record,
            "previous": _previous_record,
        }
        try:
            with open(LATEST_PREVIOUS_JSON, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.warning("Failed writing latest_and_previous.json: %s", e)
        # roll current into previous for the next call
        _previous_record = current_record

def _serialize_record(uid: str, kind: str = "coldstart") -> dict:
    with pod_lock:
        timings = pod_timings.get(uid, {}).copy()
        name = pod_state.get(uid, {}).get("name", pod_name_by_uid.get(uid))
        ns = pod_state.get(uid, {}).get("namespace", namespace_by_uid.get(uid))
        svc = service_by_uid.get(uid, "unknown")

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

    return {
        "kind": kind,  # "coldstart" or "reprobe"
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "namespace": ns,
        "service": svc,
        "pod_uid": uid,
        "pod_name": name,
        "total_ms": round(total_ms, 3),
        "stages": stages,
    }

# ────────────  Span + timing helpers  ────────────
def emit_span(stage, start, end, uid):
    span = tracer.start_span(stage, start_time=int(_utc(start).timestamp()*1e9))
    span.end(end_time=int(_utc(end).timestamp()*1e9))
    dur_ms = (end - start).total_seconds()*1000
    with pod_lock:
        pod_timings.setdefault(uid, {})[stage] = (start, end, dur_ms)
    log.info("%s [%s]: %s", STAGES.get(stage, stage), uid[:6], _ms(dur_ms/1000))

def print_table(uid):
    timings = pod_timings.get(uid)
    if not timings:
        return
    svc = service_by_uid.get(uid, "unknown")
    total = (timings["stage7-first-request"][1] - timings["stage1-scheduling"][0]).total_seconds()*1000
    print("\n\u250f" + "━"*70 + "┓")
    print(f"┃ {'COLD START PROFILE':^68} ┃")
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
def monitor_qproxy(ns, pod_name, uid, sched_ts):
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
def _svc_name(pod):
    lbl = pod.metadata.labels or {}
    return (lbl.get("serving.knative.dev/service")
            or lbl.get("app.kubernetes.io/name")
            or lbl.get("app"))

def process_pod(pod):
    global ready_uid
    uid, name, ns = pod.metadata.uid, pod.metadata.name, pod.metadata.namespace
    if uid in processed:
        return

    pod_name_by_uid[uid] = name
    namespace_by_uid[uid] = ns

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

    # Stage 2: first container running timestamp
    if "scheduled" in st and "running" not in st:
        for cs in (pod.status.container_statuses or []):
            if cs.state and cs.state.running:
                st["running"] = cs.state.running.started_at
                emit_span("stage2-image-pull", st["scheduled"], st["running"], uid)
                break

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

def watch_pods(ns, selector):
    setup_k8s()

    # 1) Initial LIST
    resp = v1.list_namespaced_pod(ns, label_selector=selector or "")
    log.info("Initial list: %d pod(s) matched selector '%s'", len(resp.items), selector or "<all>")
    for p in resp.items:
        process_pod(p)
    rv = resp.metadata.resource_version

    # 2) WATCH from that resourceVersion
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
                et = ev["type"]
                obj = ev["object"]
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

# ───────  HTTP probe (Stages 4/6/7)  ───────
def probe(url, uid, ready_ts, kind: str = "coldstart"):
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

    t7 = datetime.now(timezone.utc)
    try:
        status = requests.get(url, timeout=5).status_code
    except requests.RequestException:
        status = -1
    emit_span("stage7-first-request", t7, datetime.now(timezone.utc), uid)
    log.info("[%s] First request HTTP status: %s", kind, status)

# ───────  Periodic cold-start trigger (delete a pod)  ───────
def periodic_coldstart(ns: str, selector: str, period_sec: int, jitter_sec: int = 10):
    """
    Every 'period_sec', delete one running pod matching 'selector' in 'ns'.
    The watcher will see the new pod and profile it as a cold start.
    Requires RBAC to delete pods in 'ns'.
    """
    if period_sec <= 0:
        return
    setup_k8s()
    time.sleep(min(5, period_sec))
    while True:
        try:
            pods = v1.list_namespaced_pod(ns, label_selector=selector or "")
            candidates = [p for p in pods.items if (p.status.phase in ("Running", "Pending"))]
            if not candidates:
                log.info("[periodic] No pods found for selector='%s' in ns=%s", selector, ns)
            else:
                victim = random.choice(candidates)
                log.info("[periodic] Deleting pod %s to force cold start …", victim.metadata.name)
                try:
                    v1.delete_namespaced_pod(victim.metadata.name, ns, grace_period_seconds=0)
                except ApiException as e:
                    log.warning("[periodic] Failed to delete pod %s: %s", victim.metadata.name, e)
        except Exception as e:
            log.warning("[periodic] Error during periodic cold start: %s", e)

        sleep_s = period_sec + random.randint(0, max(1, jitter_sec))
        time.sleep(sleep_s)

# ───────  main  ───────
def main():
    ap = argparse.ArgumentParser("Cold-start profiler (every pod + periodic cold starts)")
    ap.add_argument("--namespace", required=True,
                    help="Namespace to watch (e.g., default)")
    ap.add_argument("--selector", required=True,
                    help="Label selector (e.g., 'serving.knative.dev/service=httpbin')")
    ap.add_argument("--url", required=False,
                    help="Route URL to probe; if omitted, auto-discovers from Knative service")
    ap.add_argument("--logs-dir", default="logs",
                    help="Directory to write logs (default: ./logs)")
    ap.add_argument("--reprobe-every", type=int, default=0,
                    help="If >0, periodically re-probe the same URL (steady-state) and log entries as kind='reprobe' every N seconds (NOT a cold start).")
    ap.add_argument("--periodic-coldstart-every-minutes", type=int, default=20,
                    help="If >0, every N minutes delete a current pod matching --selector to force a new cold start that will be profiled.")
    args = ap.parse_args()

    # Prepare file paths
    logs_path = _ensure_logs_dir(args.logs_dir)
    global HISTORY_LOG, LATEST_PREVIOUS_JSON
    HISTORY_LOG = str(logs_path / "stage_timings.ndjson")
    LATEST_PREVIOUS_JSON = str(logs_path / "latest_and_previous.json")

    # Start the watch thread
    threading.Thread(target=watch_pods,
                     args=(args.namespace, args.selector),
                     daemon=True).start()

    # Start the periodic cold-start thread
    period_sec = max(0, args.periodic_coldstart_every_minutes) * 60
    if period_sec > 0:
        threading.Thread(target=periodic_coldstart,
                         args=(args.namespace, args.selector, period_sec),
                         daemon=True).start()
        log.info("Periodic cold-start enabled: every %d minutes", args.periodic_coldstart_every_minutes)
    else:
        log.info("Periodic cold-start disabled")

    log.info("Profiler running in ns=%s selector='%s' (URL=%s)",
             args.namespace, args.selector, args.url or "<auto>")

    # Optional steady-state re-probe loop state
    next_reprobe_at = 0
    last_probe_target = None  # (url, uid, ready_ts)

    while True:
        # If a pod just became Ready, profile it (coldstart)
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

                # Determine probe URL: explicit > auto from Knative service label
                probe_url = args.url
                if not probe_url:
                    svc = service_by_uid.get(uid)
                    ns = st.get("namespace", args.namespace)
                    if svc:
                        probe_url = f"http://{svc}.{ns}.svc.cluster.local"
                        log.info("Auto-discovered probe URL: %s", probe_url)
                    else:
                        log.warning("No service label found for pod %s; skipping HTTP probe", uid[:6])

                if probe_url:
                    try:
                        probe(probe_url, uid, ready_ts, kind="coldstart")
                    except Exception as e:
                        log.warning("Probe failed for %s: %s", uid[:6], e)

                # Wait up to 120s for stage 5 to be recorded (queue-proxy ready)
                deadline = time.time() + 120
                while ("stage5-runtime-startup" not in pod_timings.get(uid, {})
                       and time.time() < deadline):
                    time.sleep(0.5)

                print_table(uid)

                # Persist results — history + latest/previous
                record = _serialize_record(uid, kind="coldstart")
                if record:
                    _write_history(record)
                    _update_latest_and_previous(record)

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
                pod_state[alias_uid] = pod_state.get(uid, {}).copy()
            try:
                probe(probe_url, alias_uid, ready_ts, kind="reprobe")
            except Exception as e:
                log.warning("Re-probe failed: %s", e)

            record = _serialize_record(alias_uid, kind="reprobe")
            if record:
                _write_history(record)
                _update_latest_and_previous(record)

            next_reprobe_at = time.time() + args.reprobe_every

if __name__ == "__main__":
    main()
