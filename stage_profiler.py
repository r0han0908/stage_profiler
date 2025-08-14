#!/usr/bin/env python3
"""
Knative cold-start stage profiler (passive-only + rolling quantiles)
-------------------------------------------------------------------
• Profiles every new pod (robust LIST + WATCH).
• Derives stages from the Kubernetes API:
    - stage1-scheduling      (PodScheduled transition)
    - stage2-image-pull      (first container(s) running; uses latest started_at across containers)
    - stage3-container-init  (ContainersReady/Ready=True)
    - stage5-runtime-startup (queue-proxy container Ready)
• No HTTP probing, no wake-from-zero, no internal traffic.
• If the API server closes the stream, it reconnects.

Structured logs (files under --logs-dir, default ./logs):
  - stage_timings.ndjson        (append-only history; one JSON per pod event)
  - latest_and_best.json        ({"current": ..., "best_overall": ...})
  - quantiles.ndjson            (append-only 5-min window rollups per svc/rev/kind)
  - quantiles_latest.json       (latest rollups across keys)

Run naming / revision-aware:
• Each profiling run gets "profiling-<n>"; if triggered by detecting a new Knative
  Revision via pod labels, it's named "revisioned-<n>" and we reset the periodic timer.

Notes:
• Since this profiler is passive-only, stages that depend on HTTP traffic (e.g., "first request")
  are not recorded here. For full end-to-end cold start (incl. first-request), drive traffic with
  an external tool and correlate using timestamps.
"""

import os, json, logging, argparse, threading, time, pathlib, random
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, List

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
    if endpoint.startswith(("http://","https://")):
        try:
            from urllib.parse import urlparse
            return urlparse(endpoint).netloc
        except Exception:
            return endpoint
    return endpoint

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
last_revision_seen: Optional[str] = None

# Periodic scheduler control (resettable)
periodic_lock = threading.Lock()
periodic_next_at = 0.0

# Stage labels (we exclude 4/6/7 because they relied on HTTP probing)
STAGES = OrderedDict([
    ("stage1-scheduling",      "Pod Scheduling"),
    ("stage2-image-pull",      "Image Pull"),
    ("stage3-container-init",  "Container Init"),
    ("stage5-runtime-startup", "Runtime Startup"),
])

v1: client.CoreV1Api  # set later

# ────────────  File outputs  ────────────
HISTORY_LOG = None          # logs/stage_timings.ndjson
BEST_CURRENT_JSON = None    # logs/latest_and_best.json
QUANTILES_LOG = None        # logs/quantiles.ndjson
QUANTILES_LATEST_JSON = None# logs/quantiles_latest.json

# Write mutex to avoid interleaving
_write_lock = threading.Lock()

def _ensure_logs_dir(logs_dir: str):
    path = pathlib.Path(logs_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path

def _write_line(path: str, obj: dict):
    try:
        with _write_lock:
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception as e:
        log.warning("Failed writing %s: %s", path, e)

def _write_json(path: str, obj: dict):
    try:
        with _write_lock:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning("Failed writing %s: %s", path, e)

_best_lock = threading.Lock()
_best_overall = None  # cached best by lowest total_ms

def _update_latest_and_best(current_record: dict):
    global _best_overall
    with _best_lock:
        cur_total = current_record.get("total_ms", float("inf"))
        if (_best_overall is None or cur_total < _best_overall.get("total_ms", float("inf"))):
            _best_overall = current_record
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "current": current_record,
            "best_overall": _best_overall,
        }
        _write_json(BEST_CURRENT_JSON, payload)

# ────────────  Quantiles over time (rolling windows)  ────────────

Q_WINDOW_SEC = int(os.getenv("QUANTILES_WINDOW_SEC", "300"))           # 5 minutes
Q_FLUSH_EVERY_SEC = int(os.getenv("QUANTILES_FLUSH_EVERY_SEC", "30"))  # every 30s
MIN_QUANTILE_COUNT = int(os.getenv("QUANTILES_MIN_COUNT", "1"))

_q_lock = threading.Lock()
_q_buckets: Dict[Tuple[str, str, str, str, int], List[float]] = {}
_q_latest: Dict[Tuple[str, str, str, str], dict] = {}

def _to_epoch(ts_iso: str) -> int:
    try:
        dt = datetime.fromisoformat(ts_iso.replace("Z","+00:00"))
        return int(_utc(dt).timestamp())
    except Exception:
        return int(time.time())

def _window_start(ts_iso: str) -> int:
    t = _to_epoch(ts_iso)
    return t - (t % Q_WINDOW_SEC)

def _percentile(sorted_vals: List[float], p: float) -> Optional[float]:
    n = len(sorted_vals)
    if n == 0: return None
    if n == 1: return sorted_vals[0]
    rank = (p/100.0) * (n - 1)
    lo = int(rank)
    hi = min(n - 1, lo + 1)
    frac = rank - lo
    return sorted_vals[lo]*(1-frac) + sorted_vals[hi]*frac

def quantiles_add(record: dict):
    if not record:
        return
    ns = record.get("namespace","unknown")
    svc = record.get("service","unknown")
    rev = record.get("revision","unknown")
    kind = record.get("kind","coldstart")  # keep field for compatibility
    ts_iso = record.get("timestamp")
    total = record.get("total_ms")
    try:
        total = float(total)
    except Exception:
        return
    w = _window_start(ts_iso or datetime.now(timezone.utc).isoformat())
    key = (ns, svc, rev, kind, w)
    with _q_lock:
        _q_buckets.setdefault(key, []).append(total)

def quantiles_worker():
    while True:
        time.sleep(Q_FLUSH_EVERY_SEC)
        now = int(time.time())
        cutoff = now - Q_WINDOW_SEC
        out_any = False
        with _q_lock:
            for key in list(_q_buckets.keys()):
                ns, svc, rev, kind, w = key
                if w <= cutoff:
                    vals = _q_buckets.pop(key, [])
                    vals.sort()
                    n = len(vals)
                    if n < MIN_QUANTILE_COUNT:
                        continue
                    def pct(p):  # simple linear interpolation
                        if not vals: return 0.0
                        r = (p/100.0) * (len(vals)-1)
                        lo = int(r); hi = min(len(vals)-1, lo+1)
                        f = r - lo
                        return vals[lo]*(1-f) + vals[hi]*f
                    window = {
                        "window_start": datetime.fromtimestamp(w, tz=timezone.utc).isoformat(),
                        "window_end":   datetime.fromtimestamp(w + Q_WINDOW_SEC, tz=timezone.utc).isoformat(),
                        "namespace": ns, "service": svc, "revision": rev, "kind": kind,
                        "count": n,
                        "p50_ms": round(pct(50.0), 3),
                        "p95_ms": round(pct(95.0), 3),
                        "p99_ms": round(pct(99.0), 3),
                        "p999_ms": round(pct(99.9), 3),
                    }
                    _write_line(QUANTILES_LOG, window)
                    _q_latest[(ns, svc, rev, kind)] = window
                    log.info("[quantiles] %s/%s rev=%s kind=%s count=%d P50=%.1f P95=%.1f P99=%.1f P99.9=%.1f",
                             ns, svc, rev, kind, n, window["p50_ms"], window["p95_ms"], window["p99_ms"], window["p999_ms"])
                    out_any = True
            if out_any:
                _write_json(QUANTILES_LATEST_JSON, {
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "window_sec": Q_WINDOW_SEC,
                    "latest": list(_q_latest.values()),
                })

# ────────────  Spans + timing helpers  ────────────
def emit_span(stage: str, start: datetime, end: datetime, uid: str):
    dur_ms = (end - start).total_seconds()*1000
    with pod_lock:
        pod_timings.setdefault(uid, {})[stage] = (start, end, dur_ms)
    span = tracer.start_span(stage, start_time=int(_utc(start).timestamp()*1e9))
    span.set_attribute("k8s.namespace.name", namespace_by_uid.get(uid, "unknown"))
    span.set_attribute("knative.service",     service_by_uid.get(uid, "unknown"))
    span.set_attribute("knative.revision",    revision_by_uid.get(uid, "unknown"))
    span.set_attribute("prof.stage_id",       stage)
    span.end(end_time=int(_utc(end).timestamp()*1e9))
    log.info("%s [%s]: %s", STAGES.get(stage, stage), uid[:6], _ms(dur_ms/1000))

def _run_name_for(uid: str) -> str:
    return run_name_by_uid.get(uid, "profiling-0")

def _best_total_bounds(t: Dict[str, Tuple[datetime, datetime, float]]) -> Optional[Tuple[datetime, datetime]]:
    """
    Choose the best available [start,end] to compute total_ms in passive mode.
    Preference: stage5 end -> stage1 start; else stage3 end -> stage1 start; etc.
    """
    order = ["stage5-runtime-startup", "stage3-container-init", "stage2-image-pull", "stage1-scheduling"]
    if "stage1-scheduling" not in t:
        return None
    start = t["stage1-scheduling"][0]
    for key in order:
        if key in t:
            end = t[key][1]
    # 'end' will be from the last available in order above
    return (start, end)

def print_table(uid: str):
    timings = pod_timings.get(uid)
    if not timings:
        return
    svc = service_by_uid.get(uid, "unknown")
    run_name = _run_name_for(uid)
    bounds = _best_total_bounds(timings)
    total = (bounds[1] - bounds[0]).total_seconds()*1000 if bounds else 0.0
    print("\n\u250f" + "━"*70 + "┓")
    print(f"┃ {'COLD START PROFILE ' + '(' + run_name + ')':^68} ┃")
    print(f"┃ Pod {uid[:8]}  Service {svc:<20} {'':>24} ┃")
    print("┣" + "━"*70 + "┫")
    print(f"┃ {'Stage':^30} │ {'Duration':^18} │ {'% Total':^15} ┃")
    print("┣" + "━"*30 + "╋" + "━"*18 + "╋" + "━"*15 + "┫")
    for sid, label in STAGES.items():
        if sid in timings:
            _, _, ms = timings[sid]
            pct = (ms/total*100) if total else 0.0
            print(f"┃ {label:30} │ {_ms(ms/1000):18} │ {pct:13.1f}% ┃")
    print("┗" + "━"*70 + "┛\n")

# ───────  Stage-5 monitor (queue-proxy)  ───────
def monitor_qproxy(ns: str, pod_name: str, uid: str, sched_ts: datetime):
    # First quick check in case it's already Ready
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
        if rev not in known_revisions:
            known_revisions.add(rev)
            last_revision_seen = rev
            log.info("[revision] New revision detected: %s", rev)
            # reset periodic schedule to "now"
            with periodic_lock:
                global periodic_next_at
                periodic_next_at = time.time()

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

    # Stage 2: latest container running timestamp across containers
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
    setup_k8s()
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
                et = ev["type"]
                obj = ev["object"]
                # Some streams may send Status dicts—guard those
                if isinstance(obj, dict) or not hasattr(obj, "metadata"):
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

# ───────  Periodic cold-start trigger (delete only; NO wake)  ───────
def periodic_coldstart(ns: str, selector: str, period_sec: int, jitter_sec: int = 10):
    """
    Resettable scheduler:
    - Triggers at 'periodic_next_at'. On each run, schedules next = now + period + jitter.
    - On new revision detection, 'periodic_next_at' is set to now (immediate run).
    """
    if period_sec <= 0:
        return
    setup_k8s()
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
                    log.info("[periodic] no pods to delete (service may be at 0); passive mode does nothing")
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

    bounds = _best_total_bounds(timings)
    total_ms = (bounds[1] - bounds[0]).total_seconds()*1000 if bounds else sum(v[2] for v in timings.values())

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

    run_label = _run_name_for(uid)

    return {
        "kind": kind,  # retained for schema compatibility
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
def assign_run_name(uid: str, is_revision_event: bool):
    global run_counter, rev_run_counter
    with run_lock:
        if is_revision_event:
            rev_run_counter += 1
            run_name_by_uid[uid] = f"revisioned-{rev_run_counter}"
        else:
            run_counter += 1
            run_name_by_uid[uid] = f"profiling-{run_counter}"

# ───────  main  ───────
def main():
    ap = argparse.ArgumentParser("Cold-start profiler (passive; no wake/probe; rolling quantiles)")
    ap.add_argument("--namespace", required=True, help="Namespace to watch (e.g., default)")
    ap.add_argument("--selector", required=True, help="Label selector (e.g., 'serving.knative.dev/service=httpbin')")
    ap.add_argument("--logs-dir", default="logs", help="Directory to write logs (default: ./logs)")
    ap.add_argument("--periodic-coldstart-every-minutes", type=int, default=10,
                    help="If >0, every N minutes delete a current pod (no wake if zero pods).")
    args = ap.parse_args()

    # Prepare file paths
    logs_path = _ensure_logs_dir(args.logs_dir)
    global HISTORY_LOG, BEST_CURRENT_JSON, QUANTILES_LOG, QUANTILES_LATEST_JSON
    HISTORY_LOG = str(logs_path / "stage_timings.ndjson")
    BEST_CURRENT_JSON = str(logs_path / "latest_and_best.json")
    QUANTILES_LOG = str(logs_path / "quantiles.ndjson")
    QUANTILES_LATEST_JSON = str(logs_path / "quantiles_latest.json")

    # Start the quantiles worker
    threading.Thread(target=quantiles_worker, daemon=True).start()

    # Start the watch thread
    threading.Thread(target=watch_pods, args=(args.namespace, args.selector), daemon=True).start()

    # Start the periodic cold-start thread (delete only, no wake)
    period_sec = max(0, args.periodic_coldstart_every_minutes) * 60
    if period_sec > 0:
        threading.Thread(target=periodic_coldstart,
                         args=(args.namespace, args.selector, period_sec),
                         daemon=True).start()
        log.info("Periodic pod delete enabled: every %d minutes", args.periodic_coldstart_every_minutes)
    else:
        log.info("Periodic pod delete disabled")

    log.info("Profiler (passive) running in ns=%s selector='%s'", args.namespace, args.selector)

    while True:
        # When a pod becomes Ready for the first time, finalize and persist a record
        if ready_event.wait(timeout=1.0):
            uid = None
            try:
                uid = ready_uid
            finally:
                ready_event.clear()
            if uid and uid not in processed:
                processed.add(uid)

                # Decide run name: tag only the first profile per newly observed revision as "revisioned"
                rev = revision_by_uid.get(uid)
                is_revision_event = (rev is not None and rev in known_revisions and rev == last_revision_seen)
                assign_run_name(uid, is_revision_event=is_revision_event)

                print_table(uid)

                record = _serialize_record(uid, kind="coldstart")
                if record:
                    _write_line(HISTORY_LOG, record)
                    _update_latest_and_best(record)
                    quantiles_add(record)

                log.info("Done with pod %s — waiting for next Ready pod …", uid[:6])

if __name__ == "__main__":
    main()
