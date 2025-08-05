#!/usr/bin/env python3
"""
Knative cold-start stage profiler (K8s-only, every pod)
------------------------------------------------------
⟶ Profiles every new Pod in the target namespace once.
⟶ All stages are derived from the Kubernetes API; Stage 5 ends when the
   queue-proxy container becomes Ready.
⟶ Results for each pod are printed as a formatted table, then the profiler
   waits for the next unique pod UID to appear.
"""

import os, logging, argparse, threading, time
from datetime import datetime, timezone
from collections import OrderedDict
from urllib.parse import urlparse

import requests
from kubernetes import client, config, watch
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter


# ───────────────────── Logging ──────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("stage-profiler")


# ───────────── Helper utilities ─────────────
def _sanitize_endpoint(raw: str) -> str:
    raw = raw.strip().rstrip("/v1/traces").rstrip("/v1/metrics")
    return urlparse(raw).netloc if raw.startswith(("http://", "https://")) else raw


def _utc(dt):  # ensure tz-aware
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _ms(sec: float) -> str:
    return f"{sec*1e3:.2f} ms"


# ────────── OpenTelemetry (profiler spans) ──────────
collector_ep = _sanitize_endpoint(
    os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") or
    os.getenv("OTEL_COLLECTOR_ENDPOINT") or
    "localhost:4317"
)
tp = TracerProvider(resource=Resource({"service.name": "stage-profiler"}))
tp.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint=collector_ep, insecure=True))
)
trace.set_tracer_provider(tp)
tracer = trace.get_tracer(__name__)


# ───────────── Global state ─────────────
pod_state, pod_timings = {}, {}
pod_lock = threading.Lock()
ready_event = threading.Event()
ready_uid = None
processed = set()

STAGES = OrderedDict([
    ("stage1-scheduling", "Pod Scheduling"),
    ("stage2-image-pull", "Image Pull"),
    ("stage3-container-init", "Container Init"),
    ("stage5-runtime-startup", "Runtime Startup"),
    ("stage4-proxy-warmup", "Proxy Warm-up"),
    ("stage6-app-init", "App Init"),
    ("stage7-first-request", "First Request"),
])

v1: client.CoreV1Api  # set in setup_k8s()


# ────────── Span + timing helpers ──────────
def emit_span(stage, start, end, uid):
    span = tracer.start_span(stage, start_time=int(_utc(start).timestamp()*1e9))
    span.end(end_time=int(_utc(end).timestamp()*1e9))
    dur_ms = (end-start).total_seconds()*1000
    with pod_lock:
        pod_timings.setdefault(uid, {})[stage] = (start, end, dur_ms)
    log.info("%s [%s]: %s", STAGES.get(stage, stage), uid[:6], _ms(dur_ms/1000))


def print_table(uid, svc):
    timings = pod_timings.get(uid, {})
    if not timings:
        return
    total = (timings["stage7-first-request"][1] -
             timings["stage1-scheduling"][0]).total_seconds()*1000
    print("\n┏" + "━"*70 + "┓")
    print(f"┃ {'COLD START PROFILE':^68} ┃")
    print(f"┃ Pod {uid[:8]}  Service {svc:<20} {'':>24} ┃")
    print("┣" + "━"*70 + "┫")
    print(f"┃ {'Stage':^30} │ {'Duration':^18} │ {'% Total':^15} ┃")
    print("┠" + "─"*30 + "┼" + "─"*18 + "┼" + "─"*15 + "┨")
    for sid, label in STAGES.items():
        if sid in timings:
            _, _, d = timings[sid]
            pct = d/total*100 if total else 0
            print(f"┃ {label:30} │ {_ms(d/1000):18} │ {pct:13.1f}% ┃")
    print("┗" + "━"*70 + "┛\n")


# ────────── Stage 5 monitor (queue-proxy) ──────────
def monitor_qproxy(ns, pod_name, uid, scheduled_ts):
    for _ in range(120):
        try:
            pod = v1.read_namespaced_pod(pod_name, ns)
            for cs in pod.status.container_statuses or []:
                if cs.name == "queue-proxy" and cs.ready:
                    emit_span("stage5-runtime-startup",
                              scheduled_ts, datetime.now(timezone.utc), uid)
                    return
        except Exception:
            pass
        time.sleep(1)
    log.warning("queue-proxy never Ready for %s", uid[:6])


# ────────── Pod event processing ──────────
def process_pod(pod):
    global ready_uid
    uid, name, ns = pod.metadata.uid, pod.metadata.name, pod.metadata.namespace
    if uid in processed:
        return

    st = pod_state.setdefault(uid, {})
    st.setdefault("submit", pod.metadata.creation_timestamp)

    for c in pod.status.conditions or []:
        if c.type == "PodScheduled" and c.status == "True" and "scheduled" not in st:
            st["scheduled"] = c.last_transition_time
            emit_span("stage1-scheduling", st["submit"], c.last_transition_time, uid)

    if "scheduled" in st and "running" not in st:
        for cs in pod.status.container_statuses or []:
            if cs.state and cs.state.running:
                st["running"] = cs.state.running.started_at
                emit_span("stage2-image-pull", st["scheduled"], st["running"], uid)
                break

    for c in pod.status.conditions or []:
        if c.type == "Ready" and c.status == "True":
            if "ready" not in st and "running" in st:
                st["ready"] = c.last_transition_time
                emit_span("stage3-container-init", st["running"], c.last_transition_time, uid)
                th = threading.Thread(target=monitor_qproxy,
                                      args=(ns, name, uid, st["scheduled"]),
                                      daemon=True)
                th.start()
                ready_uid = uid
                ready_event.set()
            break


# ────────── Kubernetes watch loop ──────────
def setup_k8s():
    global v1
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
    v1 = client.CoreV1Api()


def watch_pods(ns, sel):
    setup_k8s()
    w = watch.Watch()
    for ev in w.stream(v1.list_namespaced_pod, namespace=ns,
                       label_selector=sel or "", timeout_seconds=120):
        process_pod(ev["object"])


# ────────── HTTP probe for Stages 4-7 ──────────
def probe(url, uid, ready_ts):
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
    log.info("First request HTTP status: %s", status)


# ───────────────────────── main ─────────────────────────
def main():
    pa = argparse.ArgumentParser("Cold-start profiler (all pods)")
    pa.add_argument("--namespace", default=os.getenv("NAMESPACE", "default"))
    pa.add_argument("--selector", default=os.getenv("SELECTOR", ""),
                    help="Label selector; blank = all pods")
    pa.add_argument("--url", required=True,
                    help="Route URL to probe, e.g. http://nginx.default")
    args = pa.parse_args()

    threading.Thread(target=watch_pods,
                     args=(args.namespace, args.selector),
                     daemon=True).start()
    log.info("Profiler running in namespace '%s' selector='%s'",
             args.namespace, args.selector or "<all>")

    while True:
        ready_event.wait()
        uid = ready_uid
        ready_event.clear()
        if uid in processed:
            continue
        processed.add(uid)

        st = pod_state[uid]
        ready_ts = st.get("ready") or st.get("scheduled") or datetime.now(timezone.utc)
        probe(args.url, uid, ready_ts)
        svc = pod.metadata.labels.get("serving.knative.dev/service", "unknown")
        print_table(uid, svc)
        log.info("Finished pod %s – waiting for next pod …", uid[:6])


if __name__ == "__main__":
    main()
