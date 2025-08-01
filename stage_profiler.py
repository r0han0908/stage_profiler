#!/usr/bin/env python3
"""
Knative cold-start stage profiler
---------------------------------
Stages
  1. scheduling
  2. image pull / unpack
  3. container init           (Running ➜ Ready)
  4. queue-proxy warm-up      (URL probe until 1st 200)
  5. runtime start-up         (Scheduled ➜ Running)
  6. app-level init           (Ready ➜ URL 200)
  7. first-request latency

All spans are exported over OTLP/gRPC to an OpenTelemetry Collector.
"""

import os
import logging
import argparse
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from kubernetes import client, config, watch
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# ─────────────────────────  Logging  ──────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("stage-profiler")

# ───────────────────  Small helpers  ─────────────────────────
def _strip_suffix(txt: str, suf: str) -> str:
    return txt[:-len(suf)] if txt.endswith(suf) else txt

def _sanitize_endpoint(raw: str) -> str:
    """Remove scheme & OTLP sub-paths → '<host>:<port>'"""
    raw = _strip_suffix(_strip_suffix(raw.strip(), "/v1/traces"), "/v1/metrics")
    if raw.startswith(("http://", "https://")):
        raw = urlparse(raw).netloc
    return raw or "localhost:4317"

def _utc(dt: datetime) -> datetime:
    """Return a timezone-aware UTC datetime."""
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

def _ms(delta_sec: float) -> str:
    return f"{delta_sec * 1e3:.2f} ms"

# ────────────────  OpenTelemetry setup  ──────────────────────
endpoint_env = (os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") or
                os.getenv("OTEL_COLLECTOR_ENDPOINT") or
                "localhost:4317")

collector_ep = _sanitize_endpoint(endpoint_env)

tp = TracerProvider(resource=Resource({"service.name": "stage-profiler"}))
tp.add_span_processor(BatchSpanProcessor(
    OTLPSpanExporter(endpoint=collector_ep, insecure=True)
))
trace.set_tracer_provider(tp)
tracer = trace.get_tracer(__name__)

# ───────────────  Shared pod-watch state  ────────────────────
pod_state        = {}
ready_event      = threading.Event()
ready_timestamp  = None                      # set when pod becomes Ready

def emit_span(name: str, start: datetime, end: datetime):
    start, end = map(_utc, (start, end))
    span = tracer.start_span(name,
                             start_time=int(start.timestamp()*1e9))
    span.end(end_time=int(end.timestamp()*1e9))
    log.info("%s: %s", name, _ms((end - start).total_seconds()))

# ────────────────  K8s event processing  ─────────────────────
def process_pod(pod):
    global ready_timestamp
    uid   = pod.metadata.uid
    state = pod_state.setdefault(uid, {})

    # Stage 1 – scheduling
    state.setdefault("submit", pod.metadata.creation_timestamp)
    for c in pod.status.conditions or []:
        if c.type == "PodScheduled" and c.status == "True" and "scheduled" not in state:
            state["scheduled"] = c.last_transition_time
            emit_span("stage1-scheduling", state["submit"], c.last_transition_time)

    # Stage 2 & 5 – image-pull / runtime start-up
    if "scheduled" in state and "running" not in state:
        for cs in pod.status.container_statuses or []:
            if cs.state.running:
                state["running"] = cs.state.running.started_at
                emit_span("stage2-image-pull", state["scheduled"], state["running"])
                emit_span("stage5-runtime-startup", state["scheduled"], state["running"])
                break

    # Stage 3 – container init
    for c in pod.status.conditions or []:
        if c.type == "Ready" and c.status == "True" and not ready_event.is_set():
            if "running" in state:
                emit_span("stage3-container-init", state["running"], c.last_transition_time)
            ready_timestamp = c.last_transition_time
            ready_event.set()
            break

def watch_pods(namespace: str, selector: str):
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
    v1 = client.CoreV1Api()
    w  = watch.Watch()
    log.info("Watching pods in %s selector='%s'", namespace, selector)
    for ev in w.stream(v1.list_namespaced_pod,
                       namespace=namespace,
                       label_selector=selector,
                       timeout_seconds=0):
        process_pod(ev["object"])

# ─────────────  HTTP probing (stages 4 & 7)  ──────────────────
def probe(url: str):
    # Stage 4 – proxy warm-up
    t0   = datetime.now(timezone.utc)
    span = tracer.start_span("stage4-proxy-warmup", start_time=int(t0.timestamp()*1e9))
    while True:
        try:
            if requests.get(url, timeout=0.5).status_code == 200:
                break
        except requests.RequestException:
            time.sleep(0.1)
    span.end()
    log.info("stage4-proxy-warmup: %s", _ms((datetime.now(timezone.utc)-t0).total_seconds()))

    # Stage 6 – app-level init (Ready ➜ first 200)
    if ready_timestamp:
        emit_span("stage6-app-init", ready_timestamp, datetime.now(timezone.utc))

    # Stage 7 – first request latency
    t7   = datetime.now(timezone.utc)
    s7   = tracer.start_span("stage7-first-request", start_time=int(t7.timestamp()*1e9))
    resp = requests.get(url)
    s7.end()
    log.info("stage7-first-request: %s (HTTP %d)",
             _ms((datetime.now(timezone.utc)-t7).total_seconds()), resp.status_code)

# ────────────────────────────  main  ─────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser(description="Knative cold-start stage profiler")
    ap.add_argument("--namespace", default=os.getenv("NAMESPACE", "default"))
    ap.add_argument("--selector",  default=os.getenv("SELECTOR", ""))
    ap.add_argument("--url", required=True,
                    help="Full URL of the service (e.g. http://nginx.default.svc.cluster.local)")
    args = ap.parse_args()

    if not args.selector and os.getenv("SERVICE_NAME"):
        args.selector = f"serving.knative.dev/service={os.getenv('SERVICE_NAME')}"

    threading.Thread(target=watch_pods,
                     args=(args.namespace, args.selector),
                     daemon=True).start()

    log.info("Waiting for Pod to become Ready …")
    if not ready_event.wait(timeout=300):
        log.error("Timeout waiting for Ready condition")
        return

    probe(args.url)

if __name__ == "__main__":
    main()
