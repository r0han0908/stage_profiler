#!/usr/bin/env python3
"""
Knative cold-start stage profiler
---------------------------------
Stages
  1. scheduling
  2. image pull / unpack
  3. container init   (running ➜ Ready)
  4. queue-proxy warm-up            (URL probe until 1st 200)
  5. runtime start-up  (scheduled ➜ running)
  6. app-level init    (Ready ➜ URL 200)
  7. first request latency

Exports spans to an OpenTelemetry Collector (OTLP/gRPC, port 4317).
"""

import os
import logging
import argparse
import threading
import time
from datetime import datetime

import requests
from kubernetes import client, config, watch
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# ─────────────────────────  Logging  ──────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("stage-profiler")

# ────────────────────  Helper utilities  ──────────────────────
def _strip_suffix(text: str, suffix: str) -> str:
    return text[:-len(suffix)] if text.endswith(suffix) else text

def _sanitize_endpoint(ep: str) -> str:
    """
    Accepts:
        'otel-collector:4317'
        'http://otel-collector:4317'
        'http://localhost:4318/v1/traces'
    Returns:
        'otel-collector:4317'   (scheme + OTLP sub-paths stripped)
    """
    ep = _strip_suffix(_strip_suffix(ep.strip(), "/v1/traces"), "/v1/metrics")
    if ep.startswith(("http://", "https://")):
        from urllib.parse import urlparse
        ep = urlparse(ep).netloc
    return ep or "localhost:4317"

def _ms(delta: float) -> str:
    return f"{delta * 1e3:.2f} ms"

# ──────────────────  OpenTelemetry setup  ─────────────────────
collector_ep = _sanitize_endpoint(os.getenv("OTEL_COLLECTOR_ENDPOINT", "localhost:4317"))
resource      = Resource({"service.name": "stage-profiler"})

tp     = TracerProvider(resource=resource)
export = OTLPSpanExporter(endpoint=collector_ep, insecure=True)
tp.add_span_processor(BatchSpanProcessor(export))
trace.set_tracer_provider(tp)
tracer = trace.get_tracer(__name__)

# ───────────────  Shared state for pod watch  ─────────────────
pod_states        = {}
ready_event       = threading.Event()
ready_timestamp   = None

def emit_span(name: str, start: datetime, end: datetime):
    dur = (end - start).total_seconds()
    with tracer.start_span(name, start_time=int(start.timestamp() * 1e9)) as s:
        s.end(end_time=int(end.timestamp() * 1e9))
    log.info("%s: %s", name, _ms(dur))

# ───────────────  K8s pod event processing  ───────────────────
def process_pod(pod):
    global ready_timestamp
    uid   = pod.metadata.uid
    state = pod_states.setdefault(uid, {})

    # stage 1 – scheduling
    state.setdefault("submit", pod.metadata.creation_timestamp)
    for c in pod.status.conditions or []:
        if c.type == "PodScheduled" and c.status == "True" and "scheduled" not in state:
            state["scheduled"] = c.last_transition_time
            emit_span("stage1-scheduling", state["submit"], c.last_transition_time)

    # stage 2 (& 5) – image pull / runtime start-up
    if "scheduled" in state and "running" not in state:
        for cs in pod.status.container_statuses or []:
            if cs.state.running:
                state["running"] = cs.state.running.started_at
                emit_span("stage2-image-pull", state["scheduled"], state["running"])
                emit_span("stage5-runtime-startup", state["scheduled"], state["running"])
                break

    # stage 3 – container init
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
    log.info("Watching pods in %s with selector '%s'", namespace, selector)
    for ev in w.stream(v1.list_namespaced_pod,
                       namespace=namespace,
                       label_selector=selector,
                       timeout_seconds=0):
        process_pod(ev["object"])

# ────────────────  HTTP probing (stages 4 & 7)  ───────────────
def probe(url: str):
    # stage 4 – proxy warm-up
    t0 = datetime.utcnow()
    span = tracer.start_span("stage4-proxy-warmup")
    while True:
        try:
            if requests.get(url, timeout=0.5).status_code == 200:
                break
        except requests.RequestException:
            pass
        time.sleep(0.1)
    span.end()
    log.info("stage4-proxy-warmup: %s", _ms((datetime.utcnow() - t0).total_seconds()))

    # stage 6 – app-level init
    if ready_timestamp:
        emit_span("stage6-app-init", ready_timestamp, datetime.utcnow())

    # stage 7 – first request latency
    t7 = datetime.utcnow()
    span7 = tracer.start_span("stage7-first-request")
    resp  = requests.get(url)
    span7.end()
    log.info("stage7-first-request: %s (HTTP %s)",
             _ms((datetime.utcnow() - t7).total_seconds()), resp.status_code)

# ───────────────────────────  main  ───────────────────────────
def main():
    p = argparse.ArgumentParser(description="Knative cold-start stage profiler")
    p.add_argument("--namespace", default=os.getenv("NAMESPACE", "default"))
    p.add_argument("--selector",  default=os.getenv("SELECTOR", ""))
    p.add_argument("--url", required=True,
                   help="Full URL to the service (e.g. http://<IP>/nginx)")
    args = p.parse_args()

    if not args.selector and os.getenv("SERVICE_NAME"):
        args.selector = f"serving.knative.dev/service={os.getenv('SERVICE_NAME')}"

    threading.Thread(target=watch_pods,
                     args=(args.namespace, args.selector),
                     daemon=True).start()

    log.info("Waiting for Pod to become Ready…")
    if not ready_event.wait(300):
        log.error("Timeout: Pod was not Ready within 5 min")
        return

    probe(args.url)

if __name__ == "__main__":
    main()
