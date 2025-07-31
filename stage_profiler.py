import os
import logging
import argparse
from datetime import datetime
from time import time
from contextlib import contextmanager

from kubernetes import client, config, watch

from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# OpenTelemetry setup
collector_endpoint = os.getenv("OTEL_COLLECTOR_ENDPOINT", "localhost:4317")
resource = Resource(attributes={"service.name": "stage-profiler"})

# Tracing setup
tracer_provider = TracerProvider(resource=resource)
trace_exporter = OTLPSpanExporter(endpoint=collector_endpoint, insecure=True)
tracer_provider.add_span_processor(BatchSpanProcessor(trace_exporter))
trace.set_tracer_provider(tracer_provider)
tracer = trace.get_tracer(__name__)

# Metrics setup
metric_exporter = OTLPMetricExporter(endpoint=collector_endpoint, insecure=True)
metric_reader = PeriodicExportingMetricReader(metric_exporter)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter(__name__)

# Histograms for stages 4-7
hist_stage4 = meter.create_histogram("stage4_proxy_warmup_duration_ms", unit="ms")
hist_stage5 = meter.create_histogram("stage5_runtime_startup_duration_ms", unit="ms")
hist_stage6 = meter.create_histogram("stage6_app_init_duration_ms", unit="ms")
hist_stage7 = meter.create_histogram("stage7_first_request_duration_ms", unit="ms")

# State store for pods
pod_states = {}

# Helper to emit explicit spans for stages 1-3
def emit_span(name: str, start: datetime, end: datetime, attributes: dict):
    start_ns = int(start.timestamp() * 1e9)
    end_ns = int(end.timestamp() * 1e9)
    span = tracer.start_span(name, start_time=start_ns, attributes=attributes)
    span.end(end_time=end_ns)
    logger.info("Emitted span %s: %s → %s", name, start, end)

# Context managers for stages 4-7
@contextmanager
def stage4_proxy_warmup(initial_delay: int, concurrency: int):
    attrs = {"probe.initial_delay": initial_delay, "proxy.concurrency": concurrency}
    start = time()
    span = tracer.start_span("stage4-proxy-warmup", attributes=attrs)
    try:
        yield
    finally:
        end = time()
        span.end()
        duration = (end - start) * 1e3
        hist_stage4.record(duration, attributes=attrs)
        logger.info("Stage4 proxy warm-up: %.2fms", duration)

@contextmanager
def stage5_runtime_startup(runtime_language: str, jvm_flags: str = ""):
    attrs = {"runtime.language": runtime_language, "jvm.flags": jvm_flags}
    start = time()
    span = tracer.start_span("stage5-runtime-startup", attributes=attrs)
    try:
        yield
    finally:
        end = time()
        span.end()
        duration = (end - start) * 1e3
        hist_stage5.record(duration, attributes=attrs)
        logger.info("Stage5 runtime start-up: %.2fms", duration)

@contextmanager
def stage6_app_init(component: str):
    attrs = {"init.component": component}
    start = time()
    span = tracer.start_span("stage6-app-init", attributes=attrs)
    try:
        yield
    finally:
        end = time()
        span.end()
        duration = (end - start) * 1e3
        hist_stage6.record(duration, attributes=attrs)
        logger.info("Stage6 app init (%s): %.2fms", component, duration)

@contextmanager
def stage7_first_request(http_protocol: str, db_conn_reused: bool):
    attrs = {"http.protocol": http_protocol, "db.conn.reused": db_conn_reused}
    start = time()
    span = tracer.start_span("stage7-first-request", attributes=attrs)
    try:
        yield
    finally:
        end = time()
        span.end()
        duration = (end - start) * 1e3
        hist_stage7.record(duration, attributes=attrs)
        logger.info("Stage7 first request: %.2fms", duration)

# Processes pod events for stages 1-3
def process_pod_event(pod):
    uid = pod.metadata.uid
    state = pod_states.setdefault(uid, {})
    name = pod.metadata.name
    namespace = pod.metadata.namespace

    # Stage 1: scheduling
    submit = pod.metadata.creation_timestamp
    if 'submit' not in state:
        state['submit'] = submit
    for cond in pod.status.conditions or []:
        if cond.type == "PodScheduled" and cond.status == "True" and 'scheduled' not in state:
            state['scheduled'] = cond.last_transition_time
            emit_span("stage1-scheduling", state['submit'], state['scheduled'], {"pod": name, "ns": namespace})

    # Stage 2: image pull
    if 'scheduled' in state and 'running' not in state:
        for c in pod.status.container_statuses or []:
            if c.state.running:
                state['running'] = c.state.running.started_at
                emit_span("stage2-image-pull", state['scheduled'], state['running'], {"pod": name, "container": c.name})
                break

    # Stage 3: container init
    if 'running' in state and 'ready' not in state:
        for cond in pod.status.conditions or []:
            if cond.type == "Ready" and cond.status == "True":
                state['ready'] = cond.last_transition_time
                emit_span("stage3-container-init", state['running'], state['ready'], {"pod": name})
                pod_states.pop(uid, None)
                break

# Watches pods and triggers processing
def watch_pods(ns: str, selector: str):
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
    v1 = client.CoreV1Api()
    w = watch.Watch()
    logger.info("Watching pods in %s with selector '%s'", ns, selector)
    for ev in w.stream(v1.list_namespaced_pod, namespace=ns, label_selector=selector, timeout_seconds=0):
        process_pod_event(ev['object'])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage profiler for Knative cold starts")
    parser.add_argument("--namespace", default="default", help="Kubernetes namespace to watch")
    parser.add_argument("--selector", default="", help="Label selector for target pods")
    args = parser.parse_args()
    try:
        watch_pods(args.namespace, args.selector)
    except KeyboardInterrupt:
        logger.info("Shutting down watch, exiting...")
