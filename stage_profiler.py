import os
import logging
import argparse
from datetime import datetime
from time import time, sleep
from contextlib import contextmanager
from threading import Thread

from kubernetes import client, config, watch
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

# ─── Logging & Tracing setup ───────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OTLP_TRACES = os.getenv("OTEL_COLLECTOR_ENDPOINT", "http://localhost:4318/v1/traces")
resource = Resource({"service.name": "stage-profiler"})

# Tracer
tracer_provider = TracerProvider(resource=resource)
trace_exporter = OTLPSpanExporter(endpoint=OTLP_TRACES)
tracer_provider.add_span_processor(BatchSpanProcessor(trace_exporter))
trace.set_tracer_provider(tracer_provider)
tracer = trace.get_tracer(__name__)

# ─── Context managers for stages 4–7 ───────────────────────────────────────────────

@contextmanager
def stage4_proxy_warmup(initial_delay: int, concurrency: int):
    attrs = {"probe.initial_delay": initial_delay, "proxy.concurrency": concurrency}
    t0 = time()
    span = tracer.start_span("stage4-proxy-warmup", attributes=attrs)
    try:
        yield
    finally:
        span.end()
        dur = (time() - t0) * 1e3
        logger.info("Stage4 proxy warm-up: %.2fms", dur)

@contextmanager
def stage5_runtime_startup(runtime_language: str, jvm_flags: str = ""):
    attrs = {"runtime.language": runtime_language, "jvm.flags": jvm_flags}
    t0 = time()
    span = tracer.start_span("stage5-runtime-startup", attributes=attrs)
    try:
        yield
    finally:
        span.end()
        dur = (time() - t0) * 1e3
        logger.info("Stage5 runtime start-up: %.2fms", dur)

@contextmanager
def stage6_app_init(component: str):
    attrs = {"init.component": component}
    t0 = time()
    span = tracer.start_span("stage6-app-init", attributes=attrs)
    try:
        yield
    finally:
        span.end()
        dur = (time() - t0) * 1e3
        logger.info("Stage6 app init (%s): %.2fms", component, dur)

@contextmanager
def stage7_first_request(http_protocol: str, db_reused: bool):
    attrs = {"http.protocol": http_protocol, "db.conn.reused": db_reused}
    t0 = time()
    span = tracer.start_span("stage7-first-request", attributes=attrs)
    try:
        yield
    finally:
        span.end()
        dur = (time() - t0) * 1e3
        logger.info("Stage7 first request: %.2fms", dur)

# ─── Helper for stages 1–3 via Kubernetes API watcher ─────────────────────────────

pod_states = {}

def emit_duration_span(name: str, start: datetime, end: datetime, attrs: dict):
    dur = (end - start).total_seconds() * 1e3
    # export span
    s_ns = int(start.timestamp() * 1e9)
    e_ns = int(end.timestamp() * 1e9)
    span = tracer.start_span(name, start_time=s_ns, attributes=attrs)
    span.end(end_time=e_ns)
    # log only duration
    logger.info("%s: %.2fms", name, dur)

def process_pod_event(pod):
    uid = pod.metadata.uid
    st = pod_states.setdefault(uid, {})
    pod_name = pod.metadata.name

    # Stage 1: scheduling
    submit = pod.metadata.creation_timestamp
    if "submit" not in st:
        st["submit"] = submit
    for cond in pod.status.conditions or []:
        if cond.type == "PodScheduled" and cond.status == "True" and "scheduled" not in st:
            st["scheduled"] = cond.last_transition_time
            emit_duration_span("stage1-scheduling", st["submit"], st["scheduled"], {"pod": pod_name})

    # Stage 2: image pull
    if "scheduled" in st and "running" not in st:
        for cs in pod.status.container_statuses or []:
            if cs.state.running:
                st["running"] = cs.state.running.started_at
                emit_duration_span("stage2-image-pull", st["scheduled"], st["running"], {"pod": pod_name})
                break

    # Stage 3: container init
    if "running" in st and "ready" not in st:
        for cond in pod.status.conditions or []:
            if cond.type == "Ready" and cond.status == "True":
                st["ready"] = cond.last_transition_time
                emit_duration_span("stage3-container-init", st["running"], st["ready"], {"pod": pod_name})
                pod_states.pop(uid, None)
                break

def watch_pods(namespace: str, selector: str):
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    api = client.CoreV1Api()
    watcher = watch.Watch()
    logger.info(f"Watching pods in {namespace} with selector '{selector}'")
    for ev in watcher.stream(
        api.list_namespaced_pod,
        namespace=namespace,
        label_selector=selector,
        timeout_seconds=0
    ):
        process_pod_event(ev["object"])

# ─── MAIN ──────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Stage profiler for Knative cold starts")
    p.add_argument("--namespace", default=os.getenv("NAMESPACE", "default"))
    p.add_argument("--selector", default=os.getenv("SELECTOR", ""))
    args = p.parse_args()

    if not args.selector and "SERVICE_NAME" in os.environ:
        args.selector = f"serving.knative.dev/service={os.environ['SERVICE_NAME']}"

    # 1) Start watcher thread (daemon so it won’t block exit)
    t = Thread(target=watch_pods, args=(args.namespace, args.selector), daemon=True)
    t.start()
    sleep(0.1)  # ensure its “Watching pods…” log appears first

    # 2) Run stages 4–7 in order
    with stage4_proxy_warmup(initial_delay=40, concurrency=10):
        logger.info("Starting proxy…")
    with stage5_runtime_startup(runtime_language="go"):
        logger.info("Starting application runtime…")
    with stage6_app_init(component="db-connector"):
        logger.info("App-level init: DB connector…")
    with stage7_first_request(http_protocol="http1.1", db_reused=False):
        logger.info("Waiting for first request…")

    # Process exits here (daemon watcher thread stops automatically)
