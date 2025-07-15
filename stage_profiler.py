#!/usr/bin/env python3

import os
import time
import traceback
from kubernetes import client, config as k8s_config
from datetime import datetime
from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.trace import use_span

# ─── Configuration ─────────────────────────────────────────────────────────────
SERVICE_NAME            = os.getenv("SERVICE_NAME", "my-app")
NAMESPACE               = os.getenv("NAMESPACE", "default")
OTEL_COLLECTOR_ENDPOINT = os.getenv("OTEL_COLLECTOR_ENDPOINT", "otel-collector.observability.svc.cluster.local:4317")

# ─── OpenTelemetry Resource ────────────────────────────────────────────────────
resource = Resource.create({
    "service.name": SERVICE_NAME,
    "k8s.namespace.name": NAMESPACE,
})

# ─── Tracing Setup ─────────────────────────────────────────────────────────────
tracer_provider = TracerProvider(resource=resource)
tracer_provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint=OTEL_COLLECTOR_ENDPOINT, insecure=True))
)
trace.set_tracer_provider(tracer_provider)
tracer = trace.get_tracer(__name__)

# ─── Metrics Setup ─────────────────────────────────────────────────────────────
metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=OTEL_COLLECTOR_ENDPOINT, insecure=True),
    export_interval_millis=5000
)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter(__name__)
scheduling_histogram = meter.create_histogram(
    name="control_plane_scheduling_duration",
    unit="s",
    description="Time taken by k8s scheduler to assign a node"
)

# ─── Kubernetes Client Setup ───────────────────────────────────────────────────
try:
    k8s_config.load_incluster_config()
except Exception:
    k8s_config.load_kube_config()
v1 = client.CoreV1Api()
ev = client.CoreV1Api()  # events are part of CoreV1

def find_application_pod(namespace: str, service: str) -> str:
    """
    Find the first Pod belonging to the Knative service via its label.
    """
    pods = v1.list_namespaced_pod(
        namespace=namespace,
        label_selector=f"serving.knative.dev/service={service}"
    )
    if not pods.items:
        raise RuntimeError(f"No pods found for Knative service '{service}' in ns '{namespace}'")
    return pods.items[0].metadata.name

def monitor_control_plane_scheduling(pod_name: str, namespace: str) -> float:
    """
    Measure and emit the time from Pod creation to the 'Scheduled' event.
    """
    # Fetch creation timestamp
    pod = v1.read_namespaced_pod(pod_name, namespace)
    creation_ts = pod.metadata.creation_timestamp

    # Poll events until we see the Scheduled event
    scheduled_ts = None
    while scheduled_ts is None:
        events = ev.list_namespaced_event(
            namespace=namespace,
            field_selector=f"involvedObject.name={pod_name},involvedObject.kind=Pod"
        ).items
        for e in events:
            if e.reason == "Scheduled":
                scheduled_ts = e.last_timestamp or e.event_time
                break
        if scheduled_ts is None:
            time.sleep(0.2)

    duration = (scheduled_ts - creation_ts).total_seconds()

    # Manually start a span with a custom start timestamp
    span = tracer.start_span("control-plane-scheduling", start_time=creation_ts.timestamp())
    # Activate it without auto-ending
    with use_span(span, end_on_exit=False):
        span.set_attribute("pod.name", pod_name)
        span.set_attribute("node", pod.spec.node_name)
        span.set_attribute("duration_s", duration)
    # Explicitly end with the scheduled timestamp
    span.end(end_time=scheduled_ts.timestamp())

    # Record histogram metric
    scheduling_histogram.record(duration, {"pod_name": pod_name})

    print(f"[control-plane-scheduling] {pod_name} → {duration:.3f}s")
    return duration

if __name__ == "__main__":
    try:
        # Wait until the Knative revision pod is present
        print(f"Waiting for pod of service '{SERVICE_NAME}' in ns '{NAMESPACE}'…")
        while True:
            try:
                # inside Knative container, HOSTNAME is the pod name
                pod_name = os.getenv("HOSTNAME") or find_application_pod(NAMESPACE, SERVICE_NAME)
                break
            except RuntimeError:
                time.sleep(1)

        print(f"Found pod: {pod_name}, starting profiling…")
        monitor_control_plane_scheduling(pod_name, NAMESPACE)
        # TODO: add more monitor_* calls for other stages here

    except Exception as e:
        print("ERROR in stage_profiler:", e)
        traceback.print_exc()
        exit(1)
