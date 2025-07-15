#!/usr/bin/env python3

import os
import time
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
ev = client.CoreV1Api()  # events are also under CoreV1

def find_application_pod(namespace: str, service: str) -> str:
    pods = v1.list_namespaced_pod(
        namespace=namespace,
        label_selector=f"serving.knative.dev/service={service}"
    )
    if not pods.items:
        raise RuntimeError(f"No pods found for Knative service '{service}'")
    return pods.items[0].metadata.name

def monitor_control_plane_scheduling(pod_name: str, namespace: str) -> float:
    # fetch Pod creation time
    pod = v1.read_namespaced_pod(pod_name, namespace)
    creation_ts = pod.metadata.creation_timestamp

    # poll Events for the "Scheduled" reason
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
            time.sleep(0.1)

    # compute duration
    duration = (scheduled_ts - creation_ts).total_seconds()

    # emit OTLP span with explicit timestamps
    with tracer.start_as_current_span(
        "control-plane-scheduling",
        start_time=creation_ts.timestamp(),
        end_time=scheduled_ts.timestamp()
    ) as span:
        span.set_attribute("pod.name", pod_name)
        span.set_attribute("node", pod.spec.node_name)
        span.set_attribute("duration_s", duration)

    # record histogram metric
    scheduling_histogram.record(duration, {"pod_name": pod_name})
    print(f"[control-plane-scheduling] {pod_name} → {duration:.3f}s")

    return duration

if __name__ == "__main__":
    try:
        # prefer HOSTNAME if set (in Pod), else discover dynamically
        pod_name = os.getenv("HOSTNAME") or find_application_pod(NAMESPACE, SERVICE_NAME)
        print(f"Profiling pod: {pod_name}")
        monitor_control_plane_scheduling(pod_name, NAMESPACE)
        # TODO: call other stage monitors here...
    except Exception as e:
        print("ERROR in stage_profiler:", e)
        import traceback; traceback.print_exc()
        exit(1)
