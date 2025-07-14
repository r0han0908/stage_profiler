#!/usr/bin/env python3

import os
import time
from datetime import datetime

from kubernetes import client, config

from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

# Environment configuration
SERVICE_NAME = os.getenv("SERVICE_NAME", "my-app")
NAMESPACE = os.getenv("NAMESPACE", "default")
OTEL_COLLECTOR_ENDPOINT = os.getenv("OTEL_COLLECTOR_ENDPOINT", "otel-collector.observability.svc.cluster.local:4317")

# Resource with service and Kubernetes metadata
resource = Resource.create({
    "service.name": SERVICE_NAME,
    "k8s.namespace.name": NAMESPACE
})

# --- Tracing setup ---
tracer_provider = TracerProvider(resource=resource)
span_processor = BatchSpanProcessor(
    OTLPSpanExporter(endpoint=OTEL_COLLECTOR_ENDPOINT, insecure=True)
)
tracer_provider.add_span_processor(span_processor)
trace.set_tracer_provider(tracer_provider)
tracer = trace.get_tracer(__name__)

# --- Metrics setup ---
metric_exporter = OTLPMetricExporter(endpoint=OTEL_COLLECTOR_ENDPOINT, insecure=True)
metric_reader = PeriodicExportingMetricReader(
    metric_exporter,
    export_interval_millis=5000
)
meter_provider = MeterProvider(
    resource=resource,
    metric_readers=[metric_reader]
)
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter(__name__)
scheduling_histogram = meter.create_histogram(
    name="control_plane_scheduling_duration",
    unit="s",
    description="Time taken by Kubernetes control-plane scheduler to assign a node"
)


def monitor_control_plane_scheduling(pod_name: str, namespace: str):
    """
    Detect when the Kubernetes scheduler assigns a node to this Pod,
    emit a span and a histogram metric of the scheduling duration.
    """
    # Try in-cluster config first, fallback to kubeconfig if not in cluster
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
    v1 = client.CoreV1Api()

    # Fetch Pod creation timestamp
    pod = v1.read_namespaced_pod(pod_name, namespace)
    creation_ts = pod.metadata.creation_timestamp  # datetime

    # Poll events until the "Scheduled" event appears
    scheduled_ts = None
    while scheduled_ts is None:
        events = v1.list_namespaced_event(
            namespace,
            field_selector=f"involvedObject.name={pod_name}"
        ).items
        for e in events:
            if e.reason == "Scheduled":
                scheduled_ts = e.last_timestamp or e.event_time
                break
        if scheduled_ts is None:
            time.sleep(0.2)

    # Calculate scheduling duration
    duration = (scheduled_ts - creation_ts).total_seconds()

    # Emit a trace span with explicit start/end timestamps
    with tracer.start_as_current_span(
        "control-plane-scheduling",
        start_time=creation_ts.timestamp(),
        end_time=scheduled_ts.timestamp()
    ) as span:
        span.set_attribute("k8s.scheduled_node", pod.spec.node_name)
        span.set_attribute("scheduling.duration_s", duration)

    # Record a histogram metric
    scheduling_histogram.record(duration, {"pod_name": pod_name})

    return duration


if __name__ == "__main__":
    pod_name = os.getenv("HOSTNAME")
    duration = monitor_control_plane_scheduling(pod_name, NAMESPACE)
    print(f"Control-plane scheduling for pod {pod_name} took {duration:.3f}s")
