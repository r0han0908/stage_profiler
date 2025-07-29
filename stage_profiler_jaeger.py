#!/usr/bin/env python3
"""
Stage profiler for Knative cold start phases:
1) Emit four named spans into OTLP collector:
   - network-warmup
   - runtime-startup
   - app-init
   - first-request
2) Sleep briefly to allow them to be exported
3) Fetch the most recent trace from Jaeger HTTP API
4) Extract and print durations for those same spans
"""

import argparse
import os
import sys
import socket
import time
import requests

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource, SERVICE_NAME as OTEL_SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

DEFAULT_LOCAL_QUERY = "localhost:31686"
DEFAULT_JAEGER_QUERY = os.getenv("JAEGER_QUERY_URL", DEFAULT_LOCAL_QUERY).replace("http://", "").replace("https://", "")
DEFAULT_OTEL_COLLECTOR = os.getenv("OTEL_COLLECTOR_ENDPOINT", "localhost:31317")
LIMIT = 1  # Only the most recent trace

def try_connect(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        ip = socket.gethostbyname(host)
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except Exception:
        return False

def choose_endpoint(candidates):
    for ep in candidates:
        if ":" not in ep:
            continue
        host, port = ep.split(":", 1)
        print(f"Trying endpoint {ep} …")
        if try_connect(host, int(port)):
            print(f"→ Using endpoint: {ep}\n")
            return ep
        else:
            print(f"   cannot reach {ep}")
    sys.exit("No reachable endpoint found among: " + ", ".join(candidates))

def emit_stage_spans(tracer, collector_host: str, collector_port: int, service_name: str):
    svc_url = f"http://{collector_host}:{collector_port}/"

    with tracer.start_as_current_span("network-warmup"):
        socket.gethostbyname(collector_host)
        try:
            conn = socket.create_connection((collector_host, collector_port), timeout=2)
            conn.close()
        except Exception:
            pass

    with tracer.start_as_current_span("runtime-startup"):
        import json
        _ = json.dumps({"warm": True})

    with tracer.start_as_current_span("app-init"):
        time.sleep(0.1)

    with tracer.start_as_current_span("first-request"):
        try:
            requests.get(svc_url, timeout=5)
        except Exception:
            pass

def fetch_latest_trace(base_url: str, service: str):
    url = f"http://{base_url}/api/traces"
    params = {"service": service, "limit": LIMIT}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
    except requests.RequestException as e:
        sys.exit(f"Error querying Jaeger ({url}): {e}")
    data = r.json().get("data", [])
    if not data:
        sys.exit(f"No traces found for service '{service}'")
    return data[0]

def extract_stage(spans, name):
    for s in spans:
        if s.get("operationName") == name:
            return s.get("duration", 0) / 1e6  # μs to ms
    return None

def main():
    parser = argparse.ArgumentParser(description="Emit and query spans for cold start profiling.")
    parser.add_argument("--service", default=os.getenv("SERVICE_NAME", "nginx"), help="Service name")
    args = parser.parse_args()

    # Select OTLP collector
    otlp_candidates = [DEFAULT_OTEL_COLLECTOR]
    otlp_endpoint = choose_endpoint(otlp_candidates)
    coll_host, coll_port = otlp_endpoint.split(":")

    # Setup tracer
    resource = Resource.create({OTEL_SERVICE_NAME: args.service})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    tracer = trace.get_tracer(__name__)

    # Emit 4 stage spans
    emit_stage_spans(tracer, coll_host, int(coll_port), args.service)

    time.sleep(5)

    # Choose Jaeger query endpoint
    query_candidates = [DEFAULT_JAEGER_QUERY, DEFAULT_LOCAL_QUERY]
    jaeger_query = choose_endpoint(query_candidates)

    # Fetch latest trace
    trace_data = fetch_latest_trace(jaeger_query, args.service)
    trace_id = trace_data.get("traceID", "<unknown>")
    spans = trace_data.get("spans", [])

    print(f"Trace ID: {trace_id}, spans found: {len(spans)}\n")
    for stage in ("network-warmup", "runtime-startup", "app-init", "first-request"):
        dur = extract_stage(spans, stage)
        if dur is None:
            print(f"{stage:<15} NOT FOUND")
        else:
            print(f"{stage:<15} {dur:8.3f} ms")

if __name__ == "__main__":
    main()
