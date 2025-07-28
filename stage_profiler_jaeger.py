#!/usr/bin/env python3
"""
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

# Defaults for collector & query endpoints
DEFAULT_OTEL_COLLECTOR    = "otel-collector.observability.svc.cluster.local:4317"
DEFAULT_LOCAL_COLLECTOR   = "localhost:31317"
DEFAULT_JAEGER_QUERY      = "jaeger-ui.observability.svc.cluster.local:16686"
DEFAULT_LOCAL_QUERY       = "localhost:31686"
LIMIT                     = 1  # most recent trace


def try_connect(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        # Use numeric address for deterministic behavior
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
    svc_host = collector_host
    svc_port = collector_port
    svc_url  = f"http://{svc_host}:{svc_port}/"

    # 1. network-warmup
    with tracer.start_as_current_span("network-warmup"):
        # DNS resolution + TCP connect attempt
        socket.gethostbyname(svc_host)
        try:
            conn = socket.create_connection((svc_host, svc_port), timeout=2)
            conn.close()
        except Exception:
            pass

    # 2. runtime-startup
    with tracer.start_as_current_span("runtime-startup"):
        import json
        _ = json.dumps({"warm": True})

    # 3. app-init
    with tracer.start_as_current_span("app-init"):
        time.sleep(0.1)

    # 4. first-request
    with tracer.start_as_current_span("first-request"):
        try:
            requests.get(svc_url, timeout=5)
        except Exception:
            pass


def fetch_latest_trace(base_url: str, service: str):
    url    = f"http://{base_url}/api/traces"
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
            return s.get("duration", 0) / 1e6  # convert μs to s
    return None


def main():
    parser = argparse.ArgumentParser(description="Stage profiler to emit and query spans.")
    parser.add_argument("--fallback-collector",
                        help="Fallback OTLP collector host:port")
    parser.add_argument("--fallback-query",
                        help="Fallback Jaeger query host:port")
    parser.add_argument("--service", default=os.getenv("SERVICE_NAME", "nginx"),
                        help="Service name for spans and queries")
    args = parser.parse_args()

    # 1) Choose an OTLP collector endpoint
    coll_candidates = [DEFAULT_OTEL_COLLECTOR, DEFAULT_LOCAL_COLLECTOR]
    if args.fallback_collector:
        coll_candidates.append(args.fallback_collector)
    otlp_endpoint = choose_endpoint(coll_candidates)

    # 2) Split host and port for collector
    host, port = otlp_endpoint.split(":", 1)
    collector_port = int(port)

    # 3) Set up OpenTelemetry tracer to export to OTLP
    resource = Resource.create({OTEL_SERVICE_NAME: args.service})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    tracer = trace.get_tracer(__name__)

    # 4) Emit the four “stage” spans
    emit_stage_spans(tracer, host, collector_port, args.service)

    # 5) Give the collector / Jaeger a moment to ingest
    time.sleep(5)

    # 6) Choose a Jaeger query endpoint
    query_candidates = [DEFAULT_JAEGER_QUERY, DEFAULT_LOCAL_QUERY]
    if args.fallback_query:
        query_candidates.append(args.fallback_query)
    jaeger_query = choose_endpoint(query_candidates)

    # 7) Fetch and analyze the latest trace
    trace_data = fetch_latest_trace(jaeger_query, args.service)
    trace_id   = trace_data.get("traceID", "<unknown>")
    spans      = trace_data.get("spans", [])

    print(f"Trace ID: {trace_id}, spans found: {len(spans)}\n")

    for stage in ("network-warmup", "runtime-startup", "app-init", "first-request"):
        dur = extract_stage(spans, stage)
        if dur is None:
            print(f"{stage:<15} NOT FOUND")
        else:
            print(f"{stage:<15} {dur:8.3f}s")

if __name__ == "__main__":
    main()
