#!/usr/bin/env python3
"""
Fetch the last trace for SERVICE from Jaeger HTTP API and
extract durations for these spans:
  4. network‑warmup
  5. runtime‑startup
  6. app‑init
  7. first‑request
"""

import os
import sys
import requests

# ─── Configuration ────────────────────────────────────────────
JAEGER_QUERY_URL = os.getenv(
    "JAEGER_QUERY_URL",
    "http://jaeger-query.observability.svc.cluster.local:16686"
)
SERVICE = os.getenv("SERVICE_NAME", "nginx")
LIMIT = 1  # fetch the most recent trace

# ─── Helpers ──────────────────────────────────────────────────
def fetch_latest_trace():
    url = f"{JAEGER_QUERY_URL}/api/traces"
    params = {"service": SERVICE, "limit": LIMIT}
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as e:
        sys.exit(f"Error querying Jaeger: {e}")
    data = resp.json()
    traces = data.get("data", [])
    if not traces:
        sys.exit(f"No traces found in Jaeger for service '{SERVICE}'")
    return traces[0]

def extract_stage(span_list, name):
    """
    Find a span by operationName and return (start_time_s, duration_s).
    Jaeger returns startTime and duration in microseconds.
    """
    for s in span_list:
        if s.get("operationName") == name:
            start_us = s.get("startTime", 0)
            dur_us = s.get("duration", 0)
            return start_us / 1e6, dur_us / 1e6
    return None, None

# ─── Main ────────────────────────────────────────────────────
if __name__ == "__main__":
    trace_data = fetch_latest_trace()
    trace_id = trace_data.get("traceID", "<unknown>")
    spans = trace_data.get("spans", [])

    print(f"Found trace ID: {trace_id} with {len(spans)} spans\n")

    stages = [
        "network-warmup",
        "runtime-startup",
        "app-init",
        "first-request",
    ]

    for stage in stages:
        start, dur = extract_stage(spans, stage)
        if start is None:
            print(f"{stage:<15} NOT FOUND")
        else:
            print(f"{stage:<15} {dur:8.3f}s")
