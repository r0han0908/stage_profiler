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
# Jaeger Query service inside the observability namespace
JAEGER_QUERY_URL = os.getenv(
    "JAEGER_QUERY_URL",
    "http://jaeger-ui.observability.svc.cluster.local:16686"
)
SERVICE = os.getenv("SERVICE_NAME", "nginx")
LIMIT = 1  # only fetch the single most recent trace

# ─── Helpers ──────────────────────────────────────────────────
def fetch_latest_trace():
    """Query Jaeger for the latest trace data for SERVICE."""
    url = f"{JAEGER_QUERY_URL}/api/traces"
    params = {"service": SERVICE, "limit": LIMIT}
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as e:
        sys.exit(f"Error querying Jaeger ({url}): {e}")
    data = resp.json().get("data", [])
    if not data:
        sys.exit(f"No traces found in Jaeger for service '{SERVICE}'")
    return data[0]

def extract_stage(spans, name):
    """
    Find the span with operationName == name and return its duration in seconds.
    Jaeger startTime/duration are in microseconds.
    """
    for span in spans:
        if span.get("operationName") == name:
            dur_us = span.get("duration", 0)
            return dur_us / 1e6
    return None

# ─── Main ────────────────────────────────────────────────────
if __name__ == "__main__":
    trace = fetch_latest_trace()
    trace_id = trace.get("traceID", "<unknown>")
    spans = trace.get("spans", [])

    print(f"Trace ID: {trace_id}, Total Spans: {len(spans)}\n")

    stages = [
        "network-warmup",
        "runtime-startup",
        "app-init",
        "first-request",
    ]

    for stage in stages:
        duration = extract_stage(spans, stage)
        if duration is None:
            print(f"{stage:<15} NOT FOUND")
        else:
            print(f"{stage:<15} {duration:8.3f}s")
