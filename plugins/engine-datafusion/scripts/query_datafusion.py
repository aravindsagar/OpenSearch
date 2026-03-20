#!/usr/bin/env python3
"""
Sample DataFusion queries for the df-test index via the PPL endpoint.

The OpenSearch SQL plugin translates PPL into a Substrait plan and sends it
as query_plan_ir, which DataFusion executes.  No Substrait building required.

Prerequisites:
    OpenSearch SQL plugin installed (substrait-plan branch):
    https://github.com/vinaykpud/sql/tree/substrait-plan

Usage:
    python3 query_datafusion.py [--index df-test] [--host localhost] [--port 9200]

Requirements:
    pip install requests
"""

import argparse
import json
import time

import requests


# ---------------------------------------------------------------------------
# Queries — PPL syntax targeting the df-test index schema:
#   int_0..int_4   (integer / Int32)
#   float_0..float_2 (float / Float32)
#   text_0..text_4 (text)
#   timestamp (date), id (keyword)
# ---------------------------------------------------------------------------

def queries(index: str) -> list[tuple[str, str]]:
    return [
        (
            "Q1: count + min/max/avg on all int fields (full scan, 5 columns)",
            f"source={index} | stats count(), "
            f"min(int_0) as min0, max(int_0) as max0, avg(int_0) as avg0, "
            f"min(int_1) as min1, max(int_1) as max1, avg(int_1) as avg1, "
            f"min(int_2) as min2, max(int_2) as max2, avg(int_2) as avg2, "
            f"min(int_3) as min3, max(int_3) as max3, avg(int_3) as avg3, "
            f"min(int_4) as min4, max(int_4) as max4, avg(int_4) as avg4",
        ),
        (
            "Q2: count + sum/avg on all float fields (full scan)",
            f"source={index} | stats count(), "
            f"sum(float_0) as sf0, avg(float_0) as af0, "
            f"sum(float_1) as sf1, avg(float_1) as af1, "
            f"sum(float_2) as sf2, avg(float_2) as af2",
        ),
        (
            "Q3: filtered aggregate — int_0 > 700000 (selective scan ~30%)",
            f"source={index} | where int_0 > 700000 "
            f"| stats count(), sum(int_1) as s, min(int_2) as mn, max(int_3) as mx",
        ),
        (
            "Q4: two-column filter + multi-aggregate (~9% selectivity)",
            f"source={index} | where int_0 > 700000 and int_1 < 300000 "
            f"| stats count(), sum(int_2) as s2, avg(float_0) as af0",
        ),
        (
            "Q5: group by int_0 bucket (mod 10) — 10 groups, aggregates per group",
            f"source={index} | stats count(), sum(int_1) as s, avg(float_0) as af "
            f"by span(int_0, 100000)",
        ),
        (
            "Q6: sort + large fetch — order by int_0 desc, int_1 asc, head 100000",
            f"source={index} | sort - int_0, + int_1 | head 100000",
        ),
        (
            "Q7: sort all rows (forces full merge-sort of 1M rows)",
            f"source={index} | sort - int_0, + float_0 | head 1000000",
        ),
        (
            "Q8: where filter + sort + head (scan → filter → sort)",
            f"source={index} | where int_2 < 500000 | sort - int_3 | head 50000",
        ),
    ]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_query(ppl_url: str, label: str, ppl: str, timeout: int, dry_run: bool) -> None:
    print(f"\n{'='*72}")
    print(f"  {label}")
    print(f"  PPL: {ppl[:120]}")
    print(f"{'='*72}")

    if dry_run:
        print("  [dry-run] skipping HTTP request")
        return

    t0 = time.monotonic()
    try:
        resp = requests.post(
            ppl_url,
            json={"query": ppl},
            headers={"Content-Type": "application/json"},
            timeout=timeout,
        )
        elapsed = time.monotonic() - t0

        if resp.ok:
            data = resp.json()
            total = data.get("total", "?")
            took = data.get("datarows") and len(data["datarows"])
            rows = data.get("datarows", [])
            print(f"  Status:  {resp.status_code}  wall={elapsed:.2f}s  rows={len(rows)}")
            if rows:
                schema = [c["name"] for c in data.get("schema", [])]
                print(f"  Columns: {schema}")
                print(f"  Row[0]:  {rows[0]}")
                if len(rows) > 1:
                    print(f"  Row[-1]: {rows[-1]}")
        else:
            print(f"  FAILED:  {resp.status_code}  wall={elapsed:.2f}s")
            # Pretty-print the error so it's readable
            try:
                err = resp.json()
                reason = (err.get("error", {}).get("reason")
                          or err.get("error", {}).get("details")
                          or json.dumps(err)[:400])
                print(f"  Error:   {reason}")
            except Exception:
                print(f"  Body:    {resp.text[:400]}")
    except requests.exceptions.Timeout:
        elapsed = time.monotonic() - t0
        print(f"  TIMEOUT after {elapsed:.1f}s — query still running on server")
    except Exception as exc:
        print(f"  ERROR: {exc}")


def main():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--host",    default="localhost")
    p.add_argument("--port",    type=int, default=9200)
    p.add_argument("--index",   default="df-test")
    p.add_argument("--timeout", type=int, default=300,
                   help="Per-query HTTP timeout in seconds (default: 300)")
    p.add_argument("--queries", default="all",
                   help="Comma-separated query numbers to run, e.g. --queries 1,3 (default: all)")
    p.add_argument("--dry-run", action="store_true",
                   help="Print queries without sending HTTP requests")
    args = p.parse_args()

    ppl_url = f"http://{args.host}:{args.port}/_plugins/_ppl"

    if args.queries == "all":
        selected = None
    else:
        selected = set(int(x) for x in args.queries.split(","))

    q_list = queries(args.index)
    print(f"DataFusion PPL query runner")
    print(f"  index={args.index}  ppl_url={ppl_url}  timeout={args.timeout}s")
    print(f"  Running: {args.queries}")

    for i, (label, ppl) in enumerate(q_list, start=1):
        if selected is not None and i not in selected:
            continue
        run_query(ppl_url, label, ppl, args.timeout, args.dry_run)

    print("\nDone.")


if __name__ == "__main__":
    main()
