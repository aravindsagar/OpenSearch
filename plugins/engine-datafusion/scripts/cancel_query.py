#!/usr/bin/env python3
"""
Start a long-running DataFusion PPL query and cancel it mid-execution.

The script:
  1. Sends a heavy PPL query in a background thread (it will block for minutes).
  2. Polls GET /_tasks until the corresponding search shard task appears.
  3. Cancels it via POST /_tasks/<task_id>/_cancel.
  4. Waits for the background thread to complete and reports whether the query
     received a cancellation error or finished before the cancel arrived.

Usage:
    python3 cancel_query.py [options]

Requirements:
    pip install requests
"""

import argparse
import sys
import threading
import time

import requests


# ---------------------------------------------------------------------------
# Heavy queries — designed to run for 30 s+ on a 1M-doc index so there is
# plenty of time to observe cancellation.
# ---------------------------------------------------------------------------

HEAVY_QUERIES = [
    (
        "Full sort of 1M rows (merge-sort, CPU-bound)",
        "source={index} | sort - int_0, + float_0 | head 1000000",
    ),
    (
        "Sort + large fetch — 100k rows ordered by two columns",
        "source={index} | sort - int_0, + int_1 | head 100000",
    ),
    (
        "Full-scan aggregate across all 5 int + 3 float columns",
        "source={index} | stats count(), "
        "min(int_0) as mn0, max(int_0) as mx0, avg(int_0) as av0, "
        "min(int_1) as mn1, max(int_1) as mx1, avg(int_1) as av1, "
        "min(int_2) as mn2, max(int_2) as mx2, avg(int_2) as av2, "
        "min(int_3) as mn3, max(int_3) as mx3, avg(int_3) as av3, "
        "min(int_4) as mn4, max(int_4) as mx4, avg(int_4) as av4, "
        "sum(float_0) as sf0, sum(float_1) as sf1, sum(float_2) as sf2",
    ),
]


# ---------------------------------------------------------------------------
# Background query thread
# ---------------------------------------------------------------------------

class QueryResult:
    def __init__(self):
        self.status_code: int | None = None
        self.elapsed: float = 0.0
        self.body: dict = {}
        self.error: str | None = None


def send_query(ppl_url: str, ppl: str, result: QueryResult, timeout: int = 300) -> None:
    """Send a PPL query (blocks until response or timeout)."""
    t0 = time.monotonic()
    try:
        resp = requests.post(
            ppl_url,
            json={"query": ppl},
            headers={"Content-Type": "application/json"},
            timeout=timeout,
        )
        result.elapsed = time.monotonic() - t0
        result.status_code = resp.status_code
        try:
            result.body = resp.json()
        except Exception:
            result.body = {"raw": resp.text[:500]}
    except requests.exceptions.Timeout:
        result.elapsed = time.monotonic() - t0
        result.error = f"HTTP timeout after {result.elapsed:.1f}s"
    except Exception as exc:
        result.elapsed = time.monotonic() - t0
        result.error = str(exc)


# ---------------------------------------------------------------------------
# Task discovery
# ---------------------------------------------------------------------------

def find_search_tasks(base_url: str) -> list[dict]:
    """Return all running search shard tasks from GET /_tasks."""
    try:
        resp = requests.get(
            f"{base_url}/_tasks",
            params={"actions": "*search*", "detailed": "true"},
            timeout=5,
        )
        if not resp.ok:
            return []
        data = resp.json()
        tasks = []
        for node_id, node in data.get("nodes", {}).items():
            for task_id, task in node.get("tasks", {}).items():
                # task_id is already the full "node_id:task_number" string.
                # Store it as "full_id" to avoid being overwritten by the
                # numeric "id" field inside the task body when we spread **task.
                tasks.append({**task, "full_id": task_id})
        return tasks
    except Exception:
        return []


def wait_for_task(base_url: str, poll_interval: float = 0.2, max_wait: float = 10.0) -> str | None:
    """
    Poll /_tasks until a new search task appears.
    Returns the task ID string (e.g. "node123:456") or None on timeout.
    """
    deadline = time.monotonic() + max_wait
    seen_before: set[str] = set()

    # Snapshot tasks already running before we started.
    for t in find_search_tasks(base_url):
        seen_before.add(t["full_id"])

    while time.monotonic() < deadline:
        time.sleep(poll_interval)
        for task in find_search_tasks(base_url):
            if task["full_id"] not in seen_before:
                return task["full_id"]

    return None


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------

def cancel_task(base_url: str, task_id: str) -> dict:
    """POST /_tasks/<task_id>/_cancel and return the response body."""
    try:
        resp = requests.post(f"{base_url}/_tasks/{task_id}/_cancel", timeout=10)
        return {"status_code": resp.status_code, "body": resp.json()}
    except Exception as exc:
        return {"status_code": None, "error": str(exc)}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--host",  default="localhost")
    p.add_argument("--port",  type=int, default=9200)
    p.add_argument("--index", default="df-test")
    p.add_argument("--query", type=int, default=1, metavar="N",
                   help="Which heavy query to use: 1 (sort 1M), 2 (sort 100k), 3 (aggregate) — default: 1")
    p.add_argument("--cancel-after", type=float, default=2.0, metavar="SEC",
                   help="Seconds to wait after the task appears before cancelling (default: 2.0)")
    p.add_argument("--no-cancel", action="store_true",
                   help="Send the query but do not cancel — useful to confirm it runs long")
    args = p.parse_args()

    base_url = f"http://{args.host}:{args.port}"
    ppl_url  = f"{base_url}/_plugins/_ppl"

    if args.query < 1 or args.query > len(HEAVY_QUERIES):
        print(f"ERROR: --query must be 1–{len(HEAVY_QUERIES)}", file=sys.stderr)
        sys.exit(1)

    label, ppl_template = HEAVY_QUERIES[args.query - 1]
    ppl = ppl_template.replace("{index}", args.index)

    print(f"Query : {label}")
    print(f"PPL   : {ppl[:120]}")
    print(f"Target: {ppl_url}")
    print()

    # ---- Step 1: start the query in a background thread -------------------
    result = QueryResult()
    t = threading.Thread(target=send_query, args=(ppl_url, ppl, result), daemon=True)
    t0_wall = time.monotonic()
    t.start()
    print("Query started in background thread.")

    if args.no_cancel:
        print("--no-cancel set, waiting for completion...")
        t.join()
    else:
        # ---- Step 2: discover the task ID ---------------------------------
        print(f"Polling /_tasks for the new search task (up to 10 s)...")
        task_id = wait_for_task(base_url)

        if task_id is None:
            print("WARNING: could not find a matching task — query may have already finished.")
            print("Try a heavier query (--query 1) or a larger index.")
            t.join()
        else:
            print(f"Found task: {task_id}")

            # ---- Step 3: wait a moment, then cancel ----------------------
            if args.cancel_after > 0:
                print(f"Waiting {args.cancel_after:.1f} s before cancelling...")
                time.sleep(args.cancel_after)

            print(f"Cancelling task {task_id} ...")
            cancel_resp = cancel_task(base_url, task_id)
            print(f"Cancel response: HTTP {cancel_resp.get('status_code')}  "
                  f"body={cancel_resp.get('body', cancel_resp.get('error'))}")

            # ---- Step 4: wait for the query thread to return -------------
            print("Waiting for the query thread to return...")
            t.join(timeout=30)
            if t.is_alive():
                print("WARNING: query thread still running after 30 s timeout.")

    # ---- Report -----------------------------------------------------------
    elapsed_total = time.monotonic() - t0_wall
    print()
    print(f"{'='*60}")
    print(f"Result")
    print(f"{'='*60}")
    print(f"  Wall time     : {elapsed_total:.2f} s")
    print(f"  Query elapsed : {result.elapsed:.2f} s")

    if result.error:
        print(f"  Outcome       : ERROR — {result.error}")
    elif result.status_code is None:
        print(f"  Outcome       : (no response received)")
    elif result.status_code == 200:
        rows = result.body.get("datarows", [])
        print(f"  Outcome       : SUCCESS (HTTP 200) — {len(rows)} rows returned")
        print(f"  NOTE: Query finished before cancellation took effect.")
    else:
        # Cancelled queries typically return 4xx with a reason containing "cancelled"
        error_block = result.body.get("error", {})
        reason = (error_block.get("reason")
                  or error_block.get("details")
                  or str(result.body)[:300])
        print(f"  Outcome       : HTTP {result.status_code} — {reason}")
        if "cancel" in reason.lower() or "cancel" in str(result.body).lower():
            print(f"  ✓ Query was cancelled successfully.")
        else:
            print(f"  (Non-200 but reason does not mention cancellation)")


if __name__ == "__main__":
    main()
