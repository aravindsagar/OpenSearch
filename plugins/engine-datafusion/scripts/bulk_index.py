#!/usr/bin/env python3
"""
Bulk index synthetic documents into OpenSearch for DataFusion engine testing.

Runs parallel indexing workers while monitoring CPU/memory/disk and
pausing workers when any threshold is breached.

Usage:
    python3 bulk_index.py [options]

Requirements:
    pip install opensearch-py psutil
"""

import argparse
import json
import logging
import random
import signal
import string
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import psutil
from opensearchpy import OpenSearch, helpers, RequestError, ConnectionError as OSConnectionError

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bulk_index")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class Config:
    host: str = "localhost"
    port: int = 9200
    index: str = "df-test"
    total_docs: int = 1_000_000
    batch_size: int = 500          # docs per bulk request
    workers: int = 4               # parallel bulk threads
    # Resource thresholds — pause when ANY is exceeded
    max_cpu_pct: float = 75.0      # % over 1-second window
    max_mem_pct: float = 80.0      # % of total RAM used
    max_disk_pct: float = 85.0     # % of disk partition used
    monitor_interval: float = 2.0  # seconds between resource checks
    resume_hysteresis: float = 5.0 # must drop this far below threshold to resume
    # Document shape
    text_fields: int = 5
    int_fields: int = 5
    float_fields: int = 3
    text_word_count: int = 50


# ---------------------------------------------------------------------------
# Resource monitor
# ---------------------------------------------------------------------------
class ResourceMonitor:
    """
    Background thread that checks CPU/memory/disk every `interval` seconds.
    Sets `pause_event` when any threshold is exceeded; clears it when all
    drop below (threshold - hysteresis).
    """

    def __init__(self, config: Config, pause_event: threading.Event, stop_event: threading.Event):
        self._cfg = config
        self._pause = pause_event
        self._stop = stop_event
        self._disk_path = "/"
        self._thread = threading.Thread(target=self._run, name="resource-monitor", daemon=True)
        self.last_cpu = 0.0
        self.last_mem = 0.0
        self.last_disk = 0.0

    def start(self):
        self._thread.start()

    def _over_threshold(self) -> Optional[str]:
        """Return a description of the first breached threshold, or None."""
        cpu = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory().percent
        disk = psutil.disk_usage(self._disk_path).percent
        self.last_cpu, self.last_mem, self.last_disk = cpu, mem, disk

        if cpu > self._cfg.max_cpu_pct:
            return f"CPU {cpu:.1f}% > {self._cfg.max_cpu_pct}%"
        if mem > self._cfg.max_mem_pct:
            return f"MEM {mem:.1f}% > {self._cfg.max_mem_pct}%"
        if disk > self._cfg.max_disk_pct:
            return f"DISK {disk:.1f}% > {self._cfg.max_disk_pct}%"
        return None

    def _under_resume(self) -> bool:
        """Return True when all metrics are back below (threshold - hysteresis)."""
        h = self._cfg.resume_hysteresis
        return (
            self.last_cpu  < self._cfg.max_cpu_pct  - h and
            self.last_mem  < self._cfg.max_mem_pct  - h and
            self.last_disk < self._cfg.max_disk_pct - h
        )

    def _run(self):
        while not self._stop.is_set():
            reason = self._over_threshold()
            if reason and not self._pause.is_set():
                self._pause.set()
                log.warning("PAUSE  — %s  (cpu=%.1f%% mem=%.1f%% disk=%.1f%%)",
                            reason, self.last_cpu, self.last_mem, self.last_disk)
            elif self._pause.is_set() and self._under_resume():
                self._pause.clear()
                log.info("RESUME — resources back to normal  "
                         "(cpu=%.1f%% mem=%.1f%% disk=%.1f%%)",
                         self.last_cpu, self.last_mem, self.last_disk)
            time.sleep(self._cfg.monitor_interval)


# ---------------------------------------------------------------------------
# Document generation
# ---------------------------------------------------------------------------
WORDS = [
    "opensearch", "datafusion", "query", "index", "shard", "segment", "lucene",
    "parquet", "arrow", "substrait", "batch", "record", "filter", "aggregate",
    "engine", "cluster", "replica", "mapping", "field", "document", "search",
    "score", "boost", "analyzer", "tokenizer", "pipeline", "ingest", "plugin",
]

def _rand_text(word_count: int) -> str:
    return " ".join(random.choices(WORDS, k=word_count))

def make_doc(cfg: Config) -> dict:
    doc = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "id": "".join(random.choices(string.ascii_lowercase + string.digits, k=12)),
    }
    for i in range(cfg.text_fields):
        doc[f"text_{i}"] = _rand_text(cfg.text_word_count)
    for i in range(cfg.int_fields):
        doc[f"int_{i}"] = random.randint(0, 1_000_000)
    for i in range(cfg.float_fields):
        doc[f"float_{i}"] = random.uniform(0.0, 1_000_000.0)
    return doc


def batch_actions(index: str, cfg: Config, start: int, count: int):
    for _ in range(count):
        yield {"_index": index, "_source": make_doc(cfg)}


# ---------------------------------------------------------------------------
# Indexing worker
# ---------------------------------------------------------------------------
class Stats:
    def __init__(self):
        self._lock = threading.Lock()
        self.indexed = 0
        self.errors = 0
        self.paused_secs = 0.0
        self.start = time.monotonic()

    def add(self, indexed: int, errors: int, paused: float):
        with self._lock:
            self.indexed += indexed
            self.errors += errors
            self.paused_secs += paused

    def report(self, total: int, monitor: ResourceMonitor):
        elapsed = time.monotonic() - self.start
        rate = self.indexed / max(elapsed, 1)
        pct = 100 * self.indexed / total
        log.info(
            "Progress: %d/%d (%.1f%%)  rate=%.0f docs/s  errors=%d  "
            "paused=%.1fs  cpu=%.1f%%  mem=%.1f%%  disk=%.1f%%",
            self.indexed, total, pct, rate, self.errors,
            self.paused_secs, monitor.last_cpu, monitor.last_mem, monitor.last_disk,
        )


def worker(
    client: OpenSearch,
    cfg: Config,
    doc_queue: "list[int]",       # list of batch start offsets owned by this worker
    pause_event: threading.Event,
    stop_event: threading.Event,
    stats: Stats,
):
    """Index batches from doc_queue, pausing when pause_event is set."""
    for start in doc_queue:
        if stop_event.is_set():
            break

        # Wait while resources are high
        paused_start = None
        while pause_event.is_set() and not stop_event.is_set():
            if paused_start is None:
                paused_start = time.monotonic()
            time.sleep(0.5)
        paused_duration = (time.monotonic() - paused_start) if paused_start else 0.0

        if stop_event.is_set():
            break

        actions = list(batch_actions(cfg.index, cfg, start, cfg.batch_size))
        try:
            ok, errors = helpers.bulk(client, actions, raise_on_error=False, stats_only=True)
            stats.add(ok, errors, paused_duration)
        except (OSConnectionError, Exception) as exc:
            log.error("Bulk request failed (start=%d): %s", start, exc)
            stats.add(0, len(actions), paused_duration)


# ---------------------------------------------------------------------------
# Index setup
# ---------------------------------------------------------------------------
def ensure_index(client: OpenSearch, cfg: Config):
    """Create index with a mapping suited for the synthetic documents.

    index.optimized.enabled activates the composite engine: Lucene handles
    writes while the DataFusion plugin reads from the Parquet files it produces.
    Without this setting the index uses the standard Lucene-only engine and
    DataFusion is never involved.
    """
    if client.indices.exists(index=cfg.index):
        # Verify it was created with the composite engine enabled
        settings = client.indices.get_settings(index=cfg.index)
        optimized = (
            settings.get(cfg.index, {})
            .get("settings", {})
            .get("index", {})
            .get("optimized", {})
            .get("enabled", "false")
        )
        secondary = (
            settings.get(cfg.index, {})
            .get("settings", {})
            .get("index", {})
            .get("composite", {})
            .get("secondary_data_formats", [])
        )
        if optimized != "true":
            log.warning(
                "Index '%s' exists but 'index.optimized.enabled' is not true — "
                "queries will NOT use DataFusion. Delete it and re-run to recreate.",
                cfg.index,
            )
        elif "Lucene" not in secondary:
            log.warning(
                "Index '%s' exists with composite engine but Lucene is not in "
                "secondary_data_formats.",
                cfg.index,
            )
        else:
            log.info("Index '%s' already exists with composite engine — appending documents", cfg.index)
        return

    props: dict = {
        "timestamp": {"type": "date"},
        "id":        {"type": "keyword"},
    }
    for i in range(cfg.text_fields):
        props[f"text_{i}"] = {"type": "text"}
    for i in range(cfg.int_fields):
        props[f"int_{i}"] = {"type": "integer"}
    for i in range(cfg.float_fields):
        props[f"float_{i}"] = {"type": "float"}

    client.indices.create(index=cfg.index, body={
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "30s",                        # reduce refresh overhead during bulk load
            "index.optimized.enabled": True,                  # composite engine: Lucene write + DataFusion read
            "index.composite.secondary_data_formats": ["Lucene"],  # also write Lucene segments alongside Parquet
        },
        "mappings": {"properties": props},
    })
    log.info("Created index '%s' with composite engine (DataFusion primary, Lucene secondary)", cfg.index)


# ---------------------------------------------------------------------------
# DataFusion verification
# ---------------------------------------------------------------------------
def verify_datafusion(client: OpenSearch, cfg: Config):
    """
    Confirm the index is going through DataFusion by inspecting two signals:

    1. Node stats: the DataFusion plugin exposes a /_nodes/datafusion endpoint.
       A non-empty response (or no 404) means the plugin is loaded.
    2. Index segments: search segments info for Parquet files on disk.
       If the composite engine wrote data, there will be .parquet segment files.
    """
    # --- Check 1: plugin loaded + version ---
    try:
        resp = client.transport.perform_request("GET", "/_plugins/datafusion/info")
        log.info("DataFusion plugin info: %s", json.dumps(resp, indent=2)[:400])
    except Exception as exc:
        log.warning(
            "/_plugins/datafusion/info failed (%s) — "
            "DataFusion plugin may not be installed or OpenSearch may not be running",
            exc,
        )

    # --- Check 2: index must have index.optimized.enabled=true ---
    try:
        settings = client.indices.get_settings(index=cfg.index)
        optimized = (
            settings.get(cfg.index, {})
            .get("settings", {})
            .get("index", {})
            .get("optimized", {})
            .get("enabled", "false")
        )
        secondary = (
            settings.get(cfg.index, {})
            .get("settings", {})
            .get("index", {})
            .get("composite", {})
            .get("secondary_data_formats", [])
        )
        if optimized == "true" and "Lucene" in secondary:
            log.info(
                "✓ composite engine active (optimized=true, secondary=[Lucene]) — "
                "writes go to Parquet + Lucene; Substrait queries use DataFusion"
            )
        elif optimized == "true":
            log.warning(
                "✓ optimized=true but Lucene is not in secondary_data_formats — "
                "add it if you need non-Substrait queries to work"
            )
        else:
            log.warning(
                "✗ index.optimized.enabled is not true — DataFusion is NOT being used. "
                "Delete the index and re-run the script to recreate it correctly."
            )
    except Exception as exc:
        log.warning("Could not verify index settings: %s", exc)

    # --- Check 3: Parquet files on disk ---
    log.info(
        "To confirm Parquet files were written, check the shard data directory:\n"
        "  find <opensearch-data-dir> -path '*/%s/*' -name '*.parquet' | head -10",
        cfg.index,
    )

    log.info(
        "Query routing summary:\n"
        "  Substrait (queryPlanIR present) → DataFusion reads Parquet\n"
        "  Standard _search / _count       → not supported on composite-engine indexes yet"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def parse_args() -> Config:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host",          default="localhost",  help="OpenSearch host (default: localhost)")
    p.add_argument("--port",          type=int, default=9200)
    p.add_argument("--index",         default="df-test",    help="Target index name")
    p.add_argument("--total-docs",    type=int, default=1_000_000, metavar="N")
    p.add_argument("--batch-size",    type=int, default=500,  metavar="N", help="Docs per bulk request")
    p.add_argument("--workers",       type=int, default=4,    metavar="N", help="Parallel bulk threads")
    p.add_argument("--max-cpu",       type=float, default=75.0, metavar="PCT")
    p.add_argument("--max-mem",       type=float, default=80.0, metavar="PCT")
    p.add_argument("--max-disk",      type=float, default=85.0, metavar="PCT")
    p.add_argument("--monitor-interval", type=float, default=2.0, metavar="SEC")
    p.add_argument("--text-fields",   type=int, default=5)
    p.add_argument("--int-fields",    type=int, default=5)
    p.add_argument("--float-fields",  type=int, default=3)
    p.add_argument("--text-words",    type=int, default=50,   help="Words per text field")

    a = p.parse_args()
    return Config(
        host=a.host, port=a.port, index=a.index,
        total_docs=a.total_docs, batch_size=a.batch_size, workers=a.workers,
        max_cpu_pct=a.max_cpu, max_mem_pct=a.max_mem, max_disk_pct=a.max_disk,
        monitor_interval=a.monitor_interval,
        text_fields=a.text_fields, int_fields=a.int_fields, float_fields=a.float_fields,
        text_word_count=a.text_words,
    )


def main():
    cfg = parse_args()

    client = OpenSearch(
        hosts=[{"host": cfg.host, "port": cfg.port}],
        http_compress=True,
        use_ssl=False,
        verify_certs=False,
        timeout=60,
        pool_maxsize=cfg.workers + 2,  # one per worker + headroom for verify/count calls
    )

    try:
        info = client.info()
        log.info("Connected to OpenSearch %s", info["version"]["number"])
    except Exception as exc:
        log.error("Cannot connect to %s:%d — %s", cfg.host, cfg.port, exc)
        sys.exit(1)

    ensure_index(client, cfg)

    # Divide work: list of batch start indices, one entry per bulk request
    num_batches = (cfg.total_docs + cfg.batch_size - 1) // cfg.batch_size
    all_starts = [i * cfg.batch_size for i in range(num_batches)]

    # Distribute batches round-robin across workers
    worker_queues: list[list[int]] = [[] for _ in range(cfg.workers)]
    for i, start in enumerate(all_starts):
        worker_queues[i % cfg.workers].append(start)

    pause_event = threading.Event()
    stop_event = threading.Event()
    stats = Stats()

    monitor = ResourceMonitor(cfg, pause_event, stop_event)
    monitor.start()

    def _sigint(sig, frame):
        log.warning("Interrupted — stopping workers gracefully...")
        stop_event.set()

    signal.signal(signal.SIGINT, _sigint)
    signal.signal(signal.SIGTERM, _sigint)

    log.info(
        "Starting: %d docs, batch=%d, workers=%d | "
        "limits: cpu<%.0f%% mem<%.0f%% disk<%.0f%%",
        cfg.total_docs, cfg.batch_size, cfg.workers,
        cfg.max_cpu_pct, cfg.max_mem_pct, cfg.max_disk_pct,
    )

    progress_interval = 10.0  # seconds between progress logs
    last_report = time.monotonic()

    with ThreadPoolExecutor(max_workers=cfg.workers, thread_name_prefix="indexer") as pool:
        futures = [
            pool.submit(worker, client, cfg, worker_queues[i], pause_event, stop_event, stats)
            for i in range(cfg.workers)
        ]
        while not all(f.done() for f in futures):
            time.sleep(1)
            if time.monotonic() - last_report >= progress_interval:
                stats.report(cfg.total_docs, monitor)
                last_report = time.monotonic()
            if stop_event.is_set():
                break

        # Wait for threads to finish
        for f in futures:
            f.result()

    stop_event.set()  # stop monitor thread

    elapsed = time.monotonic() - stats.start
    log.info(
        "Done: indexed=%d errors=%d elapsed=%.1fs avg_rate=%.0f docs/s paused=%.1fs",
        stats.indexed, stats.errors, elapsed,
        stats.indexed / max(elapsed, 1),
        stats.paused_secs / cfg.workers,  # per-worker average
    )

    # Restore normal refresh
    client.indices.put_settings(index=cfg.index, body={"index": {"refresh_interval": "1s"}})
    client.indices.refresh(index=cfg.index)
    # Use index stats (reads segment metadata directly) — NOT _count, which goes through
    # the Lucene query phase and fails on composite-engine indexes because
    # DatafusionContext.searcher() is null for non-Substrait requests.
    stats = client.indices.stats(index=cfg.index, metric="docs")
    doc_count = stats["indices"][cfg.index]["total"]["docs"]["count"]
    log.info("Index '%s' now has %d documents", cfg.index, doc_count)

    verify_datafusion(client, cfg)


if __name__ == "__main__":
    main()
