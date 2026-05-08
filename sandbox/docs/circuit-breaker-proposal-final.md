# Native Circuit Breaker for DataFusion — Design Proposal

## 1. Problem

OpenSearch's circuit breakers prevent out-of-memory crashes by rejecting operations when memory usage exceeds configured limits. They track JVM heap allocations and trip when thresholds are exceeded.

With DataFusion, query execution moves to Rust. Rust allocates memory via jemalloc, completely outside the JVM heap. The existing circuit breakers cannot see or limit this memory. A DataFusion query can exhaust all available native memory without any breaker tripping.

## 2. Memory Model

On Amazon OpenSearch Service, JVM heap is pre-allocated at startup (`-Xms == -Xmx`, typically 50% of instance RAM). It does not grow or shrink. The remaining memory is shared between native allocations and the OS file system cache (which OpenSearch relies on heavily for performance):

```
Instance RAM (e.g., 64 GB)
├── JVM Heap: 32 GB (fixed, pre-allocated, managed by GC)
│    └── Existing circuit breakers protect this
├── OS File Cache: variable (critical for search performance)
│    └── Must not be starved by native allocations
├── Native Budget: conservative (default: 25% of JVM max heap ≈ 8 GB)
│    └── Rust/DataFusion allocations live here
│    └── NEW: Native circuit breaker protects this
└── OS/kernel overhead: ~2 GB
```

The native budget is intentionally conservative because OpenSearch depends on the OS file system cache being resident in RAM for read performance. Setting native memory too high would evict cached file pages and degrade search latency.

These are **independent memory pools**. JVM heap pressure doesn't affect native memory availability and vice versa. Each pool needs its own protection, with its own limits.

## 3. Requirements

The native circuit breaker must:

1. **Reject queries that consume too much native memory** — prevent a single query (or the sum of all queries) from exhausting the native budget
2. **Track actual native memory usage** — not just what DataFusion operators declare, but total Rust-side allocations (via jemalloc)
3. **Expose stats** — operators must see native breaker stats in `_nodes/stats` alongside existing breakers
4. **Be configurable** — limits must be adjustable via cluster settings
5. **Add minimal overhead** — benchmarked at <10% latency impact

## 4. Design

### 4.1 Two-Level Check

Every DataFusion memory allocation (`QueryMemoryPool.try_grow(N)`) passes through two checks:

**Level 1 — Request check (per-query budget):**
```
(sum of all active query reservations + N) × overhead > request_limit?
```
- Prevents queries from consuming more than their allotted share
- Analogous to Java's `request` breaker (60% of heap)
- Mechanism: atomic Compare-And-Swap (lock-free, ~5ns)

**Level 2 — Node check (total native memory):**
```
jemalloc_total_allocated + N > node_limit?
```
- Prevents total Rust-side memory from exceeding the native budget
- Catches untracked allocations (I/O buffers, Tokio overhead, Arrow FFI)
- Mechanism: comparison against a cached jemalloc value (~1ns)

If either check fails, the allocation is rejected with a `CircuitBreakingException` (HTTP 429).

### 4.2 Stats Push (Java Upcall)

After every DataFusion memory allocation call (whether it trips or not), Rust pushes current stats to Java via a lightweight FFM upcall:

```
Rust check_and_reserve(N):
  1. Level 1 check (CAS)
  2. Level 2 check (cached jemalloc comparison)
  3. Push stats to Java → sets request_used_bytes and tripped counts in Java child breaker
```

The upcall simply writes values into the Java-side `ChildMemoryCircuitBreaker` — no `checkParentLimit`, no FFM downcall back to Rust. This gives real-time stats visibility in `_nodes/stats` without any expensive operations.

### 4.3 jemalloc Stats Caching

Reading jemalloc stats requires `epoch.advance()` which costs ~1-10μs — too expensive per allocation. A background Tokio task refreshes the cached value every 1 second:

```
Rust (spawned on IO runtime during df_init_circuit_breaker):
  loop {
      sleep(1 second)
      → jemalloc epoch.advance()
      → read stats.allocated
      → store in cached_total_bytes atomic
  }
```

The Level 2 check reads this cached value. Staleness of up to 1 second is acceptable because:
- Level 1 (request check) runs in real-time on every allocation — catches per-query overuse immediately
- Total native memory changes gradually (not in single-allocation bursts)
- The `GreedyMemoryPool` hard ceiling remains as a safety net below the breaker

**Resilience:** The background task runs in an infinite loop with `catch_unwind` around the jemalloc call. If `epoch.advance()` panics or returns an error, the loop logs the failure and retries on the next tick — it never exits. If jemalloc is permanently unavailable, the cached value stays at 0 and Level 2 effectively becomes a no-op. The system degrades gracefully: Level 1 (CAS) + `GreedyMemoryPool` still provide protection.

### 4.4 Stats Visibility

OpenSearch exposes circuit breaker stats via `GET _nodes/stats/breaker`. Each registered breaker reports `limit_size_in_bytes`, `estimated_size_in_bytes` (current usage), `overhead`, and `tripped` count. To make native memory visible here, we use the existing `CircuitBreakerPlugin` extension point.

**How it works:**

1. **Registration:** `DataFusionPlugin` implements `CircuitBreakerPlugin` and returns `BreakerSettings("native_request", limit, overhead, MEMORY, TRANSIENT)` at startup. The `HierarchyCircuitBreakerService` creates a standard `ChildMemoryCircuitBreaker` for this name and places it in the breakers map.

2. **Callback:** The service calls `plugin.setCircuitBreaker(breaker)` back on the plugin, giving it a reference to the created `ChildMemoryCircuitBreaker` instance.

3. **Stats push:** The plugin registers a lightweight FFM upcall with Rust. After every `check_and_reserve`/`release` in Rust, this upcall fires and updates the Java-side `ChildMemoryCircuitBreaker`'s internal counter via `addWithoutBreaking(delta)`. This keeps the Java breaker's `getUsed()` in real-time sync with Rust's `request_used_bytes`.

4. **Result:** When `_nodes/stats` is requested, the service iterates all breakers (including `native_request`), calls `getUsed()` on each, and renders the stats. The `native_request` breaker reports real-time Rust memory usage without any additional FFM call at stats-read time.

**Note:** The `ChildMemoryCircuitBreaker` created by the service has `addEstimateBytesAndMaybeBreak` available, but nothing on the Java side calls it — all enforcement happens in Rust. The Java-side breaker is purely a stats container whose counter is kept in sync by the upcall.

### 4.5 Error Propagation

When the breaker trips, Rust encodes the error as:
```
CB:<bytes_wanted>:<bytes_limit>:<current_used>:<human_message>
```

Java-side parsing converts this to `CircuitBreakingException` with HTTP 429 status, matching the format of existing Java circuit breaker errors.

## 5. Limits & Configuration

| Setting | Default | Dynamic | Description |
|---------|---------|---------|-------------|
| `indices.breaker.native_request.limit` | Same as `datafusion.memory_pool_limit_bytes` | Yes | Level 1: max memory for all active DataFusion queries combined |
| `indices.breaker.native_request.overhead` | `1.0` | Yes | Multiplier applied to allocations before checking Level 1 limit |
| `indices.breaker.native_request.node_limit` | `datafusion.memory_pool_limit_bytes × 1.5` | Yes | Level 2: max total Rust-side memory (includes untracked allocations) |

**How defaults are derived:**
- `request_limit` = `datafusion.memory_pool_limit_bytes` (defaults to 25% of JVM max heap, e.g., 8GB on a 32GB heap node). This is intentionally conservative to preserve OS file cache for search performance.
- `node_limit` = `request_limit × 1.5` — allows 50% headroom above the query pool for untracked allocations (reader buffers, Tokio, Arrow FFI, write-path memory).
- `overhead` = `1.0` — no multiplier by default (DataFusion's `MemoryPool` tracking is accurate for operator memory).

**Relationship to existing limits:**
```
GreedyMemoryPool limit (hard ceiling, safety net)
  ≥ request_limit (Level 1 — trips first with proper error)
      node_limit (Level 2 — catches total native pressure)
```

The `GreedyMemoryPool` limit should equal `request_limit`. The breaker trips first (with proper `CircuitBreakingException`), and the pool acts as a hard safety net if the breaker is misconfigured.

## 6. Implementation

### Rust (`circuit_breaker.rs`)

Single `NativeCircuitBreaker` struct:
- `request_used_bytes: AtomicUsize` — sum of active query MemoryPool reservations
- `cached_total_bytes: AtomicUsize` — jemalloc total, refreshed by background thread
- `request_limit: AtomicUsize` — Level 1 limit
- `node_limit: AtomicUsize` — Level 2 limit
- `overhead_millionths: AtomicU64` — fixed-point overhead multiplier
- `child_tripped: AtomicU64`, `node_tripped: AtomicU64` — trip counters

Entry point `check_and_reserve(bytes)`:
1. Level 1: `fetch_update` CAS on `request_used_bytes`
2. Level 2: `cached_total_bytes + bytes > node_limit?`
3. On failure at either level: roll back Level 1 reservation, return error
4. Push stats to Java via upcall (sets current `request_used_bytes` and trip counts in Java child breaker)

`release(bytes)`: decrements `request_used_bytes` (called on `shrink`). Also pushes updated stats to Java.

### Java (`DataFusionPlugin`)

- Implements `CircuitBreakerPlugin` → registers `native_request` child breaker
- `setCircuitBreaker(breaker)` → stores reference to the `ChildMemoryCircuitBreaker`
- Registers a stats-push upcall callback that Rust calls after every `check_and_reserve`/`release`:
  - Computes delta from last known value, calls `addWithoutBreaking(delta)` on the Java child breaker
  - This keeps `_nodes/stats` in real-time sync with Rust

### FFM Bridge

| Function | Called by | Purpose |
|----------|-----------|---------|
| `df_init_circuit_breaker(request_limit, node_limit, overhead)` | Startup | Initialize breaker + spawn jemalloc refresh timer on IO runtime |
| `df_register_stats_callback(fn_ptr)` | Startup | Register Java upcall for real-time stats push |
| `df_set_breaker_limit(limit)` | Cluster setting change | Update request_limit |
| `df_set_breaker_node_limit(limit)` | Cluster setting change | Update node_limit |

## 7. Benchmarked Performance

| Configuration | Overhead vs baseline |
|--------------|---------------------|
| Level 1 only (CAS check) | 0-6% |
| Level 1 + Level 2 (cached jemalloc) | 0-6% |
| Level 1 + Level 2 + Java upcall (stats push) | 0-6% |
| Level 2 with uncached jemalloc | 200-1400% ❌ |

The stats-push upcall is lightweight (just setting a value in Java, no FFM downcall back to Rust) and adds negligible overhead. The 1-second jemalloc cache is the critical optimization.
