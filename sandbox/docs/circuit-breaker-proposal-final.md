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
├── OS File Cache: variable
│    └── Must not be starved by native allocations
├── Native Budget: conservative (default: 25% of JVM max heap ≈ 8 GB)
│    └── Rust/DataFusion allocations live here
│    └── NEW: Native circuit breaker protects this
└── OS/kernel overhead: ~2 GB
```

These are **independent memory pools**. JVM heap pressure doesn't affect native memory availability and vice versa. Each pool needs its own protection, with its own limits.

## 3. Requirements

Taking a look at the existing Java-side Circuit breaker is useful to understand what we need to implement for Native memory circuit breaker.

### 3.1 How Java Circuit Breakers Work

OpenSearch's existing circuit breakers protect the JVM heap from OOM crashes. The hierarchy:

```
Parent Breaker (95% of JVM max heap in real-memory mode)
 ├── request      (60% of heap) — tracks memory used by search/aggregation operations
 ├── fielddata    (40% of heap) — tracks field data cache
 ├── in_flight    (100% of heap) — tracks in-flight network requests
 └── [custom]     (via CircuitBreakerPlugin)
```

**How it trips:** When an operation allocates memory, it calls `addEstimateBytesAndMaybeBreak(bytes)` on the relevant child breaker. The child uses a CAS loop to atomically increment its `used` counter. If `used × overhead > limit`, it trips. After the child check passes, `checkParentLimit()` is called — in real-memory mode, this checks actual JVM heap usage (via `ManagementFactory.getMemoryMXBean()`) against 95% of max heap.

**Key properties:**
- Child breakers track **total** memory across all concurrent operations (not per-request)
- The parent breaker provides a node-level safety net using actual heap usage
- Stats are exposed via `GET _nodes/stats/breaker` (limit, estimated usage, tripped count)
- Limits are dynamically configurable via cluster settings

### 3.2 Native CB requirements

The native circuit breaker must provide equivalent protection for Rust-side memory:

1. **Reject queries from consuming too much native memory** — analogous to the `request` child breaker, but tracking Rust allocations instead of JVM heap
2. **Track actual native memory usage** — analogous to the parent's real-memory mode, but using jemalloc stats instead of JVM MemoryMXBean
3. **Expose stats** — native breaker must appear in `_nodes/stats` alongside existing breakers, with the same fields (limit, estimated usage, tripped count)
4. **Be configurable** — limits must be adjustable via cluster settings, same as existing breakers
5. **Add minimal overhead** — benchmarked at <10% latency impact (Java CB overhead is negligible due to simple CAS operations; native CB must match this)

## 4. Design

### 4.1 Two-Level Check

Every DataFusion memory allocation (`QueryMemoryPool.try_grow(N)`) passes through two checks:

**Level 1 — Request check (total query memory budget):**
```
(total memory reserved by all active queries + N) × overhead > request_limit?
```
- Prevents the combined memory of all concurrent queries from exceeding the allotted budget
- Analogous to Java's `request` breaker (60% of heap) — which also tracks total across all requests, not per-request
- Mechanism: atomic Compare-And-Swap (lock-free, ~5ns)

**Level 2 — Node check (total native memory):**
```
jemalloc_total_allocated + N > node_limit?
```
- Prevents total Rust-side memory from exceeding the native budget
- Catches untracked allocations (I/O buffers, Tokio overhead, Arrow FFI)
- Mechanism: comparison against a cached jemalloc value (~1ns)

If either check fails, the allocation is rejected with a `CircuitBreakingException` (HTTP 429).

### 4.2 jemalloc Stats Caching

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
- Level 1 (request check) runs in real-time on every allocation — catches overuse immediately
- Total native memory changes gradually (not in single-allocation bursts)
- The `GreedyMemoryPool` hard ceiling remains as a safety net below the breaker

**Resilience:** The background task runs in an infinite loop with `catch_unwind` around the jemalloc call. If `epoch.advance()` panics or returns an error, the loop logs the failure and retries on the next tick — it never exits. If jemalloc is permanently unavailable, the cached value stays at 0 and Level 2 effectively becomes a no-op. The system degrades gracefully: Level 1 (CAS) + `GreedyMemoryPool` still provide protection.

### 4.3 Stats Visibility

OpenSearch exposes circuit breaker stats via `GET _nodes/stats/breaker`. Each registered breaker reports `limit_size_in_bytes`, `estimated_size_in_bytes` (current usage), `overhead`, and `tripped` count. To make native memory visible here, we use the existing `CircuitBreakerPlugin` extension point combined with a Java-side polling timer.

**How it works:**

1. **Registration:** `DataFusionPlugin` implements `CircuitBreakerPlugin` and returns `BreakerSettings("native_request", limit, overhead, MEMORY, TRANSIENT)` at startup. The `HierarchyCircuitBreakerService` creates a standard `ChildMemoryCircuitBreaker` for this name and places it in the breakers map.

2. **Callback:** The service calls `plugin.setCircuitBreaker(breaker)` back on the plugin, giving it a reference to the created `ChildMemoryCircuitBreaker` instance.

3. **Stats sync:** The plugin starts a `ScheduledExecutorService` (1-second interval) that calls `NativeBridge.getBreakerStats()` — an FFM downcall to Rust that returns `[request_used_bytes, total_used_bytes, child_tripped, node_tripped]`. The timer computes the delta and calls `addWithoutBreaking(delta)` on the Java child breaker, and increments the tripped counter for any new trips.

4. **Result:** When `_nodes/stats` is requested, the service iterates all breakers (including `native_request`), calls `getUsed()` on each, and renders the stats. Stats may lag by up to 1 second, which is acceptable for monitoring.

**Note:** The Java-side breaker is purely a stats container — all enforcement happens in Rust. Nothing on the Java side calls `addEstimateBytesAndMaybeBreak`.

**Why polling instead of push:** FFM upcalls can only be called from threads attached to the JVM. Rust Tokio worker threads are not attached, so calling an upcall from `check_and_reserve` crashes the JVM. The polling approach avoids this — the FFM downcall originates from a Java thread and is always safe.

### 4.4 Error Propagation

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

`release(bytes)`: decrements `request_used_bytes` (called on `shrink`).

### Java (`DataFusionPlugin`)

- Implements `CircuitBreakerPlugin` → registers `native_request` child breaker
- `setCircuitBreaker(breaker)` → stores reference, starts stats-sync timer
- Stats-sync timer (1s `ScheduledExecutorService`):
  - Calls `NativeBridge.getBreakerStats()` (FFM downcall → Rust returns stats struct)
  - Computes delta from last known value, calls `addWithoutBreaking(delta)` on the Java child breaker
  - Syncs tripped count via `circuitBreak()` calls for each new trip

### FFM Bridge

| Function | Called by | Purpose |
|----------|-----------|---------|
| `df_init_circuit_breaker(request_limit, node_limit, overhead)` | Startup | Initialize breaker + spawn jemalloc refresh timer on IO runtime |
| `df_get_breaker_stats(out_ptr)` | Java stats-sync timer (1s) | Returns `[request_used, total_used, child_tripped, node_tripped]` |
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
