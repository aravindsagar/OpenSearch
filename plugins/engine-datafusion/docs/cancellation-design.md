# DataFusion Query Cancellation Design

## Runtime Architecture

The DataFusion engine uses two separate Tokio runtimes:

- **`io_runtime`** (`datafusion-io-*` threads) — handles all JNI entry points, async setup work, and `streamNext` polling. This is a standard `tokio::Runtime`.
- **`cpu_executor`** (`datafusion-cpu-*` threads) — a [`DedicatedExecutor`](../jni/src/executor.rs) that runs CPU-bound DataFusion plan execution. Kept separate to avoid blocking I/O handling under concurrent load.

These are initialized once at service start in [`runtime_manager.rs`](../jni/src/runtime_manager.rs) and are process-lifetime singletons.

---

## Tasks in Flight During a Query

A single shard-level query fans out into several concurrent Tokio tasks:

| # | Runtime | Spawned by | Work |
|---|---------|-----------|------|
| 1 | `io_runtime` | `executeQueryPhaseAsync` JNI | Substrait decode, physical plan build, returns stream pointer to Java |
| 2 | `cpu_executor` | `CrossRtStream` constructor | Polls `SendableRecordBatchStream`, forwards batches over MPSC channel |
| 3 | `io_runtime` | `streamNext` JNI (one per call) | Polls `CrossRtStream.try_next()`, delivers batch to Java via `ActionListener` |

DataFusion also spawns internal tasks (one per `target_partitions`) inside `execute_stream`, but those are managed by DataFusion itself and are cancelled when task 2 is cancelled.

---

## TL;DR

Each query is mapped to a `CancellationToken` stored in a global `ACTIVE_QUERIES` registry keyed by `ShardSearchContextId`. To cancel a query, Java calls `cancelQuery(taskId)`, which fires the token.

The token is raced inside `executeQueryPhaseAsync` (task 1) and `streamNext` (task 3) using `select!`. When the token fires, both JNI methods return early to Java — the former with an error, the latter with an EOF signal. In both cases Java's normal exit path calls `streamClose`, which drops the `CrossRtStream`. Dropping `CrossRtStream` drops its internal `driver` future, which owns the `JoinSet` for the cpu_executor task (task 2). `JoinSet::drop` calls `abort_all()`, and DataFusion's operators yield at the next Tokio budget boundary — cancelling all remaining work.

One invariant must hold: `streamClose` must always be called **after** the current `loadNextBatch()` `CompletableFuture` has completed. The `select!` ensures that future completes quickly when cancelled, but cancellation code must not call `streamClose` directly — it must let the batch loop's normal exit path do it.

---

## Implementation: ACTIVE_QUERIES Registry

A global `DashMap` keyed by `ShardSearchContextId` (a stable `long` passed from Java as `task_id`) holds a `CancellationToken` per in-flight query:

```rust
// lib.rs
pub struct QueryContext {
    pub cancellation_token: CancellationToken,
}

static ACTIVE_QUERIES: Lazy<DashMap<i64, QueryContext>> = Lazy::new(DashMap::new);
```

An entry is inserted when `executeQueryPhaseAsync` begins and removed when `streamClose` is called (normal path) or when the query errors before returning a stream.

### Registry lifetime

```
executeQueryPhaseAsync called
  → ACTIVE_QUERIES.insert(task_id, QueryContext::new())

Normal end-of-stream:
  streamClose called → ACTIVE_QUERIES.remove(task_id)

Cancelled during setup (task 1):
  select! fires → ACTIVE_QUERIES.remove(task_id) (inside select! arm)

Cancelled during streaming (task 3):
  cancelQuery(task_id) → token.cancel()
  → streamNext select! fires → Java gets EOF → Java calls streamClose
  → ACTIVE_QUERIES.remove(task_id)
```

---

## Cancellation via `select!`

### Query-phase setup window (task 1)

Between the JNI call to `executeQueryPhaseAsync` and the `CompletableFuture` completing, Java has no stream pointer and cannot call `streamClose`. The `CancellationToken` covers this window by racing against the setup work directly:

```rust
// executeQueryPhaseAsync — io_runtime task body
ACTIVE_QUERIES.insert(task_id, QueryContext::new());
select! {
    result = execute_query_with_cross_rt_stream(...) => {
        if result.is_err() { ACTIVE_QUERIES.remove(&task_id); }
        result
    }
    _ = token.cancelled() => {
        ACTIVE_QUERIES.remove(&task_id);
        Err(DataFusionError::Execution(format!("Query {} cancelled", task_id)))
    }
}
```

If cancellation fires during Substrait decode or physical plan build, the `select!` branch returns an error immediately. The `CompletableFuture` completes exceptionally — no stream pointer is ever returned to Java, so `streamClose` is never called.

### Streaming (task 3)

`streamNext` is added `task_id` to its JNI signature (same as `streamClose` already has), looks up the token, and races `try_next` against cancellation:

```rust
// streamNext — proposed change
select! {
    result = stream.try_next() => { /* normal path */ }
    _ = token.cancelled()      => Ok(0)  // return EOF signal to Java
}
```

When the token fires, `token.cancelled()` wins the `select!`. The io_runtime task returns `Ok(0)`, Java's `CompletableFuture<Boolean>` completes with `false`, and the batch loop exits naturally and calls `streamClose`.

### Triggering cancellation from Java

`cancelQuery(long taskId)` calls `token.cancel()` on the registered token. This is a fast, non-blocking operation — it sets an atomic flag and wakes any futures waiting on `token.cancelled()`.

Java calls `cancelQuery` from three sites:

- **`SearchShardTask.onCancelled()` hook** — the primary, immediate path. `DatafusionEngine.executeQueryPhaseAsync` registers `() -> NativeBridge.cancelQuery(contextId)` on the task via `task.setCancellationListener(...)` before launching the query. Any cancellation signal (HTTP disconnect, `POST /_tasks/<id>/_cancel`, timeout) immediately fires the token, regardless of which execution phase the query is in. The listener is cleared when the async operation completes to avoid retaining a reference to a completed query.
- **`loadNextBatch()` batch loop** — secondary poll-based check. If `context.isCancelled()` returns true between batches, `cancelQuery` is called as a belt-and-suspenders fallback.
- **`DatafusionContext.doClose()`** — covers teardown paths (node shutdown, context eviction) where the task may not have been explicitly cancelled.

In all cases, `streamClose` is left to the batch loop's normal exit path. Cancellation code must not call `streamClose` directly.

---

## Drop-Based Cancellation of the cpu_executor Task (task 2)

Cancelling the cpu_executor task requires no explicit signal — it happens automatically when `streamClose` drops the `CrossRtStream`.

### The JoinSet mechanism

[`DedicatedExecutor::spawn`](../jni/src/executor.rs) uses a `JoinSet` to implement cancel-on-drop:

```rust
// executor.rs
pub fn spawn<T>(&self, task: T) -> impl Future<...> {
    let mut join_set = JoinSet::new();
    join_set.spawn_on(task, &handle);  // task starts running on cpu_executor
    async move {
        join_set.join_next().await     // JoinSet is owned by this returned future
        ...
    }.boxed()
}
```

The returned `BoxFuture` owns the `JoinSet`. Dropping that future drops the `JoinSet`, which calls `abort_all()`.

### How CrossRtStream holds that future

[`CrossRtStream`](../jni/src/cross_rt_stream.rs) stores the result of `exec.spawn(fut)` — pending and unawaited — as its `driver: BoxFuture<'static, ()>` field:

```rust
// cross_rt_stream.rs
pub struct CrossRtStream {
    driver: BoxFuture<'static, ()>,  // owns the JoinSet future from exec.spawn()
    inner: ReceiverStream<...>,      // MPSC channel receiver
    ...
}
```

Rust drops struct fields in declaration order, so `driver` drops before `inner`. Dropping `driver` fires `abort_all()`. Dropping `inner` (the MPSC `Receiver`) closes the channel, causing `tx_captured.send()` in the cpu task's loop to fail — a secondary stop signal that reinforces the abort.

### The full cascade from `streamClose`

```
streamClose → drop RecordBatchStreamAdapter<CrossRtStream>
  → drop CrossRtStream.driver (BoxFuture)
      → drop JoinSet future owned by driver
          → JoinSet::drop() → abort_all()
              → cpu_executor task receives Tokio abort signal
                  → DataFusion operators yield at next budget boundary → task cancelled
  → drop CrossRtStream.inner (MPSC Receiver)
      → channel closed → tx_captured.send() fails → cpu task loop returns
```

DataFusion's built-in source operators (`ParquetExec`, etc.) participate in Tokio's cooperative scheduling via `tokio::task::consume_budget()`. This ensures that once `abort_all()` fires, the cpu task stops within a bounded number of polling cycles — not instantaneously, but promptly. See the [DataFusion cancellation blog post](https://datafusion.apache.org/blog/2025/06/30/cancellation/) for a detailed explanation of how DataFusion uses Tokio's task budget system.

---

## Cancellation Outcome by Task

| Task | Cancelled by | Mechanism |
|------|-------------|-----------|
| io_runtime setup task (task 1) | `token.cancel()` → `select!` fires | Returns error to Java immediately |
| cpu_executor stream driver (task 2) | `streamClose` → drop `CrossRtStream.driver` → `JoinSet::abort_all()` | Tokio task abort, DF operators yield at budget boundary |
| io_runtime `streamNext` task (task 3) | `token.cancel()` → `select!` fires | Returns EOF (0) to Java; `streamClose` follows from batch loop exit |
| DataFusion internal partition tasks | Cascade from task 2 abort | Managed by DataFusion's `execute_stream` |

---

## Known Limitations

**Fetch phase mid-execution.** `executeFetchPhase` uses `block_on` and cannot be interrupted once started. A pre-cancellation check at the start of the JNI method handles the case where cancellation was signalled before `executeFetchPhase` begins. Cancellation mid-fetch requires converting it to async (tracked separately).

**Parquet row-group granularity.** Tokio task abort delivers at `consume_budget()` boundaries, which in DataFusion's Parquet reader fall at row-group boundaries. A single very large row group may not be interrupted mid-read. Cancellation is prompt, not instantaneous.
