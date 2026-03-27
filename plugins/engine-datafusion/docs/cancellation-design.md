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
| 2a–2n | `cpu_executor` | DataFusion `execute_stream` (inside task 2) | One task per `target_partitions`; reads Parquet row groups, sends batches into the plan |
| 3 | `io_runtime` | `streamNext` JNI (one per call) | Polls `CrossRtStream.try_next()`, delivers batch to Java via `ActionListener` |

Tasks 2a–2n are spawned by DataFusion's `RecordBatchReceiverStreamBuilder` via `JoinSet::spawn` (not detached `tokio::spawn`). The `JoinSet` is embedded inside the `SendableRecordBatchStream` returned by `CoalescePartitionsExec`. When task 2's stream is dropped (triggered by task 2 being aborted), the `JoinSet` is dropped, firing `abort_all()` on partition tasks. Each then stops at its next `.await` point. We hold no direct handle to tasks 2a–2n and cannot observe when they have fully completed.

---

## TL;DR

**io_runtime tasks:**
Every query is registered in a global `ACTIVE_QUERIES` registry keyed by `ShardSearchContextId`, regardless of whether it is cancellable. For queries whose Java task is a `CancellableTask`, a `CancellationToken` is also stored in the registry entry. Java signals this via an `is_cancellable` flag passed to `executeQueryPhaseAsync`. To cancel a query, Java calls `cancelQuery(taskId)`, which fires the token.

The token is raced inside `executeQueryPhaseAsync` (task 1) and `streamNext` (task 3) using `select!`. When the token fires, both JNI methods return early to Java — the former with an error, the latter with an EOF signal. For non-cancellable queries the cancellation arm is replaced with `futures::future::pending()`, which never fires, so the `select!` reduces to a plain `await`.

**cpu_executor tasks:**
`cancelQuery` also calls `AbortHandle::abort()` on the cpu task (task 2) if it is already running, freeing the cpu thread without waiting for Java to call `streamClose`. The `AbortHandle` is stored in `QueryContext.cpu_abort_handle` after `executeQueryPhaseAsync` creates the stream.

`AbortHandle::abort()` is **non-blocking and fire-and-forget** — it sets a cancellation flag and returns immediately. Task 2 stops at its next `await` point. When task 2's future is dropped, it drops the `SendableRecordBatchStream`, which drops the `JoinSet` inside DataFusion's `RecordBatchReceiverStreamBuilder`, firing `abort_all()` on partition tasks 2a–2n. Each partition task then stops at its own next `.await` point. We hold no direct handle to tasks 2a–2n and cannot observe when they have fully completed.

As a safety net, `streamClose` still drops the `CrossRtStream`, which drops its `driver` future, which owns the `JoinSet`. `JoinSet::drop` calls `abort_all()` — so the cpu task is cancelled even if `cancelQuery` was never called (e.g., normal end-of-stream close). DataFusion's operators yield at the next Tokio budget boundary (see https://datafusion.apache.org/blog/2025/06/30/cancellation/).

---

## Implementation: ACTIVE_QUERIES Registry

A global `DashMap` keyed by `ShardSearchContextId` (a stable `long` passed from Java as `task_id`) tracks every in-flight query, regardless of whether it is cancellable:

```rust
// lib.rs
pub struct QueryContext {
    /// Some only when the associated Java task is a CancellableTask.
    /// None for non-cancellable queries — the token is never created and
    /// the select! cancellation arm uses futures::future::pending() instead.
    pub cancellation_token: Option<CancellationToken>,
    /// Stored after executeQueryPhaseAsync succeeds. None until then, None
    /// if the executor is shutting down, and None for non-cancellable queries.
    pub cpu_abort_handle: OnceLock<Option<tokio::task::AbortHandle>>,
}

static ACTIVE_QUERIES: Lazy<DashMap<i64, QueryContext>> = Lazy::new(DashMap::new);
```

Java passes an `is_cancellable: jboolean` flag to `executeQueryPhaseAsync`. The Rust side creates a `CancellationToken` only when this flag is true. `ACTIVE_QUERIES` always has an entry for every in-flight query so it can serve as a general registry for lifecycle tracking, active query counts, and future observability — independent of whether cancellation is wired up.

An entry is inserted when `executeQueryPhaseAsync` begins and removed when `streamClose` is called (normal path) or when the query errors before returning a stream.

### Registry lifetime

```
executeQueryPhaseAsync called (is_cancellable=true)
  → ACTIVE_QUERIES.insert(task_id, QueryContext { cancellation_token: Some(token), .. })

executeQueryPhaseAsync called (is_cancellable=false)
  → ACTIVE_QUERIES.insert(task_id, QueryContext { cancellation_token: None, .. })

Normal end-of-stream (cancellable or not):
  streamClose called → ACTIVE_QUERIES.remove(task_id)

Cancelled during setup (task 1, cancellable only):
  select! cancellation arm fires → ACTIVE_QUERIES.remove(task_id)

Cancelled during streaming (task 3, cancellable only):
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
let cancellation_token = is_cancellable.then(CancellationToken::new);
ACTIVE_QUERIES.insert(task_id, QueryContext::new(cancellation_token.clone()));

// Extract the cancellation future outside select! for readability.
// For non-cancellable queries cancellation_token is None and pending() never
// resolves, so the cancellation arm in the select! below can never fire.
let cancellation_fut = async {
    match cancellation_token {
        Some(ref token) => token.cancelled().await,
        None => std::future::pending().await,
    }
};

select! {
    result = execute_query_with_cross_rt_stream(...) => {
        if result.is_err() { ACTIVE_QUERIES.remove(&task_id); }
        result
    }
    _ = cancellation_fut => {
        ACTIVE_QUERIES.remove(&task_id);
        Err(DataFusionError::Execution(format!("Query {} cancelled", task_id)))
    }
}
```

If cancellation fires during Substrait decode or physical plan build, the `select!` branch returns an error immediately. The `CompletableFuture` completes exceptionally — no stream pointer is ever returned to Java, so `streamClose` is never called.

### Streaming (task 3)

`streamNext` takes `task_id` in its JNI signature (same as `streamClose` already has), clones the token from `ACTIVE_QUERIES`, and races `try_next` against cancellation. For non-cancellable queries the token is `None` and the `select!` is skipped entirely — `try_next` is awaited directly:

```rust
// streamNext
let token = ACTIVE_QUERIES.get(&task_id)
    .and_then(|ctx| ctx.cancellation_token.clone());

// For non-cancellable queries token is None; skip select! and await directly.
let maybe_batch = if let Some(token) = token {
    select! {
        result = stream.try_next() => result?,
        _ = token.cancelled()      => return Ok(0),  // EOF signal to Java
    }
} else {
    stream.try_next().await?
};
```

When the token fires, `token.cancelled()` wins the `select!`. The io_runtime task returns `Ok(0)`, Java's `CompletableFuture<Boolean>` completes with `false`, and the batch loop exits naturally and calls `streamClose`.

### Considered alternative: AbortHandle + await for io tasks

An alternative to `CancellationToken` for io tasks is the same `AbortHandle`-based approach used for the cpu task: store the `JoinHandle` (or `AbortHandle`) returned by `spawn_jni_task`, call `abort()` in `cancelQuery`, and then await the handle to get a definitive `Err(JoinError::Cancelled)` signal before invoking the Java callback.

**Why it does not work without significant restructuring**

`AbortHandle::abort()` drops the task future at its next `await` point — no cleanup code runs. The Java `ActionListener` (`GlobalRef`) is moved into the task closure, so it is dropped silently with the future. The Java `CompletableFuture` is never completed and hangs indefinitely.

To recover the ActionListener after abort you would have to extract it from the task before it runs:

```rust
// Would require lifting the ActionListener out of the task:
let listener_arc = Arc::new(Mutex::new(Some(action_listener)));
let listener_for_task = Arc::clone(&listener_arc);

let handle = io_runtime.spawn(async move {
    let result = stream.try_next().await;
    listener_for_task.lock().unwrap().take().map(|l| call_listener(l, result));
});

// In cancelQuery — cannot block, so must spawn a second task:
handle.abort();
io_runtime.spawn(async move {
    match handle.await {
        Err(e) if e.is_cancelled() => {
            listener_arc.lock().unwrap().take().map(|l| call_listener_with_eof(l));
        }
        _ => {} // task finished normally; listener already called from within the task
    }
});
```

**Problems with this approach**

- **`cancelQuery` cannot block.** Awaiting the `JoinHandle` directly in `cancelQuery` would block the calling Java thread until the io task reaches its next `await` point. This makes cancellation synchronously slow. The workaround — spawning a second io task to do the deferred await-then-callback — adds another layer of async coordination.

- **Requires `Arc<Mutex<Option<ActionListener>>>` for every spawned task.** Every `streamNext` invocation would need to wrap its `ActionListener` in shared state to allow the deferred cleanup task to call it. This is runtime overhead on the hot path (one allocation per `streamNext` call).

- **Exactly-once callback guarantee becomes explicit.** With the shared `Option`, both the task and the cleanup task must coordinate via `.take()` to ensure the callback fires exactly once. This logic is implicit and easy to get wrong (e.g., the task completes normally and calls the listener, then the cleanup task also fires before checking).

- **Dropping `JoinHandle` in `spawn_jni_task` does not abort the task.** Unlike `JoinSet`, dropping a bare `JoinHandle` in Tokio merely detaches the task — it continues running. So storing handles solely to later call `abort()` requires a non-trivial change to how io tasks are managed.

**Comparison**

| | `CancellationToken` + `select!` | `AbortHandle` + await |
|---|---|---|
| Cancellation speed | Delivered at next `.await` point — identical to AbortHandle | Delivered at next `.await` point — identical to CancellationToken |
| Sub-task cleanup (`JoinSet`-owned) | Equivalent — `select!` drops the losing future, which drops the `JoinSet` and calls `abort_all()` | Equivalent — future drop calls `abort_all()` on the `JoinSet` |
| Sub-task cleanup (detached `spawn`) | Neither approach helps | Neither approach helps |
| ActionListener ownership | Inside the task; called from the cancellation arm | Must be lifted out; shared via `Arc<Mutex<Option<...>>>` |
| Certainty of cancellation | `select!` drops the losing branch before running the arm — stream is cancelled when callback fires | Await on `JoinHandle` gives a definitive cancelled signal |
| `cancelQuery` blocking | Non-blocking — token fires an atomic flag | Would block or require a second spawned task |
| Hot-path overhead | One `CancellationToken` clone per query | One `Arc<Mutex<Option<>>>` allocation per `streamNext` call |
| Cleanup code | Runs naturally inside the cancellation arm | Must be wired through the deferred cleanup task |
| Fit for cpu task (task 2) | Not used — no callback obligation | Natural fit — task just stops, no callbacks |

**Sub-task cleanup: both approaches are equivalent for `JoinSet`-owned sub-tasks**

If an io task owns a `JoinSet` of internally spawned sub-tasks, both approaches provide the same structural cleanup guarantee:

- **AbortHandle**: `abort()` causes the parent future to be dropped at its next `.await`. Rust drops every field in that future — including the `JoinSet` — which calls `abort_all()` on all sub-tasks.

- **CancellationToken**: When `token.cancelled()` wins the `select!` race, `select!` drops the *losing* branch's future immediately before running the cancellation arm. If the losing future (e.g. `stream.try_next()`) owns or transitively holds a `JoinSet`, it is dropped at that point and `abort_all()` fires. The cancellation arm does not need explicit cleanup code — the drop happens structurally via `select!`'s losing-branch semantics, just as it does when the parent future is dropped by `abort()`.

Neither approach helps with sub-tasks spawned detached via `tokio::spawn`. Those are invisible to both mechanisms and keep running regardless.

Tasks 1 and 3 as currently implemented do not own a `JoinSet` of sub-tasks, so this is not a concern today. If they were refactored to spawn sub-tasks internally, both approaches would clean them up equivalently as long as the sub-tasks are held in a `JoinSet` rather than spawned detached.

**Cancellation speed is identical**

Neither mechanism cancels faster than the other. Both `AbortHandle::abort()` and `CancellationToken::cancel()` are non-blocking flag-setting operations. In both cases, the running task is not preempted — it continues executing until it reaches its next `.await` point, at which point Tokio checks the abort flag (for `AbortHandle`) or the `select!` polls `token.cancelled()` (for `CancellationToken`). If a task is in the middle of a CPU-bound loop with no `.await`, neither mechanism will interrupt it. The choice between them is purely about what happens *at* that `.await` point — silent drop vs. running cleanup code.

**Why `CancellationToken` is preferred for io tasks**

The `select!` cancellation arm runs from inside the task where the `ActionListener` already lives. When the arm executes, `select!` has already dropped the `stream.try_next()` future, so the stream is cancelled and the callback is called exactly once with no shared state. The cpu task uses `AbortHandle` precisely because it has no callback obligation — dropping its future silently is the correct behaviour. The two mechanisms are chosen to match the semantics of each task, not used interchangeably.

### Triggering cancellation from Java

`cancelQuery(long taskId)` calls `token.cancel()` on the registered token. This is a fast, non-blocking operation — it sets an atomic flag and wakes any futures waiting on `token.cancelled()`.

Java calls `cancelQuery` from three sites:

- **`SearchShardTask.onCancelled()` hook** — the primary, immediate path. `DatafusionEngine.executeQueryPhaseAsync` registers `() -> NativeBridge.cancelQuery(contextId)` on the task via `task.setCancellationListener(...)` before launching the query. Any cancellation signal (HTTP disconnect, `POST /_tasks/<id>/_cancel`, timeout) immediately fires the token, regardless of which execution phase the query is in. The listener is cleared when the async operation completes to avoid retaining a reference to a completed query.
- **`loadNextBatch()` batch loop** — secondary poll-based check. If `context.isCancelled()` returns true between batches, `cancelQuery` is called as a belt-and-suspenders fallback.
- **`DatafusionContext.doClose()`** — covers teardown paths (node shutdown, context eviction) where the task may not have been explicitly cancelled.

In all cases, `streamClose` is left to the batch loop's normal exit path. Cancellation code must not call `streamClose` directly.

---

## Cancellation of the cpu_executor Task (task 2)

The cpu task is cancelled via two complementary paths. The first path is faster; the second is a safety net.

### Path 1: AbortHandle (immediate, on cancelQuery)

`DedicatedExecutor::spawn_with_abort_handle` spawns the task immediately and returns an `AbortHandle` alongside the result future:

```rust
// executor.rs
pub fn spawn_with_abort_handle<T>(&self, task: T)
    -> (BoxFuture<'static, Result<T::Output, JobError>>, Option<AbortHandle>)
{
    let mut join_set = JoinSet::new();
    let abort_handle = join_set.spawn_on(task, &handle);  // task starts running now
    let fut = async move { join_set.join_next().await... }.boxed();
    (fut, Some(abort_handle))
}
```

`CrossRtStream::new_with_df_error_stream` calls `spawn_with_abort_handle` **eagerly at construction time** (not lazily on first poll). The `AbortHandle` is returned alongside `Self`:

```rust
// cross_rt_stream.rs
pub fn new_with_df_error_stream(stream, exec) -> (Self, Option<AbortHandle>) {
    let (spawn_fut, abort_handle) = exec.spawn_with_abort_handle(fut); // task starts here
    let driver = async move { spawn_fut.await... }.boxed();
    (CrossRtStream { driver, inner, .. }, abort_handle)
}
```

`executeQueryPhaseAsync` stores the handle in `QueryContext.cpu_abort_handle` after the stream is created:

```rust
// lib.rs — select! result arm
Ok((stream_ptr, abort_handle)) => {
    if let (Some(handle), Some(ctx)) = (abort_handle, ACTIVE_QUERIES.get(&task_id)) {
        let _ = ctx.cpu_abort_handle.set(handle);
    }
    Ok(stream_ptr)
}
```

`cancelQuery` then fires the token and aborts the cpu task. Both are guarded by `Option` — a `cancelQuery` call for a non-cancellable `task_id` is a no-op:

```rust
if let Some(ctx) = ACTIVE_QUERIES.get(&task_id) {
    if let Some(token) = &ctx.cancellation_token {
        token.cancel();
    }
    if let Some(handle) = ctx.cpu_abort_handle.get().and_then(|h| h.as_ref()) {
        handle.abort();  // cpu task scheduled for cancellation at next await point
    }
}
```

This schedules task 2 for cancellation at its next `await` point in DataFusion's Parquet reader, without waiting for Java to call `streamClose`. `abort()` is non-blocking — it returns before task 2 has actually stopped.

If `cancelQuery` fires before the stream is created (i.e., `cpu_abort_handle` is still `None`), the `CancellationToken` in the `select!` fires and the cpu task was never spawned — no abort is needed.

### Path 2: JoinSet drop (safety net, on streamClose)

[`CrossRtStream`](../jni/src/cross_rt_stream.rs) stores `spawn_fut` (the `BoxFuture` that owns the `JoinSet`) as its `driver` field:

```rust
pub struct CrossRtStream {
    driver: BoxFuture<'static, ()>,  // owns the JoinSet via spawn_fut
    inner: ReceiverStream<...>,      // MPSC channel receiver
    ...
}
```

When `streamClose` drops the `CrossRtStream`, Rust drops fields in declaration order: `driver` before `inner`. Dropping `driver` drops the `JoinSet`, which calls `abort_all()`. Dropping `inner` closes the MPSC channel, causing any in-flight `tx.send()` in the cpu task to fail — a secondary stop.

This path fires regardless of whether `cancelQuery` was called, ensuring the cpu task is cleaned up on normal end-of-stream closes too.

### The full cascade from `streamClose`

```
streamClose → drop RecordBatchStreamAdapter<CrossRtStream>
  → drop CrossRtStream.driver (BoxFuture owning JoinSet)
      → JoinSet::drop() → abort_all()
          → task 2 receives Tokio abort signal → stops at next await point
              → SendableRecordBatchStream dropped
                  → RecordBatchReceiverStreamBuilder's JoinSet dropped → abort_all()
                      → tasks 2a–2n receive Tokio abort signal → stop at next await point
  → drop CrossRtStream.inner (MPSC Receiver)
      → channel closed → tx_captured.send() fails → task 2 loop exits (secondary)
```

DataFusion's built-in source operators (`ParquetExec`, etc.) participate in Tokio's cooperative scheduling via `tokio::task::consume_budget()`. This ensures that once `abort_all()` fires, the cpu task stops within a bounded number of polling cycles — not instantaneously, but promptly. See the [DataFusion cancellation blog post](https://datafusion.apache.org/blog/2025/06/30/cancellation/) for a detailed explanation of how DataFusion uses Tokio's task budget system.

---

## Cancellation Observability

### Background: OpenSearch's TaskCancellationMonitoringService

OpenSearch tracks cancelled-but-still-running tasks via `TaskCancellationMonitoringService`. Its mechanism:

1. **`onTaskCancelled(task)`** — adds `task.getId() → false` to a `cancelledTaskTracker` map ("not yet counted")
2. **Periodic scheduler** scans live `TaskManager` tasks, filters by `isCancelled() && time_since_cancel >= threshold`. For each such task seen **for the first time** (value=`false`), it flips to `true` and increments `totalLongRunningCancelledTaskCount`. The `false → true` flip prevents double-counting across scheduler cycles.
3. **`onTaskCompleted(task)`** — removes from the tracker.

The cumulative `total_count_post_cancel` is built entirely in Java by this polling loop — it is not an atomic counter incremented at cancel time. The Java service already tracks the `SearchShardTask` lifecycle, which spans the full Rust cancellation window (from `cancelQuery` until `streamClose` completes and the task is deregistered). What it cannot see is **which layer within Rust** is still running.

### What we can observe in Rust

| Layer | Observable? | Mechanism |
|---|---|---|
| io task (task 1, task 3) | Yes | `ACTIVE_QUERIES` entry exists with `cancellation_token == Some(t)` where `t.is_cancelled()` |
| cpu task 2 | Partially — can detect when task 2 **has stopped** | Add `JobError::Cancelled` variant; driver future sees it after `abort()` takes effect |
| DataFusion partition tasks (2a–2n) | **No** | Spawned detached via `tokio::spawn` inside DataFusion; no handle, no completion signal |

For the io layer, `ACTIVE_QUERIES` already contains everything needed: entries where `is_cancelled()` is true are queries in the post-cancel window. For the cpu layer, we need one additional change: a `JobError::Cancelled` variant in `executor.rs` so the driver future can distinguish task 2 being aborted from the executor shutting down:

```rust
// executor.rs
pub enum JobError {
    WorkerGone,
    Cancelled,   // task was aborted via AbortHandle::abort()
    Panic { msg: String },
}

// In spawn_with_abort_handle's error mapping:
Err(e) => if e.is_cancelled() { JobError::Cancelled } else { JobError::WorkerGone },
```

### Proposed: ID-based post-cancel tracking

Rather than simple counters, tracking the actual `task_id` sets enables set operations that are more informative and allow Java to correlate with `TaskManager` metadata:

```rust
// lib.rs
/// task IDs where cancelQuery fired but streamClose has not yet been called.
/// Covers the io-layer post-cancel window (tasks 1 and 3).
static IO_POST_CANCEL_IDS: Lazy<DashSet<i64>> = Lazy::new(DashSet::new);

/// task IDs where abort() was called on task 2 but task 2 has not yet stopped.
/// Lives outside ACTIVE_QUERIES so it can outlast streamClose.
static CPU_POST_CANCEL_IDS: Lazy<DashSet<i64>> = Lazy::new(DashSet::new);
```

Lifecycle of each set:

| Event | IO set | CPU set |
|---|---|---|
| `cancelQuery` called (token fired) | `insert(task_id)` | — |
| `cancelQuery` called (abort handle present) | — | `insert(task_id)` |
| `streamClose` removes ACTIVE_QUERIES entry | `remove(task_id)` | — (task 2 may still be running) |
| Driver sees `JobError::Cancelled` (task 2 yielded) | — | `remove(task_id)` |

The union `IO_POST_CANCEL_IDS ∪ CPU_POST_CANCEL_IDS` gives the set of unique task IDs with any Rust-side lag after cancellation. The difference `CPU_POST_CANCEL_IDS \ IO_POST_CANCEL_IDS` isolates the most interesting case: cpu threads still running after the io layer has already returned EOF to Java.

These sets would be exposed to Java via a new JNI method (returning `long[]`) so that a Java-side monitoring component can apply the same `cancelledTaskTracker` dedup pattern as `TaskCancellationMonitoringService` to build cumulative totals.

### Accuracy limitation of CPU_POST_CANCEL_IDS

`CPU_POST_CANCEL_IDS.remove(task_id)` fires when the `JobError::Cancelled` signal reaches the driver — i.e., when task 2's `JoinSet` sees the cancellation. At that point, task 2 has stopped. However, DataFusion's internal partition tasks (2a–2n) may still be running for a brief additional period: they stop only after their coordination channel closes (which happens when task 2's `SendableRecordBatchStream` is dropped) and each reaches its next `consume_budget()` call.

In practice this overshoot is bounded to at most one Parquet row-group read, but it means `CPU_POST_CANCEL_IDS` being empty does **not** guarantee that all CPU work for a query has ceased. There is no mechanism to observe tasks 2a–2n directly without changes to DataFusion itself.

---

## Known Limitations

**Fetch phase mid-execution.** `executeFetchPhase` uses `block_on` and cannot be interrupted once started. A pre-cancellation check at the start of the JNI method handles the case where cancellation was signalled before `executeFetchPhase` begins. Cancellation mid-fetch requires converting it to async (tracked separately).

**Parquet row-group granularity.** Tokio task abort delivers at `consume_budget()` boundaries, which in DataFusion's Parquet reader fall at row-group boundaries. A single very large row group may not be interrupted mid-read. Cancellation is prompt, not instantaneous.

**DataFusion internal task visibility.** Tasks 2a–2n are spawned by DataFusion into a `JoinSet` embedded in the `SendableRecordBatchStream`. They are cancelled structurally when task 2's stream is dropped (via `abort_all()` on that `JoinSet`), so they do not leak. However, we hold no direct handle to them and cannot observe when they have fully stopped. `CPU_POST_CANCEL_IDS` tracks task 2's lifecycle only; there is a bounded window after `CPU_POST_CANCEL_IDS.remove` where partition tasks may still be consuming cpu_executor threads before they each reach their own next `.await` point.
