# DataFusion Query Cancellation — Implementation Plan

## Background

When a DataFusion query is cancelled (timeout, explicit cancel API, node failure), the Rust
side currently has no way to stop in-flight work. A single shard-level query fans out into:

- One **io_runtime** task: runs `execute_query_with_cross_rt_stream` (schema inference, Substrait
  decode, physical plan build, then returns stream pointer to Java).
- One **cpu_executor** task: the `CrossRtStream` driver that polls the DataFusion
  `SendableRecordBatchStream` and sends batches through the MPSC channel.
- N **DataFusion internal tasks**: spawned by `execute_stream` during physical plan execution
  (one per `target_partitions`).
- One **io_runtime** task per `streamNext` call: polls `CrossRtStream.try_next()`.

### How DataFusion cancellation works (v49+)

Dropping the `SendableRecordBatchStream` (or any stream wrapping it) cancels background work
because:

1. `DedicatedExecutor::spawn` uses a `JoinSet`. Dropping the `JoinSet` future calls
   `abort_all()`, which issues a Tokio task abort on the cpu_executor task.
2. DataFusion built-in source operators (Parquet, filter, aggregate, etc.) participate in
   Tokio's **per-task operation budget** system. Once a task is aborted, these operators
   yield at the next budget boundary, after which Tokio cancels the task.
3. `TaskContext` has **no cancel API** — there is no `with_cancel_token()`. Drop is the
   only mechanism.

When `streamClose` is called, this cascade already fires:
```
streamClose → drop RecordBatchStreamAdapter<CrossRtStream>
  → drop CrossRtStream.driver (BoxFuture)
    → drop JoinSet future returned by exec.spawn()
      → JoinSet::drop() → abort_all() → cpu_executor task aborted
        → DataFusion operators yield at next budget point → cancelled
```

### The gap: query-phase setup window

Between the JNI call to `executeQueryPhaseAsync` and the CompletableFuture completing, the
io_runtime task is live with no handle on the Java side. Java cannot call `streamClose`
because it has no stream pointer yet. This window covers:
- Schema inference (async Parquet footer reads)
- Substrait plan decode
- `execute_logical_plan` → `create_physical_plan`

This gap is covered by Option B: a `CancellationToken` that races against the io_runtime task.

---

## Design: Option B

### Task ID

Use `readerContext.id().getId()` (`long`) as the stable per-shard-search task ID. This is
the `ShardSearchContextId` inner long, unique within a node across the entire query lifecycle.
Note: `DatafusionContext.id()` currently returns `null` — it must be fixed to return
`readerContext.id()`.

### Rust: global `ACTIVE_QUERIES` registry

```rust
static ACTIVE_QUERIES: Lazy<DashMap<i64, CancellationToken>> = Lazy::new(DashMap::new);
```

`DashMap` is already a declared dependency. `CancellationToken` is from `tokio_util`.

### Cancellation flow

```
Java: context.isCancelled() == true
  → NativeBridge.cancelQuery(taskId)
    → ACTIVE_QUERIES.get(taskId).map(|t| t.cancel())
      → select! in io_runtime task fires
        → ACTIVE_QUERIES.remove(taskId)
        → listener.onFailure(DataFusionException("Query <id> cancelled"))

  → stream.close()  (if stream pointer already obtained)
    → streamClose JNI → drop CrossRtStream
      → JoinSet::abort_all → cpu_executor task aborted
        → DF operators yield at budget point → cancelled
```

### Fetch phase

`executeFetchPhase` uses `block_on` intentionally. Cancellation mid-execution is not possible.
At the start of the JNI method, check whether the token for `task_id` has already been
cancelled and return an error early. This prevents starting a new Parquet scan for a query
that is already cancelled.

---

## Implementation Plan

### Step 1 — Rust: `Cargo.toml`

Add `tokio_util` as an explicit workspace and crate dependency:

```toml
# workspace Cargo.toml
tokio-util = { version = "0.7", features = ["sync"] }

# jni/Cargo.toml
tokio-util = { workspace = true }
```

---

### Step 2 — Rust: `lib.rs` — global registry

```rust
use tokio_util::sync::CancellationToken;

static ACTIVE_QUERIES: Lazy<DashMap<i64, CancellationToken>> = Lazy::new(DashMap::new);
```

---

### Step 3 — Rust: `lib.rs` — modify `executeQueryPhaseAsync`

Add `task_id: jlong` as a new parameter (after `runtime_ptr`, before `listener`).

```rust
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_executeQueryPhaseAsync(
    ...,
    task_id: jlong,       // NEW
    listener: JObject,
)
```

Inside the function, before calling `spawn_jni_task`:

```rust
let token = CancellationToken::new();
ACTIVE_QUERIES.insert(task_id, token.clone());

spawn_jni_task(
    &io_runtime,
    "executeQueryPhaseAsync",
    listener_ref,
    async move {
        tokio::select! {
            result = execute_query_with_cross_rt_stream(
                table_path, files_meta, table_name, plan_bytes_vec,
                is_query_plan_explain_enabled, target_partitions,
                runtime, cpu_executor,
            ) => {
                ACTIVE_QUERIES.remove(&task_id);
                result
            }
            _ = token.cancelled() => {
                ACTIVE_QUERIES.remove(&task_id);
                Err(DataFusionError::Execution(
                    format!("Query {} cancelled", task_id)
                ))
            }
        }
    },
    |env, lr, ptr| set_action_listener_ok_global(env, lr, ptr),
);
```

---

### Step 4 — Rust: `lib.rs` — deregister on `streamClose`

`streamClose` must remove the token from the registry so it is not leaked if Java closes
the stream without explicitly cancelling (normal end-of-stream path):

```rust
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_streamClose(
    _env: JNIEnv,
    _class: JClass,
    stream: jlong,
    task_id: jlong,   // NEW
) {
    ACTIVE_QUERIES.remove(&task_id);
    let _ = unsafe { Box::from_raw(stream as *mut RecordBatchStreamAdapter<CrossRtStream>) };
}
```

> **Alternative**: If adding `task_id` to `streamClose` is undesirable (it couples stream
> lifecycle to task lifecycle), deregistration can happen solely in Step 3's `select!` arms.
> Any leftover entry in `ACTIVE_QUERIES` is harmless — a `cancelQuery` call on a completed
> task ID simply finds no entry (DashMap returns `None`) and is a no-op.

---

### Step 5 — Rust: `lib.rs` — `executeFetchPhase` early-exit check

At the top of `executeFetchPhase`, before `block_on`:

```rust
// Skip execution if query was already cancelled
if ACTIVE_QUERIES.get(&task_id)
    .map_or(false, |t| t.is_cancelled())
{
    // Throw a Java exception so the caller knows the fetch was skipped
    let _ = env.throw_new(
        "org/opensearch/datafusion/DataFusionException",
        format!("Fetch phase skipped: query {} cancelled", task_id),
    );
    return 0;
}
```

`executeFetchPhase` also needs `task_id: jlong` added to its JNI signature.

---

### Step 6 — Rust: `lib.rs` — new `cancelQuery` JNI method

```rust
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_cancelQuery(
    _env: JNIEnv,
    _class: JClass,
    task_id: jlong,
) {
    if let Some(token) = ACTIVE_QUERIES.get(&task_id) {
        token.cancel();
        log_info!("Cancelled query with task_id={}", task_id);
    } else {
        log_info!("cancelQuery called for unknown/completed task_id={}", task_id);
    }
}
```

---

### Step 7 — Java: `NativeBridge.java`

```java
// Add new method
public static native void cancelQuery(long taskId);

// Update signatures
public static native void executeQueryPhaseAsync(
    long readerPtr, String tableName, byte[] plan,
    boolean isQueryPlanExplainEnabled, int partitionCount,
    long runtimePtr, long taskId,          // taskId is NEW
    ActionListener<Long> listener);

public static native long executeFetchPhase(
    long readerPtr, long[] rowIds, String[] includeFields,
    String[] excludeFields, long runtimePtr, long taskId); // taskId is NEW
```

---

### Step 8 — Java: `DatafusionContext.java`

Fix `id()` (currently returns `null`) and `isCancelled()` (currently returns `false`):

```java
@Override
public ShardSearchContextId id() {
    return readerContext.id();
}

@Override
public boolean isCancelled() {
    return task != null && task.isCancelled();
}
```

Add a helper to expose the stable task ID for JNI:

```java
public long getDatafusionTaskId() {
    return readerContext.id().getId();
}
```

---

### Step 9 — Java: `DatafusionSearcher.java`

Pass `taskId` to both JNI calls:

```java
@Override
public CompletableFuture<Long> searchAsync(DatafusionQuery query, Long runtimePtr, long taskId) {
    CompletableFuture<Long> result = new CompletableFuture<>();
    NativeBridge.executeQueryPhaseAsync(
        reader.getReaderPtr(), query.getIndexName(), query.getSubstraitBytes(),
        query.getQueryPlanExplainEnabled(), query.getTargetPartitionsCount(),
        runtimePtr, taskId,                   // taskId is NEW
        new ActionListener<Long>() { ... });
    return result;
}

@Override
public long search(DatafusionQuery query, Long runtimePtr, long taskId) {
    // fetch phase
    return NativeBridge.executeFetchPhase(
        reader.getReaderPtr(), rowIds, includeFields, excludeFields,
        runtimePtr, taskId);                  // taskId is NEW
}
```

---

### Step 10 — Java: cancellation trigger

The site that calls `searchAsync` / iterates batches must call `cancelQuery` on detection.
`DatafusionContext.isCancelled()` is already checked by OpenSearch's `SearchContext` machinery
(timeout handling, Cancel Task API). The cancellation trigger belongs in the component that
drives the batch iteration loop and has access to `DatafusionContext`:

```java
// In the batch iteration loop (pseudo-code):
while (iterator.hasNext()) {
    if (context.isCancelled()) {
        NativeBridge.cancelQuery(context.getDatafusionTaskId());
        stream.close();  // also drop CrossRtStream to abort cpu_executor task
        throw new TaskCancelledException("DataFusion query cancelled: " + context.getDatafusionTaskId());
    }
    processBatch(iterator.next());
}
```

Additionally, `DatafusionContext.doClose()` should call `cancelQuery` to cover the case where
the search context is closed mid-query (e.g. node shutdown, request timeout):

```java
@Override
protected void doClose() {
    NativeBridge.cancelQuery(getDatafusionTaskId());  // fire token if still registered
    Releasables.close(engineSearcher);
    originalContext.close();
}
```

---

## File Change Summary

| File | Change |
|------|--------|
| `jni/Cargo.toml` | Add `tokio-util` |
| `jni/src/lib.rs` | `ACTIVE_QUERIES` static, `cancelQuery` JNI, `task_id` param to `executeQueryPhaseAsync` + `executeFetchPhase` + `streamClose`, `select!` wrapper, early-exit in fetch phase |
| `NativeBridge.java` | `cancelQuery` native decl; updated signatures for `executeQueryPhaseAsync`, `executeFetchPhase`, `streamClose` |
| `DatafusionContext.java` | Fix `id()`, fix `isCancelled()`, add `getDatafusionTaskId()`, call `cancelQuery` in `doClose()` |
| `DatafusionSearcher.java` | Pass `taskId` to both JNI call sites |
| Caller of batch loop | Add `isCancelled()` check + `cancelQuery` + `stream.close()` |

---

## Testing Plan

### 1. Rust Unit Tests (in `jni/src/lib.rs` or a `tests/` module)

#### `test_active_queries_registration`
Call the core registration logic directly (extract into a helper if needed):
- Insert a token for task_id=42.
- Assert `ACTIVE_QUERIES.contains_key(&42)`.
- Simulate query completion (remove).
- Assert `ACTIVE_QUERIES.get(&42).is_none()`.

#### `test_cancel_query_fires_token`
- Insert a token for task_id=99.
- Call `ACTIVE_QUERIES.get(&99).unwrap().cancel()`.
- Assert `ACTIVE_QUERIES.get(&99).unwrap().is_cancelled()`.

#### `test_select_wrapper_cancels_slow_future`
Simulates the io_runtime task being interrupted mid-setup:
```rust
let token = CancellationToken::new();
let child = token.clone();
let handle = tokio::spawn(async move {
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(60)) => Ok(0i64),
        _ = child.cancelled() => Err(DataFusionError::Execution("cancelled".into())),
    }
});
token.cancel();
let result = handle.await.unwrap();
assert!(matches!(result, Err(DataFusionError::Execution(msg)) if msg.contains("cancelled")));
```

#### `test_fetch_phase_skipped_when_already_cancelled`
- Insert a **pre-cancelled** token: `let t = CancellationToken::new(); t.cancel(); ACTIVE_QUERIES.insert(42, t);`
- Call the early-exit check logic.
- Assert it returns an error/skips execution without touching any Parquet files.

#### `test_cancel_noop_for_unknown_task`
- Call `cancelQuery` for task_id=-1 (not registered).
- Assert no panic, returns normally.

---

### 2. Java Unit Tests

#### `DatafusionContextCancellationTest`
```java
// Mock SearchShardTask
SearchShardTask mockTask = mock(SearchShardTask.class);
when(mockTask.isCancelled()).thenReturn(true);

DatafusionContext ctx = ...; // inject mockTask
assertTrue(ctx.isCancelled());
```

#### `DatafusionContextNotCancelledTest`
Same setup with `isCancelled()` → false.

#### `DatafusionContextIdTest`
Assert `ctx.id()` is not null and `ctx.getDatafusionTaskId()` returns a non-zero long.

---

### 3. Existing Integration Test Extension: `DataFusionReaderManagerTests`

These tests already run the JNI layer end-to-end. Add:

#### `testQueryPhaseCancelledBeforeStreamReturned`
- Start `executeQueryPhaseAsync` with a real substrait plan.
- Immediately call `NativeBridge.cancelQuery(taskId)` (before awaiting the CompletableFuture).
- Assert the CompletableFuture completes exceptionally with a message containing "cancelled".
- Assert `ACTIVE_QUERIES` no longer contains `taskId` (call a new diagnostic JNI method, or
  infer from the error).

> **Timing note**: schema inference may complete before cancel fires if the test machine is
> fast. Use a substrait plan against many large Parquet files, or introduce a configurable
> delay in a test-only code path, to reliably hit the setup window.

#### `testQueryPhaseCancelledDuringStreaming`
- Run `executeQueryPhaseAsync` to completion (get stream pointer).
- Call `NativeBridge.streamNext` once to confirm it works.
- Call `NativeBridge.cancelQuery(taskId)`.
- Call `NativeBridge.streamClose(streamPtr, taskId)`.
- Assert the next `streamNext` (if any in-flight) completes exceptionally or returns 0.
- Assert no resource leaks (thread leak filter still passes).

#### `testFetchPhaseSkippedWhenCancelled`
- Do NOT call `executeQueryPhaseAsync` (no token registered).
- Insert a pre-cancelled token into `ACTIVE_QUERIES` for the test task_id (via a test-only
  JNI helper, or by calling `cancelQuery` immediately after `executeQueryPhaseAsync` and
  before `executeFetchPhase`).
- Call `executeFetchPhase` with the same task_id.
- Assert it throws a `DataFusionException` containing "cancelled".

---

### 4. Manual End-to-End Tests

These require a running single-node OpenSearch with the DataFusion engine enabled.

#### Test A: Cancel via Task API during streaming

1. Index a moderately large dataset (~1M docs) using the DataFusion engine.
2. Submit a search request with a non-trivial filter (to ensure meaningful Parquet I/O):
   ```
   POST /my_index/_search
   {"query": {"range": {"timestamp": {"gte": "2020-01-01"}}}, "size": 10000}
   ```
3. While the request is in flight, get its task ID:
   ```
   GET /_tasks?actions=*search*&detailed=true
   ```
4. Cancel it:
   ```
   POST /_tasks/<task_id>/_cancel
   ```
5. **Expected**: the search request returns a `TaskCancelledException` quickly (within 1-2
   seconds of the cancel call), not after the full query completes.
6. **Check logs**: should see `INFO  "Cancelled query with task_id=<id>"` from Rust.

#### Test B: Query times out

1. Set a short search timeout on the index:
   ```
   PUT /my_index/_settings
   {"index.search.idle.after": "1s"}
   ```
   Or use `?timeout=500ms` on the search request.
2. Submit a slow query.
3. **Expected**: the query is cancelled by the timeout mechanism; same log line as Test A.

#### Test C: Cancel during query-phase setup (hardest to hit manually)

This window is narrow (< 500ms typically). To force it:

1. Use a test-only cluster setting or JVM flag that introduces a sleep in
   `DatafusionSearcher.searchAsync` after calling the JNI method but before it completes
   (not production code, test hook only).
2. Or: create an index with hundreds of Parquet files to slow schema inference.
3. Submit a query and immediately cancel.
4. **Expected**: `executeQueryPhaseAsync`'s CompletableFuture completes exceptionally with
   "cancelled"; no stream pointer is ever returned to Java.

#### Test D: Normal query still works after cancel infrastructure is added

1. Submit a search and let it complete normally.
2. Assert correct results are returned.
3. Assert `ACTIVE_QUERIES` is empty after completion (via a diagnostic JNI method or log).

---

## Known Limitations

- **Fetch phase mid-execution**: `executeFetchPhase` uses `block_on`. If the cancel fires
  after `executeFetchPhase` has started the `block_on` call, the block runs to completion.
  The early-exit check at the start of the JNI method only helps if cancellation was already
  signalled before the fetch begins. Fixing this requires converting `executeFetchPhase` to
  async (tracked separately).

- **Parquet I/O granularity**: Even with task abort, DataFusion operators yield at Tokio
  budget boundaries — typically at row-group boundaries in the Parquet reader. A single
  large row group may not be interrupted mid-read. Cancellation is prompt, not instantaneous.

- **Custom operators**: Any custom `ExecutionPlan` nodes that do not call
  `tokio::task::coop::poll_proceed` will not respect Tokio's budget mechanism. The
  `EnsureCooperative` optimizer rule can be added to `SessionStateBuilder` to wrap such
  operators automatically.
