# Circuit Breaker Integration — Revised Proposal v2

## Problems

1. **`NativeProxyCircuitBreaker` is not in the Java breakers map.** The `CircuitBreakerPlugin` API only provides `BreakerSettings` — the service creates a `ChildMemoryCircuitBreaker` (with `used=0` since nothing calls `addEstimateBytesAndMaybeBreak` on it). Our custom impl is never used.

2. **Java-side allocations don't check combined JVM + Rust memory.** `checkParentLimit` in real-memory mode only checks JVM heap.

3. **No way to inject a custom `CircuitBreaker` impl** through the existing plugin API.

## Requirements

When **either** Java or Rust allocates memory, check:
1. **Request level:** Is this request consuming too much? (child breaker)
2. **Runtime level:** Is this runtime (Java or Rust) approaching its limit?
3. **Node level:** Is total node memory (JVM + Rust) approaching the system limit?

## Key Insight from Review

In **real-memory mode** (default), the parent limit is `95% of JVM max heap`. Adding native memory to `currentMemoryUsage()` without changing the limit would cause premature tripping. The correct approach is:

- **Keep the existing Java parent check unchanged** (95% of JVM heap — protects JVM from OOM)
- **Add a separate node-level check** that compares `JVM heap + Rust total` against a new `node_limit` (total system memory budget)
- These are **two different checks with two different limits**, not one modified check

This is exactly what `checkParentFromRust` already does — it runs the combined check first, then calls the existing parent check. The issue is that this only happens on the Rust path, not the Java path.

---

## Proposed Solution

### Server-Side Change: Extend `CircuitBreakerPlugin`

Add one default method:

```java
// In CircuitBreakerPlugin.java:
/**
 * Optionally provide a custom CircuitBreaker implementation.
 * If present, this is used instead of creating a ChildMemoryCircuitBreaker from BreakerSettings.
 * The returned breaker is placed in the breakers map and participates in parent aggregation.
 */
default CircuitBreaker createCustomCircuitBreaker(BreakerSettings settings) {
    return null; // default: let the service create it as before
}
```

In `HierarchyCircuitBreakerService` constructor, when processing custom breakers:

```java
for (int i = 0; i < customBreakers.size(); i++) {
    BreakerSettings settings = customBreakers.get(i);
    CircuitBreakerPlugin plugin = customBreakerPlugins.get(i); // need to pass plugins alongside settings
    CircuitBreaker breaker = plugin.createCustomCircuitBreaker(settings);
    if (breaker == null) {
        breaker = validateAndCreateBreaker(settings);
    }
    childCircuitBreakers.put(settings.getName(), breaker);
}
```

This requires `Node.java` to pass the plugin references alongside the settings (currently it only passes `List<BreakerSettings>`). Change to pass `List<Pair<BreakerSettings, CircuitBreakerPlugin>>` or similar.

### What This Enables

Our `NativeProxyCircuitBreaker` is now in the breakers map. This means:
- `_nodes/stats` shows real Rust memory via `getUsed()`
- `memoryUsed()` includes Rust memory in the transient/permanent sums (for estimated mode and durability classification)
- Any code calling `circuitBreakerService.getBreaker("native_request")` gets our impl

### Node-Level Check: Two Separate Checks

**Do NOT modify `currentMemoryUsage()`.** Instead, keep two separate checks:

1. **Existing Java parent check** (`checkParentLimit`): `JVM heap + newBytes > 95% JVM max heap`
   - Protects JVM from OOM
   - Unchanged, backward compatible

2. **Combined node check** (new): `JVM heap + Rust total + newBytes > node_limit`
   - Protects the node from total memory exhaustion
   - `node_limit` = system memory − safety margin (new setting)
   - Runs on BOTH Java and Rust allocation paths

### Where the Combined Node Check Runs

**On Rust allocations:** Already implemented in `checkParentFromRust` (the upcall). It does the combined check, then calls the existing parent check.

**On Java allocations:** This is the gap. We need to add the combined check to the Java path. Two options:

**Option A: Override `checkParentLimit` behavior via the custom breaker**

Make `NativeProxyCircuitBreaker.addEstimateBytesAndMaybeBreak()` NOT a no-op. Instead, have it do the combined node check:

```java
@Override
public double addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
    // Combined node check: JVM heap + Rust total + bytes > node_limit
    long jvmHeap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    long rustTotal = cachedRustMemory; // from background thread volatile
    if (jvmHeap + rustTotal + bytes > nodeLimit) {
        circuitBreak(label, bytes);
    }
    // Don't track bytes in used — Rust tracks its own memory
    return 0;
}
```

But wait — `addEstimateBytesAndMaybeBreak` is only called on the `native_request` breaker if someone explicitly calls it. The Java search path calls it on the `request` breaker, not ours. So this doesn't help for Java allocations.

**Option B: Augment the existing parent check via `checkParentLimit` wrapper (no server change needed)**

The existing `checkParentLimit` is called after every Java child breaker check. We can't modify it without a server change. But we CAN make the existing parent check trip on combined pressure by ensuring `NativeProxyCircuitBreaker.getUsed()` returns a value that, when added to JVM heap in estimated mode, exceeds the parent limit.

But in **real-memory mode**, `getUsed()` is only used for durability classification, not the limit check. So this doesn't work for real-memory mode.

**Option C: Server-side change to `checkParentLimit` (recommended)**

Add a hook in `checkParentLimit` that allows registered breakers to contribute to the total:

```java
// In HierarchyCircuitBreakerService.checkParentLimit():
public void checkParentLimit(long newBytesReserved, String label) throws CircuitBreakingException {
    final MemoryUsage memoryUsed = memoryUsed(newBytesReserved);
    long parentLimit = this.parentSettings.getLimit();
    
    // NEW: Also check combined node limit if native memory is present
    CircuitBreaker nativeBreaker = this.breakers.get("native_request");
    if (nativeBreaker != null) {
        long nativeUsed = nativeBreaker.getUsed();
        long combinedUsage = memoryUsed.totalUsage + nativeUsed;
        long nodeLimit = this.nodeLimit; // new setting: total system memory budget
        if (nodeLimit > 0 && combinedUsage > nodeLimit) {
            throw new CircuitBreakingException(...);
        }
    }
    
    // Existing check unchanged
    if (memoryUsed.totalUsage > parentLimit) { ... }
}
```

This is more generic if we make it iterate all custom breakers rather than hardcoding "native_request":

```java
// Sum native memory from all custom breakers that report non-zero getUsed()
long nativeTotal = 0;
for (CircuitBreaker breaker : this.breakers.values()) {
    if (breaker instanceof NativeMemoryBreaker) { // marker interface
        nativeTotal += breaker.getUsed();
    }
}
if (nodeLimit > 0 && memoryUsed.totalUsage + nativeTotal > nodeLimit) { ... }
```

Or even simpler — use the existing `memoryUsed.totalUsage` which already sums all children (including our native breaker) in estimated mode. In real-memory mode, add native breaker usage on top:

```java
long effectiveUsage = memoryUsed.totalUsage;
if (trackRealMemoryUsage) {
    // In real-memory mode, totalUsage = JVM heap. Add native memory.
    CircuitBreaker nativeBreaker = this.breakers.get("native_request");
    if (nativeBreaker != null) {
        effectiveUsage += nativeBreaker.getUsed();
    }
}
// Check against a node-level limit (new setting, default = parentLimit)
if (nodeLimit > 0 && effectiveUsage > nodeLimit) { ... }
```

### Background Thread: `NativeMemoryCollectorService`

A service that refreshes jemalloc stats and updates a Java-side volatile:

```java
public class NativeMemoryCollectorService extends AbstractLifecycleComponent {
    private volatile long cachedRustMemory = 0;
    private Scheduler.Cancellable scheduledFuture;

    @Override
    protected void doStart() {
        // Initial refresh
        refreshNativeMemory();
        // Schedule periodic refresh
        scheduledFuture = threadPool.scheduleWithFixedDelay(
            this::refreshNativeMemory, TimeValue.timeValueSeconds(1), ThreadPool.Names.GENERIC
        );
    }

    private void refreshNativeMemory() {
        NativeBridge.refreshJemallocCache(); // FFM call: epoch.advance() + store in Rust atomic
        long[] stats = NativeBridge.getBreakerStats();
        cachedRustMemory = stats[3]; // total_used_bytes
    }

    public long getCachedRustMemory() { return cachedRustMemory; }
}
```

`NativeProxyCircuitBreaker.getUsed()` reads from this service's volatile — **zero FFM cost on the hot path**.

### Eliminating the Circular FFM Call

With the background thread approach:
- `NativeProxyCircuitBreaker.getUsed()` → reads `cachedRustMemory` volatile (no FFM)
- Rust upcall → `checkParentFromRust` → `svc.checkParentLimit()` → `memoryUsed()` → `NativeProxyCircuitBreaker.getUsed()` → reads volatile (no FFM)

**No circular FFM calls.** The only FFM calls are:
- Rust → Java upcall (once per allocation, or time-gated)
- Background thread → Rust (once per second, off the hot path)

### New FFM Function

```rust
#[no_mangle]
pub extern "C" fn df_refresh_jemalloc_cache() {
    // Called by background thread only
    let m = native_bridge_common::allocator::mib();
    m.epoch.advance().ok();
    let alloc = m.allocated.read().unwrap_or(0);
    BREAKER.get().map(|b| b.cached_total_bytes.store(alloc, Ordering::Release));
}
```

---

## Summary of Server-Side Changes

| Change | Scope | Purpose |
|--------|-------|---------|
| `CircuitBreakerPlugin.createCustomCircuitBreaker()` | Interface default method | Allow plugins to provide custom `CircuitBreaker` impl |
| `HierarchyCircuitBreakerService` constructor | Use plugin-provided breaker if available | Get `NativeProxyCircuitBreaker` into the breakers map |
| `checkParentLimit()` | Add node-level combined check | Detect JVM + native pressure on Java allocation path |
| New setting: `indices.breaker.total.node_limit` | Cluster setting | Total system memory budget (JVM + native) |
| `Node.java` | Pass plugin refs alongside BreakerSettings | Enable the constructor to call `createCustomCircuitBreaker` |

All changes are backward-compatible:
- `createCustomCircuitBreaker()` defaults to `null` (existing behavior)
- Node-level check only fires if `node_limit > 0` (default: 0 = disabled, or auto-derived)
- No behavior change for users without native plugins

---

## Complete Flow After Fix

**Rust allocation (`try_grow`):**
1. Child CAS check (request_used + N > child_limit)
2. Node check (cached_jemalloc + N > node_limit) — reads Rust-side cached atomic
3. Java parent upcall → `checkParentFromRust`:
   - Combined check: `JVM heap + cachedRustMemory + N > node_limit`
   - Existing parent: `JVM heap + N > 95% JVM heap`

**Java allocation (`addEstimateBytesAndMaybeBreak` on `request` breaker):**
1. Child CAS check (request_used + N > request_limit) — existing
2. `checkParentLimit()`:
   - Existing: `JVM heap + N > 95% JVM heap` (real-memory mode)
   - **NEW**: `JVM heap + cachedRustMemory + N > node_limit`

**Both paths check all three levels:** ✅

---

## Open Questions

1. **`node_limit` default value:** Auto-derive from `Runtime.maxMemory() + configured native budget`? Or require explicit configuration?
2. **Should the Rust upcall be time-gated?** Benchmarks show FFM upcall adds ~0-6% overhead. Could keep it real-time for maximum correctness.
3. **Marker interface vs hardcoded name:** Should `checkParentLimit` look for a marker interface (`NativeMemoryBreaker`) or hardcode `"native_request"`? Marker interface is more generic but adds a new interface to `core/`.
