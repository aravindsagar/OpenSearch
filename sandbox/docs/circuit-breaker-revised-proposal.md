# Circuit Breaker Integration — Revised Proposal

## Problems Identified

1. **`NativeProxyCircuitBreaker` is never used by the Java parent breaker.** `CircuitBreakerPlugin` only provides `BreakerSettings` — the service always creates a `ChildMemoryCircuitBreaker` (which tracks nothing since we never call `addEstimateBytesAndMaybeBreak` on it from Java). The parent's `memoryUsed()` iterates `this.breakers.values()` and calls `getUsed()` on each — but our breaker is a standard `ChildMemoryCircuitBreaker` with `used=0`, not our `NativeProxyCircuitBreaker`.

2. **Java-side allocations don't check combined JVM + Rust memory.** When Java allocates (BigArrays, aggregations), `checkParentLimit` only checks JVM heap. It doesn't know about Rust memory pressure.

3. **No way to inject a custom `CircuitBreaker` impl** through the existing plugin API. The breakers map is `Collections.unmodifiableMap` and `validateAndCreateBreaker` always creates `ChildMemoryCircuitBreaker` or `NoopCircuitBreaker`.

## Requirements

When **either** Java or Rust allocates memory, we need to check:
1. Is the **request** consuming too much memory? (child-level, per-runtime)
2. Is the **current runtime** (Java or Rust) approaching its allotted limit? (runtime-level)
3. Is the **node** running out of total memory? (node-level, cross-domain)

## Proposed Approaches

### Approach 1: Minimal Server Change — Extend `CircuitBreakerPlugin` (Recommended)

**Change to `server/`:** Add one method to `CircuitBreakerPlugin`:

```java
public interface CircuitBreakerPlugin {
    BreakerSettings getCircuitBreaker(Settings settings);
    void setCircuitBreaker(CircuitBreaker circuitBreaker);
    
    // NEW: optionally provide a custom CircuitBreaker implementation
    default Optional<CircuitBreaker> createCircuitBreaker(BreakerSettings settings, 
                                                           HierarchyCircuitBreakerService parent) {
        return Optional.empty(); // default: let the service create it
    }
}
```

**Change to `HierarchyCircuitBreakerService` constructor:** When creating custom breakers, check if the plugin provides its own implementation:

```java
// In constructor, when processing custom breakers:
for (BreakerSettings customSettings : customBreakers) {
    CircuitBreaker breaker = pluginMap.get(customSettings.getName())
        .flatMap(plugin -> plugin.createCircuitBreaker(customSettings, this))
        .orElseGet(() -> validateAndCreateBreaker(customSettings));
    childCircuitBreakers.put(customSettings.getName(), breaker);
}
```

**Plugin side:** `DataFusionPlugin.createCircuitBreaker()` returns our `NativeProxyCircuitBreaker` which:
- `getUsed()` returns cached Rust total memory (from background thread)
- `addEstimateBytesAndMaybeBreak()` is a no-op (enforcement is Rust-side)
- `getLimit()` returns the native memory budget

**How this solves the problems:**
- The parent's `memoryUsed()` now calls our `NativeProxyCircuitBreaker.getUsed()` → returns real Rust memory
- In **real-memory mode**: parent checks `JVM heap + newBytes > parentLimit`. Our breaker's `getUsed()` contributes to the durability classification. To include native memory in the actual limit check, we'd also need to modify `currentMemoryUsage()` — see below.
- In **estimated mode**: parent sums all children including ours → native memory directly contributes to the trip decision.

**For real-memory mode (additional change):** Override `currentMemoryUsage()` in `HierarchyCircuitBreakerService` to include native memory:

```java
@Override
long currentMemoryUsage() {
    long jvmHeap = realMemoryUsage();
    // If a native breaker is registered, add its usage
    CircuitBreaker nativeBreaker = this.breakers.get("native_request");
    if (nativeBreaker != null) {
        jvmHeap += nativeBreaker.getUsed();
    }
    return jvmHeap;
}
```

Or more generically, sum all custom breakers' `getUsed()` on top of JVM heap.

**Pros:**
- Minimal server change (2 small additions)
- Backward compatible (default method returns `Optional.empty()`)
- Plugin controls its own breaker implementation
- Parent breaker automatically sees native memory
- Works for both real-memory and estimated modes

**Cons:**
- Requires a server/ PR (but it's small and generic — benefits any plugin that needs custom breaker behavior)
- `currentMemoryUsage()` change means every parent check reads native memory (but it's cached, so ~1ns)

---

### Approach 2: No Server Change — ChildMemoryCircuitBreaker with Background Sync

**Idea:** Use the standard `ChildMemoryCircuitBreaker` that the service creates, but keep its `used` counter in sync with Rust memory via a background thread.

**How:** The background thread periodically (every 1s) reads jemalloc stats and calls `addWithoutBreaking(delta)` on the Java-side `ChildMemoryCircuitBreaker` to sync its counter with Rust reality.

```java
// Background thread (every 1s):
long rustTotal = NativeBridge.getBreakerStats()[3]; // total_used_bytes
long currentJava = nativeBreaker.getUsed();
long delta = rustTotal - currentJava;
nativeBreaker.addWithoutBreaking(delta); // sync Java counter to Rust reality
```

**How this solves the problems:**
- The Java-side `ChildMemoryCircuitBreaker` for `native_request` now has `used` ≈ Rust total memory (updated every 1s)
- Parent's `memoryUsed()` calls `getUsed()` on it → sees Rust memory
- In **real-memory mode**: still only checks JVM heap for the limit (native memory only affects durability). This is a gap.
- In **estimated mode**: native memory contributes to the parent sum.

**Pros:**
- Zero server changes
- Uses existing infrastructure exactly as designed
- Simple to implement

**Cons:**
- **Does NOT solve the real-memory mode gap** — parent still only checks JVM heap
- `addWithoutBreaking` bypasses the parent check (confirmed in code) — so the sync itself doesn't trigger parent evaluation
- 1s staleness in the Java-side counter
- Slightly hacky — we're abusing `addWithoutBreaking` for a purpose it wasn't designed for (syncing, not releasing)
- If the background thread dies, the counter drifts

---

### Approach 3: No Server Change — Wrap `HierarchyCircuitBreakerService`

**Idea:** Instead of using `HierarchyCircuitBreakerService` directly, create a `NativeAwareCircuitBreakerService` that wraps it and overrides `checkParentLimit`.

**Problem:** `createCircuitBreakerService` in `Node.java` is a static factory that returns `HierarchyCircuitBreakerService`. There's no plugin hook to replace it. We'd need to modify `Node.java` to allow plugins to provide a custom `CircuitBreakerService` — which is a bigger change than Approach 1.

**Verdict:** Not viable without a larger server change. Discarded.

---

## Background Thread for jemalloc Stats

Regardless of which approach is chosen, a background thread should refresh jemalloc stats:

```java
// NativeMemoryCollectorService (extends AbstractLifecycleComponent)
// Injected with ThreadPool in createComponents()

@Override
protected void doStart() {
    scheduledFuture = threadPool.scheduleWithFixedDelay(() -> {
        // 1. Call Rust to refresh jemalloc cache
        NativeBridge.refreshJemallocCache(); // new FFM function that calls epoch.advance() + stores result
        
        // 2. Read the cached value
        long rustTotal = NativeBridge.getBreakerStats()[3]; // total_used_bytes (now fresh)
        
        // 3. Update Java-side breaker counter (for Approach 2)
        // OR: store in a volatile field that NativeProxyCircuitBreaker reads (for Approach 1)
        cachedRustMemory = rustTotal;
        
    }, TimeValue.timeValueSeconds(1), ThreadPool.Names.GENERIC);
}
```

**Rust side:** Add `df_refresh_jemalloc_cache()` FFM function that calls `epoch.advance()` and stores the result in the cached atomic. The circuit breaker hot path only reads the cached value.

**Advantage:** The jemalloc refresh is completely decoupled from the query hot path. No query thread ever calls `epoch.advance()`.

---

## Recommendation

**Approach 1 (Extend CircuitBreakerPlugin)** is the cleanest solution:

1. **Small, generic server change** — adds `createCircuitBreaker()` default method to `CircuitBreakerPlugin` and a 3-line check in the constructor. Benefits any plugin needing custom breaker behavior.

2. **`currentMemoryUsage()` override** — adds native memory to the real-memory-mode parent check. This is the only way to truly solve the "Java allocations don't see Rust pressure" problem in real-memory mode.

3. **Background thread** — refreshes jemalloc every 1s, stores in a volatile. `NativeProxyCircuitBreaker.getUsed()` reads the volatile (zero FFM cost on the hot path).

4. **Rust-side CB unchanged** — still does child CAS + cached node check + Java parent upcall. The upcall now goes through the real parent check which includes native memory.

### Complete Flow After Fix

**Rust allocation (`try_grow`):**
1. Child CAS check (request memory)
2. Node check (cached jemalloc vs node_limit)
3. Java parent upcall → `checkParentLimit()` → `currentMemoryUsage()` returns JVM heap + native memory (from `NativeProxyCircuitBreaker.getUsed()` which reads the background-thread-cached volatile)

**Java allocation (`addEstimateBytesAndMaybeBreak`):**
1. Child CAS check (request memory) — existing
2. `checkParentLimit()` → `currentMemoryUsage()` returns JVM heap + native memory → checks combined against parent limit

**Both paths now check all three levels:**
- ✅ Request memory (child breaker)
- ✅ Runtime memory (Rust: jemalloc cached; Java: JVM heap via MemoryMXBean)
- ✅ Node memory (parent: JVM heap + Rust total)
