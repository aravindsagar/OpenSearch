# Circuit Breaker Integration: Rust ↔ Java Memory Protection

## Background

OpenSearch uses **circuit breakers** to prevent out-of-memory crashes. The `HierarchyCircuitBreakerService` manages a tree of breakers:

- **Child breakers** (fielddata, request, in_flight_requests, plugin-registered) track memory used by specific subsystems
- **Parent breaker** checks total node memory before any child allocation is approved

In **real memory mode** (default), the parent reads JVM heap usage directly via `MemoryMXBean`. In **estimated mode**, it sums child breaker `used * overhead` values.

With the DataFusion integration, OpenSearch now has a **Rust runtime** (using jemalloc) that allocates memory outside the JVM heap. The Rust side has its own circuit breaker (`try_grow`) that checks:
1. Per-request memory vs a per-request limit
2. Total Rust memory (jemalloc) vs a Rust-side budget
3. An upcall to Java for node-level checks

## Requirements

When **either** Java or Rust allocates memory, the system must answer three questions:

| # | Question | Protects against |
|---|----------|-----------------|
| 1 | Is this request consuming too much memory? | Single query hogging resources |
| 2 | Is the current runtime (JVM or Rust) near its budget? | One runtime starving the other |
| 3 | Is total node memory (JVM + Rust) near exhaustion? | Node OOM kill |

Additionally:
- The DataFusion/Rust plugin is **optional** — OpenSearch must work identically without it
- Native memory must appear in `_nodes/stats/breaker` for observability
- The solution must not add FFM overhead to hot query paths

## Current Gaps

### Gap 1: Plugin-registered breaker is inert

`CircuitBreakerPlugin.getCircuitBreaker()` returns `BreakerSettings`. The service always creates a `ChildMemoryCircuitBreaker` from those settings — plugins cannot provide custom implementations. The created breaker's `used` stays at 0 because nothing on the Java side calls `addEstimateBytesAndMaybeBreak()` for Rust allocations.

**Impact:** `_nodes/stats` shows 0 bytes for native memory. Estimated-mode parent ignores Rust memory.

### Gap 2: Java parent breaker ignores native memory

```java
// HierarchyCircuitBreakerService.java
long currentMemoryUsage() {
    return MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed(); // JVM heap only
}
```

When Java code allocates (fielddata, request buffers, etc.), the parent check sees only JVM heap. If Rust is using 6GB and JVM is at 3GB on an 8GB node, the parent thinks usage is 3GB — not 9GB.

**Impact:** Java allocations can push the node into OOM when Rust memory is high.

## Solution Overview

The solution is split into two phases:

- **P0 (plugin-only):** A background thread syncs Rust memory stats to the Java child breaker, and the Rust→Java upcall performs a combined JVM+Rust node check. This covers the Rust allocation path fully and provides observability. The Java allocation path remains partially unprotected (acceptable for launch with proper node sizing).

- **P1 (server change):** Modify `currentMemoryUsage()` to include a plugin-supplied native memory value, closing the Java allocation path gap.

---

## P0: Plugin-Only Implementation

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Java (JVM Heap)                │  Rust (jemalloc)           │
│                                 │                            │
│  ChildMemoryCircuitBreaker      │  Rust CB (try_grow):       │
│  "native_request"               │    1. request vs limit     │
│  used = synced from Rust        │    2. jemalloc vs budget   │
│                                 │    3. upcall → Java        │
│  Parent breaker:                │                            │
│  checkParentLimit() →           │                            │
│    JVM heap only (P0 gap)       │                            │
└─────────────────────────────────────────────────────────────┘
         ▲                                    │
         │         Background Thread (1/sec)  │
         │    ┌───────────────────────────┐   │
         └────│ read jemalloc             │◄──┘
              │ update Rust CB cache      │
              │ sync delta → child breaker│
              └───────────────────────────┘
```

### Component 1: Background Poller Thread

A daemon thread runs every 1 second:

```java
long rustTotal = NativeBridge.getBreakerStats()[3]; // jemalloc allocated_bytes
NativeBridge.setCachedRustMemory(rustTotal);        // Rust query path reads this cache

long delta = rustTotal - nativeChildBreaker.getUsed();
nativeChildBreaker.addWithoutBreaking(delta);       // sync to Java child breaker
```

`ChildMemoryCircuitBreaker.addWithoutBreaking(delta)` updates the internal `AtomicLong` without tripping. By computing the delta from the current `getUsed()`, the child breaker tracks actual Rust memory.

**Benefits:**
- `_nodes/stats/breaker` shows real native memory
- Estimated-mode parent includes Rust memory in its sum
- Rust query path avoids calling jemalloc on every allocation (reads cached value instead)

### Component 2: Rust→Java Upcall (`checkParentFromRust`)

When Rust's `try_grow` passes its local checks, it upcalls to Java:

```java
static long checkParentFromRust(long bytesToReserve) {
    long jvmHeap = MemoryMXBean.getHeapMemoryUsage().getUsed();
    long rustTotal = cachedRustMemory; // from background thread
    if (jvmHeap + rustTotal + bytesToReserve > nodeLimit) {
        return 1; // trip
    }
    return 0;
}
```

This is a **plugin-side combined check** that compensates for the parent breaker not knowing about native memory. It protects the Rust allocation path against node-level OOM.

### Component 3: Coordinator Class

`NativeProxyCircuitBreaker` is repurposed as `NativeCircuitBreakerCoordinator` — it no longer implements `CircuitBreaker`. It:
- Holds the `ChildMemoryCircuitBreaker` reference (received via `setCircuitBreaker()`)
- Manages the background poller
- Provides the static `checkParentFromRust` upcall
- Stores `cachedRustMemory` and `nodeLimit`

### Wiring

```java
// DataFusionPlugin.java
public void setCircuitBreaker(CircuitBreaker circuitBreaker) {
    // Called by Node.java before createComponents() — store reference
    this.nativeBreaker = circuitBreaker;
}

// In createComponents():
coordinator = new NativeCircuitBreakerCoordinator(nativeBreaker, nodeLimit);
coordinator.start(); // starts background thread + registers upcall
```

### P0 Coverage Matrix

| Allocation path | Req #1 (per-request) | Req #2 (runtime budget) | Req #3 (node total) |
|----------------|---------------------|------------------------|---------------------|
| Rust allocates | ✅ Rust CB | ✅ Rust CB (jemalloc) | ✅ `checkParentFromRust` |
| Java allocates | ✅ Child breaker | ✅ JVM heap vs limit | ❌ Parent sees JVM only |

The Java→node gap is acceptable for launch: the Rust upcall prevents Rust from over-allocating, and Java's parent limit (95% of JVM heap) independently caps JVM usage. OOM requires both to be near limits simultaneously.

---

## P1: Server-Side Change (Fast Follow)

### Goal

Make `checkParentLimit()` include native memory when Java allocates, closing the last gap.

### Option A: Native Memory Supplier (Recommended)

Add a supplier hook to `HierarchyCircuitBreakerService`:

```java
// Server change (~10 lines):
private volatile LongSupplier nativeMemorySupplier = () -> 0L;

public void registerNativeMemory(LongSupplier supplier, long nativeBudget) {
    this.nativeMemorySupplier = supplier;
    // Adjust parent limit to account for native budget
    long adjustedLimit = this.parentSettings.getLimit() + (long)(nativeBudget * 0.95);
    this.parentSettings = new BreakerSettings(PARENT, adjustedLimit, 1.0, Type.PARENT, null);
}

long currentMemoryUsage() {
    return realMemoryUsage() + nativeMemorySupplier.getAsLong();
}
```

Plugin wires it:
```java
service.registerNativeMemory(
    () -> NativeCircuitBreakerCoordinator.getCachedRustMemory(),
    configuredRustBudget
);
```

To give the plugin access to `CircuitBreakerService`, add a callback to `CircuitBreakerPlugin`:
```java
// New default method:
default void setCircuitBreakerService(CircuitBreakerService service) {}
```

**Pros:**
- Minimal server change, backward compatible (supplier defaults to 0)
- Works with existing G1GC recovery strategy
- Parent limit auto-adjusts for native budget
- Plugin reads cached value — no FFM call in the hot path

**Cons:**
- Requires new `setCircuitBreakerService` callback (small API addition)
- Only one native supplier supported (sufficient — only one native runtime expected)

### Option B: Custom CircuitBreaker Registration

Allow plugins to provide a fully constructed `CircuitBreaker` instead of just `BreakerSettings`:

```java
// New method on CircuitBreakerPlugin:
default CircuitBreaker createCircuitBreaker(Settings settings, HierarchyCircuitBreakerService service) {
    return null; // null = use existing BreakerSettings path
}
```

**Pros:**
- Plugin has full control over `getUsed()`, `getTrippedCount()`, etc.
- Clean extension point for future plugins

**Cons:**
- Does **not** fix the parent check — in real memory mode, `currentMemoryUsage()` still ignores child `getUsed()` values. The parent never calls `getUsed()` on children for its check.
- Larger API surface change
- Would still need Option A to actually close the gap

### Recommendation

**Option A alone** is sufficient and minimal. Option B provides cleaner stats but doesn't solve the core problem without Option A. If both are desired, implement Option A first (it closes the gap), then Option B as a refinement.

---

## P1 Coverage Matrix (with Option A)

| Allocation path | Req #1 (per-request) | Req #2 (runtime budget) | Req #3 (node total) |
|----------------|---------------------|------------------------|---------------------|
| Rust allocates | ✅ Rust CB | ✅ Rust CB (jemalloc) | ✅ `checkParentFromRust` |
| Java allocates | ✅ Child breaker | ✅ JVM heap vs limit | ✅ Parent sees JVM + native |
