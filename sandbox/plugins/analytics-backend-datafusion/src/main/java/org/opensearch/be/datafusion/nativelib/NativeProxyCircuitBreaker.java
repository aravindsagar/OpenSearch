/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;

/**
 * A read-only proxy that implements {@link CircuitBreaker} by reading stats from the
 * Rust-side NativeRequestBreaker via FFM. Enforcement happens entirely in Rust —
 * the Java methods {@code addEstimateBytesAndMaybeBreak} and {@code addWithoutBreaking}
 * are no-ops.
 *
 * <p><b>Naive benchmark mode:</b> {@code getUsed()} makes a real FFM downcall on every
 * invocation (no caching). The Rust side upcalls to Java's {@code checkParentLimit}
 * on every allocation for real-time node-level checks.
 *
 * <p>This class is registered via {@code CircuitBreakerPlugin} so that:
 * <ul>
 *   <li>The native breaker appears in {@code _nodes/stats} under the {@code breakers} section</li>
 *   <li>The Java parent breaker can include native memory in its aggregation</li>
 * </ul>
 */
public class NativeProxyCircuitBreaker implements CircuitBreaker {

    private static final Logger logger = LogManager.getLogger(NativeProxyCircuitBreaker.class);
    private static final String NAME = "native_request";

    /** Reference to the breaker service for the upcall. Set once during init. */
    private static volatile HierarchyCircuitBreakerService breakerService;

    private volatile long limitBytes;
    private volatile double overhead;

    /**
     * @param limitBytes initial breaker limit
     * @param overhead initial overhead multiplier
     * @param service the hierarchy breaker service for parent check upcalls
     */
    public NativeProxyCircuitBreaker(long limitBytes, double overhead, HierarchyCircuitBreakerService service) {
        this.limitBytes = limitBytes;
        this.overhead = overhead;
        breakerService = service;
    }

    /**
     * Static callback invoked by Rust via FFM upcall. Calls Java's checkParentLimit
     * for a real-time node-level check (JVM heap + all children including native proxy).
     *
     * @param bytesToReserve bytes the Rust side wants to allocate
     * @return 0 if OK, 1 if the parent breaker tripped
     */
    public static long checkParentFromRust(long bytesToReserve) {
        HierarchyCircuitBreakerService svc = breakerService;
        if (svc == null) {
            return 0; // not initialized yet — allow
        }
        try {
            svc.checkParentLimit(bytesToReserve, "native_request");
            return 0; // OK
        } catch (CircuitBreakingException e) {
            logger.warn("Java parent breaker tripped on Rust upcall: {}", e.getMessage());
            return 1; // tripped
        }
    }

    @Override
    public double addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        // No-op: enforcement happens in Rust inside QueryMemoryPool.try_grow()
        return 0;
    }

    @Override
    public long addWithoutBreaking(long bytes) {
        // No-op: Rust tracks its own used_bytes
        return 0;
    }

    @Override
    public long getUsed() {
        // FFM downcall for fresh Rust-side stats on every invocation (naive — no caching).
        long[] stats = NativeBridge.getBreakerStats();
        return stats[1]; // used_bytes from Rust
    }

    @Override
    public long getLimit() {
        return limitBytes;
    }

    @Override
    public double getOverhead() {
        return overhead;
    }

    @Override
    public long getTrippedCount() {
        long[] stats = NativeBridge.getBreakerStats();
        return stats[2];
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Durability getDurability() {
        return Durability.TRANSIENT;
    }

    @Override
    public void setLimitAndOverhead(long newLimit, double newOverhead) {
        this.limitBytes = newLimit;
        this.overhead = newOverhead;
        NativeBridge.setBreakerLimit(newLimit);
        NativeBridge.setBreakerOverhead((long) (newOverhead * 1_000_000));
    }
}
