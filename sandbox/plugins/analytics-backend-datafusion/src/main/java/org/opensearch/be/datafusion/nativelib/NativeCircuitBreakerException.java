/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;

/**
 * Converts native Rust circuit breaker errors into {@link CircuitBreakingException}.
 *
 * <p>Rust encodes circuit breaker trips as error strings with a {@code CB:} prefix:
 * <pre>CB:{bytes_wanted}:{bytes_limit}:{current_used}:{human_message}</pre>
 *
 * <p>This utility parses that format and constructs the equivalent Java exception
 * so the user sees the same HTTP 429 response as a Java-side breaker trip.
 */
public final class NativeCircuitBreakerException {

    private static final String CB_PREFIX = "CB:";

    private NativeCircuitBreakerException() {}

    /**
     * If the exception message starts with "CB:", parse it into a {@link CircuitBreakingException}.
     * Otherwise return the original exception unchanged.
     */
    public static Exception maybeConvert(Exception nativeError) {
        String msg = nativeError.getMessage();
        if (msg == null || msg.startsWith(CB_PREFIX) == false) {
            return nativeError;
        }
        // Format: "CB:<bytes_wanted>:<bytes_limit>:<current_used>:<message>"
        // Split with limit 5 so the message (which may contain colons) is preserved intact.
        String[] parts = msg.substring(CB_PREFIX.length()).split(":", 4);
        if (parts.length < 4) {
            return nativeError;
        }
        try {
            long bytesWanted = Long.parseLong(parts[0]);
            long bytesLimit = Long.parseLong(parts[1]);
            // parts[2] = currentUsed (not needed for the exception constructor)
            String reason = parts[3];
            return new CircuitBreakingException(reason, bytesWanted, bytesLimit, CircuitBreaker.Durability.TRANSIENT);
        } catch (NumberFormatException e) {
            return nativeError;
        }
    }
}
