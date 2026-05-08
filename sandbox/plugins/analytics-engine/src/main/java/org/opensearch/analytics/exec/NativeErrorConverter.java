/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;

/**
 * Converts native Rust errors into appropriate Java exceptions.
 * <p>
 * Rust errors arrive as {@link RuntimeException} with encoded messages.
 * This class inspects the message prefix and converts to the appropriate
 * typed exception when possible.
 */
public final class NativeErrorConverter {

    private static final String CB_PREFIX = "CB:";

    private NativeErrorConverter() {}

    /**
     * Converts a native error to the appropriate Java exception type.
     * If the error doesn't match any known pattern, returns it unchanged.
     *
     * @param nativeError the exception from native code
     * @return a typed exception if conversion is possible, otherwise the original
     */
    public static Exception convert(Exception nativeError) {
        String msg = nativeError.getMessage();
        if (msg == null) {
            return nativeError;
        }
        if (msg.startsWith(CB_PREFIX)) {
            return convertCircuitBreakerError(msg, nativeError);
        }
        return nativeError;
    }

    /**
     * Walks the exception cause chain looking for a convertible native error.
     * Returns the converted exception if found, otherwise returns the original unchanged.
     */
    public static Exception convertChain(Exception root) {
        Throwable current = root;
        while (current != null) {
            String msg = current.getMessage();
            if (msg != null && msg.contains(CB_PREFIX)) {
                // Extract the CB: portion from the message (may be embedded in a longer string)
                int idx = msg.indexOf(CB_PREFIX);
                String cbMsg = msg.substring(idx);
                return convertCircuitBreakerError(cbMsg, root);
            }
            current = current.getCause();
        }
        return root;
    }

    private static Exception convertCircuitBreakerError(String msg, Exception cause) {
        String[] parts = msg.substring(CB_PREFIX.length()).split(":", 4);
        if (parts.length < 4) {
            CircuitBreakingException cbe = new CircuitBreakingException(msg, 0, 0, CircuitBreaker.Durability.TRANSIENT);
            cbe.initCause(cause);
            return cbe;
        }
        try {
            long bytesWanted = Long.parseLong(parts[0]);
            long bytesLimit = Long.parseLong(parts[1]);
            String reason = parts[3];
            CircuitBreakingException cbe = new CircuitBreakingException(reason, bytesWanted, bytesLimit, CircuitBreaker.Durability.TRANSIENT);
            cbe.initCause(cause);
            return cbe;
        } catch (NumberFormatException e) {
            CircuitBreakingException cbe = new CircuitBreakingException(msg, 0, 0, CircuitBreaker.Durability.TRANSIENT);
            cbe.initCause(cause);
            return cbe;
        }
    }
}
