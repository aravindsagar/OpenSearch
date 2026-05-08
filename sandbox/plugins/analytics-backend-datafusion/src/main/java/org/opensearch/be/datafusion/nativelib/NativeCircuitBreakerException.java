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
 * Converts native Rust circuit breaker errors (CB: prefix) into {@link CircuitBreakingException}.
 */
public final class NativeCircuitBreakerException {

    private static final String CB_PREFIX = "CB:";

    private NativeCircuitBreakerException() {}

    /**
     * If the exception message starts with "CB:", parse into CircuitBreakingException.
     * Otherwise return the original exception unchanged.
     */
    public static Exception maybeConvert(Exception nativeError) {
        String msg = nativeError.getMessage();
        if (msg == null || msg.startsWith(CB_PREFIX) == false) {
            return nativeError;
        }
        String[] parts = msg.substring(CB_PREFIX.length()).split(":", 4);
        if (parts.length < 4) {
            return nativeError;
        }
        try {
            long bytesWanted = Long.parseLong(parts[0]);
            long bytesLimit = Long.parseLong(parts[1]);
            String reason = parts[3];
            return new CircuitBreakingException(reason, bytesWanted, bytesLimit, CircuitBreaker.Durability.TRANSIENT);
        } catch (NumberFormatException e) {
            return nativeError;
        }
    }
}
