/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Cached jemalloc memory statistics, refreshed every second.
//!
//! Provides total native memory usage without the cost of `epoch.advance()` per call.
//! Used by the circuit breaker for node-level checks and available to any component
//! that needs to know total Rust-side memory usage.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use log::error;
use tokio::runtime::Handle as TokioHandle;

static CACHED_ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);

/// Start the background refresh timer on the given Tokio runtime.
/// Should be called once during runtime initialization (e.g., `df_init_runtime_manager`).
pub fn start_refresh_timer(io_runtime: &TokioHandle) {
    io_runtime.spawn(async {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let alloc = native_bridge_common::allocator::allocated_bytes();
                if alloc > 0 {
                    CACHED_ALLOCATED_BYTES.store(alloc as usize, Ordering::Release);
                }
            }));
            if result.is_err() {
                error!("jemalloc stats refresh panicked — will retry next tick");
            }
        }
    });
}

/// Returns the cached total native memory allocated (via jemalloc).
/// Updated every ~1 second by the background timer. Returns 0 if the timer hasn't run yet.
pub fn allocated_bytes() -> usize {
    CACHED_ALLOCATED_BYTES.load(Ordering::Acquire)
}

/// Set the cached value directly. For testing only.
#[cfg(test)]
pub fn set_allocated_bytes_for_test(bytes: usize) {
    CACHED_ALLOCATED_BYTES.store(bytes, Ordering::Release);
}
