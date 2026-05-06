/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Native circuit breaker mirroring OpenSearch's `ChildMemoryCircuitBreaker`.
//!
//! Tracks aggregate query-path memory and rejects allocations when usage
//! exceeds the configured limit × overhead. Exposes stats for the Java-side
//! `NativeProxyCircuitBreaker` to read via FFM.

use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use once_cell::sync::OnceCell;

/// Global singleton breaker instance.
static BREAKER: OnceCell<NativeRequestBreaker> = OnceCell::new();

/// Initialize the global breaker. Returns `Err` if already initialized.
pub fn init(limit_bytes: usize, overhead_millionths: u64) -> Result<(), &'static str> {
    BREAKER
        .set(NativeRequestBreaker {
            limit_bytes: AtomicUsize::new(limit_bytes),
            used_bytes: AtomicUsize::new(0),
            tripped_count: AtomicU64::new(0),
            overhead_millionths: AtomicU64::new(overhead_millionths),
        })
        .map_err(|_| "circuit breaker already initialized")
}

/// Get a reference to the global breaker, if initialized.
pub fn get() -> Option<&'static NativeRequestBreaker> {
    BREAKER.get()
}

// ---------------------------------------------------------------------------
// NativeRequestBreaker
// ---------------------------------------------------------------------------

/// Rust-side child circuit breaker for DataFusion query memory.
pub struct NativeRequestBreaker {
    limit_bytes: AtomicUsize,
    used_bytes: AtomicUsize,
    tripped_count: AtomicU64,
    overhead_millionths: AtomicU64,
}

impl NativeRequestBreaker {
    /// Check limit and reserve bytes atomically. Returns error if breaker trips.
    ///
    /// Uses `fetch_update` (internally `compare_exchange_weak`) for lock-free
    /// accounting. Overhead is applied only during the limit check, not stored
    /// in `used_bytes` — so `release(N)` can simply subtract N.
    pub fn try_reserve(&self, bytes: usize) -> Result<(), CircuitBreakError> {
        let limit = self.limit_bytes.load(Ordering::Relaxed);
        // limit == 0 means disabled (no-op)
        if limit == 0 {
            return Ok(());
        }
        let overhead = self.overhead();

        let result = self.used_bytes.fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current| {
            let with_overhead = ((current + bytes) as f64 * overhead) as usize;
            if with_overhead > limit {
                None // reject — triggers Err(current) from fetch_update
            } else {
                Some(current + bytes)
            }
        });

        match result {
            Ok(_) => Ok(()),
            Err(current) => {
                self.tripped_count.fetch_add(1, Ordering::Relaxed);
                Err(CircuitBreakError {
                    bytes_wanted: bytes,
                    bytes_limit: limit,
                    current_used: current,
                })
            }
        }
    }

    /// Release bytes without limit check. Called on shrink/free.
    pub fn release(&self, bytes: usize) {
        self.used_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Update limit dynamically (from cluster settings).
    pub fn set_limit(&self, new_limit: usize) {
        self.limit_bytes.store(new_limit, Ordering::Release);
    }

    /// Update overhead dynamically.
    pub fn set_overhead(&self, overhead_millionths: u64) {
        self.overhead_millionths.store(overhead_millionths, Ordering::Release);
    }

    /// Snapshot stats for Java to read.
    pub fn stats(&self) -> BreakerStats {
        BreakerStats {
            limit_bytes: self.limit_bytes.load(Ordering::Relaxed),
            used_bytes: self.used_bytes.load(Ordering::Relaxed),
            tripped_count: self.tripped_count.load(Ordering::Relaxed),
            overhead_millionths: self.overhead_millionths.load(Ordering::Relaxed),
        }
    }

    fn overhead(&self) -> f64 {
        self.overhead_millionths.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }
}

// ---------------------------------------------------------------------------
// CircuitBreakError
// ---------------------------------------------------------------------------

/// Error returned when the breaker trips. Carries structured fields so Java
/// can reconstruct a `CircuitBreakingException`.
#[derive(Debug, Clone)]
pub struct CircuitBreakError {
    pub bytes_wanted: usize,
    pub bytes_limit: usize,
    pub current_used: usize,
}

impl CircuitBreakError {
    /// Encode as a structured string for FFM error propagation.
    /// Format: "CB:<bytes_wanted>:<bytes_limit>:<current_used>:<message>"
    /// The last field (message) may contain colons — Java splits with limit 4.
    pub fn to_encoded_string(&self) -> String {
        format!("CB:{}:{}:{}:{}", self.bytes_wanted, self.bytes_limit, self.current_used, self)
    }
}

impl fmt::Display for CircuitBreakError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[native_request] Data too large, data would be [{}/{}], \
             which is larger than the limit of [{}/{}]",
            self.current_used + self.bytes_wanted,
            human_bytes(self.current_used + self.bytes_wanted),
            self.bytes_limit,
            human_bytes(self.bytes_limit),
        )
    }
}

// ---------------------------------------------------------------------------
// BreakerStats
// ---------------------------------------------------------------------------

/// Stats snapshot returned to Java via FFM.
#[derive(Debug, Clone)]
pub struct BreakerStats {
    pub limit_bytes: usize,
    pub used_bytes: usize,
    pub tripped_count: u64,
    pub overhead_millionths: u64,
}

// ---------------------------------------------------------------------------
// NativeNodeBreaker (parent-level, node-wide check)
// ---------------------------------------------------------------------------

static NODE_BREAKER: OnceCell<NativeNodeBreaker> = OnceCell::new();

/// Upcall function pointer: calls Java's checkParentLimit.
/// Signature: (bytes_to_reserve: i64) -> i64 (0 = OK, 1 = tripped)
pub type CheckParentFn = unsafe extern "C" fn(i64) -> i64;
static CHECK_PARENT_CALLBACK: std::sync::atomic::AtomicPtr<()> =
    std::sync::atomic::AtomicPtr::new(std::ptr::null_mut());

/// Register the Java parent check callback. Called once at startup.
pub fn register_parent_check_callback(fn_ptr: CheckParentFn) {
    CHECK_PARENT_CALLBACK.store(fn_ptr as *mut (), Ordering::Release);
}

/// Initialize the node-level breaker.
pub fn init_node_breaker(node_limit: usize) -> Result<(), &'static str> {
    NODE_BREAKER
        .set(NativeNodeBreaker {
            node_limit: AtomicUsize::new(node_limit),
            tripped_count: AtomicU64::new(0),
        })
        .map_err(|_| "node breaker already initialized")
}

/// Get the node-level breaker, if initialized.
pub fn get_node_breaker() -> Option<&'static NativeNodeBreaker> {
    NODE_BREAKER.get()
}

/// Rust-side parent breaker. Upcalls to Java's checkParentLimit for a
/// real-time node-level check combining JVM heap + native memory.
pub struct NativeNodeBreaker {
    node_limit: AtomicUsize,
    tripped_count: AtomicU64,
}

impl NativeNodeBreaker {
    /// Upcall to Java's checkParentLimit for a real-time combined check.
    /// Returns Ok if the parent allows the allocation, Err if it trips.
    pub fn check_node_limit(&self, bytes: usize) -> Result<(), CircuitBreakError> {
        let fn_ptr = CHECK_PARENT_CALLBACK.load(Ordering::Acquire);
        if fn_ptr.is_null() {
            // Callback not registered — skip node check
            return Ok(());
        }
        let check_parent: CheckParentFn = unsafe { std::mem::transmute(fn_ptr) };
        let result = unsafe { check_parent(bytes as i64) };
        if result != 0 {
            let limit = self.node_limit.load(Ordering::Relaxed);
            self.tripped_count.fetch_add(1, Ordering::Relaxed);
            return Err(CircuitBreakError {
                bytes_wanted: bytes,
                bytes_limit: limit,
                current_used: 0, // Java side has the real numbers
            });
        }
        Ok(())
    }

    pub fn set_limit(&self, new_limit: usize) {
        self.node_limit.store(new_limit, Ordering::Release);
    }

    pub fn stats(&self) -> NodeBreakerStats {
        NodeBreakerStats {
            node_limit: self.node_limit.load(Ordering::Relaxed),
            tripped_count: self.tripped_count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeBreakerStats {
    pub node_limit: usize,
    pub tripped_count: u64,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn human_bytes(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = 1024 * KB;
    const GB: usize = 1024 * MB;
    if bytes >= GB {
        format!("{:.1}gb", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1}mb", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1}kb", bytes as f64 / KB as f64)
    } else {
        format!("{}b", bytes)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    // Helper: create a standalone breaker (not the global singleton) for testing.
    fn make_breaker(limit: usize, overhead: f64) -> NativeRequestBreaker {
        NativeRequestBreaker {
            limit_bytes: AtomicUsize::new(limit),
            used_bytes: AtomicUsize::new(0),
            tripped_count: AtomicU64::new(0),
            overhead_millionths: AtomicU64::new((overhead * 1_000_000.0) as u64),
        }
    }

    #[test]
    fn test_basic_reserve_and_release() {
        let b = make_breaker(1000, 1.0);
        assert!(b.try_reserve(500).is_ok());
        assert_eq!(b.stats().used_bytes, 500);
        assert!(b.try_reserve(400).is_ok());
        assert_eq!(b.stats().used_bytes, 900);
        b.release(900);
        assert_eq!(b.stats().used_bytes, 0);
    }

    #[test]
    fn test_trip_at_limit() {
        let b = make_breaker(1000, 1.0);
        assert!(b.try_reserve(1000).is_ok());
        let err = b.try_reserve(1).unwrap_err();
        assert_eq!(err.bytes_wanted, 1);
        assert_eq!(err.bytes_limit, 1000);
        assert_eq!(err.current_used, 1000);
        assert_eq!(b.stats().tripped_count, 1);
    }

    #[test]
    fn test_overhead_multiplier() {
        let b = make_breaker(1000, 2.0);
        // 400 * 2.0 = 800 < 1000 → OK
        assert!(b.try_reserve(400).is_ok());
        // (400 + 200) * 2.0 = 1200 > 1000 → TRIP
        assert!(b.try_reserve(200).is_err());
        assert_eq!(b.stats().tripped_count, 1);
    }

    #[test]
    fn test_dynamic_limit_update() {
        let b = make_breaker(1000, 1.0);
        assert!(b.try_reserve(500).is_ok());
        b.set_limit(400);
        // 500 + 100 = 600 > 400 → TRIP
        assert!(b.try_reserve(100).is_err());
    }

    #[test]
    fn test_disabled_when_limit_zero() {
        let b = make_breaker(0, 1.0);
        assert!(b.try_reserve(999_999_999).is_ok());
        assert_eq!(b.stats().used_bytes, 0); // not tracked when disabled
    }

    #[test]
    fn test_concurrent_reserves() {
        let b = Arc::new(make_breaker(1_000_000, 1.0));
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let b = Arc::clone(&b);
                thread::spawn(move || {
                    let mut reserved = 0usize;
                    for _ in 0..1000 {
                        if b.try_reserve(1).is_ok() {
                            reserved += 1;
                        }
                    }
                    reserved
                })
            })
            .collect();
        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(b.stats().used_bytes, total);
        assert_eq!(total, 10_000); // all should succeed (10k < 1M)
    }

    #[test]
    fn test_error_encoding() {
        let err = CircuitBreakError {
            bytes_wanted: 1024,
            bytes_limit: 512,
            current_used: 256,
        };
        let encoded = err.to_encoded_string();
        assert!(encoded.starts_with("CB:1024:512:256:"));
        assert!(encoded.contains("native_request"));
    }
}
