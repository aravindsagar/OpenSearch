/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Native circuit breaker for DataFusion query memory.
//!
//! A single `NativeCircuitBreaker` performs three checks on every allocation:
//! 1. **Child check:** query-path `request_used_bytes × overhead > child_limit`
//! 2. **Node check:** total Rust process memory (jemalloc) `> node_limit`
//! 3. **Java parent check:** upcall to Java's combined JVM + native check
//!
//! Stats expose both `request_used_bytes` (tracked via CAS) and `total_used_bytes`
//! (from jemalloc) so Java can use whichever is appropriate.

use std::fmt;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use log::{debug, warn};
use once_cell::sync::OnceCell;

static BREAKER: OnceCell<NativeCircuitBreaker> = OnceCell::new();
static START_TIME: once_cell::sync::Lazy<Instant> = once_cell::sync::Lazy::new(Instant::now);

/// Upcall to Java's parent check. Signature: (bytes: i64) -> i64 (0=OK, non-zero=tripped)
pub type CheckParentFn = unsafe extern "C" fn(i64) -> i64;
static CHECK_PARENT_CALLBACK: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

/// Initialize the circuit breaker. Called once at startup.
pub fn init(child_limit: usize, node_limit: usize, overhead_millionths: u64) -> Result<(), &'static str> {
    debug!(
        "Initializing NativeCircuitBreaker: child_limit={}B, node_limit={}B, overhead={:.6}",
        child_limit, node_limit, overhead_millionths as f64 / 1_000_000.0
    );
    BREAKER
        .set(NativeCircuitBreaker {
            child_limit: AtomicUsize::new(child_limit),
            node_limit: AtomicUsize::new(node_limit),
            request_used_bytes: AtomicUsize::new(0),
            overhead_millionths: AtomicU64::new(overhead_millionths),
            child_tripped: AtomicU64::new(0),
            node_tripped: AtomicU64::new(0),
            parent_tripped: AtomicU64::new(0),
            last_node_check_ns: AtomicU64::new(0),
            last_node_check_ok: std::sync::atomic::AtomicBool::new(true),
        })
        .map_err(|_| "circuit breaker already initialized")
}

/// Register the Java parent check upcall. Called once at startup.
pub fn register_parent_callback(fn_ptr: CheckParentFn) {
    CHECK_PARENT_CALLBACK.store(fn_ptr as *mut (), Ordering::Release);
}

/// Get the global breaker instance.
pub fn get() -> Option<&'static NativeCircuitBreaker> {
    BREAKER.get()
}

// ---------------------------------------------------------------------------
// NativeCircuitBreaker
// ---------------------------------------------------------------------------

pub struct NativeCircuitBreaker {
    child_limit: AtomicUsize,
    node_limit: AtomicUsize,
    request_used_bytes: AtomicUsize,
    overhead_millionths: AtomicU64,
    child_tripped: AtomicU64,
    node_tripped: AtomicU64,
    parent_tripped: AtomicU64,
    // Time-based cache: node + parent checks run at most once per second
    last_node_check_ns: AtomicU64,
    last_node_check_ok: std::sync::atomic::AtomicBool,
}

/// Cache TTL for node + parent checks (1 second in nanoseconds).
const NODE_CHECK_CACHE_NS: u64 = 1_000_000_000;

impl NativeCircuitBreaker {
    /// Main entry point: check all levels and reserve bytes.
    /// - Child check: ALWAYS (cheap atomic CAS)
    /// - Node + parent check: at most once per second (time-based cache)
    pub fn check_and_reserve(&self, bytes: usize) -> Result<(), CircuitBreakError> {
        // 1. Child check — always (real-time per-query protection)
        self.check_child(bytes)?;

        // 2. Node + parent check — time-gated (at most once per second)
        let now = self.now_ns();
        let last = self.last_node_check_ns.load(Ordering::Relaxed);
        if now.saturating_sub(last) > NODE_CHECK_CACHE_NS {
            // Time to run a fresh check. CAS to claim it (avoid thundering herd).
            if self.last_node_check_ns.compare_exchange(last, now, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                debug!("CB: running fresh node+parent check ({}ms since last)", (now - last) / 1_000_000);
                let ok = self.run_node_and_parent_check(bytes);
                self.last_node_check_ok.store(ok, Ordering::Release);
                if !ok {
                    self.request_used_bytes.fetch_sub(bytes, Ordering::Relaxed);
                    return Err(CircuitBreakError {
                        bytes_wanted: bytes,
                        bytes_limit: self.node_limit.load(Ordering::Relaxed),
                        current_used: self.total_used_bytes(),
                    });
                }
            }
        } else if !self.last_node_check_ok.load(Ordering::Acquire) {
            // Last check tripped — keep rejecting until next fresh check clears it
            self.request_used_bytes.fetch_sub(bytes, Ordering::Relaxed);
            return Err(CircuitBreakError {
                bytes_wanted: bytes,
                bytes_limit: self.node_limit.load(Ordering::Relaxed),
                current_used: self.total_used_bytes(),
            });
        }

        Ok(())
    }

    /// Runs the expensive node + parent checks. Returns true if OK, false if tripped.
    fn run_node_and_parent_check(&self, bytes: usize) -> bool {
        if let Err(_) = self.check_node(bytes) {
            return false;
        }
        if let Err(_) = self.check_parent(bytes) {
            return false;
        }
        true
    }

    /// Release bytes. Called from `QueryMemoryPool.shrink()`.
    pub fn release(&self, bytes: usize) {
        self.request_used_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Child-level check: (request_used + bytes) × overhead > child_limit?
    fn check_child(&self, bytes: usize) -> Result<(), CircuitBreakError> {
        let limit = self.child_limit.load(Ordering::Relaxed);
        if limit == 0 {
            return Ok(());
        }
        let overhead = self.overhead();

        let result = self.request_used_bytes.fetch_update(
            Ordering::AcqRel, Ordering::Relaxed, |current| {
                let with_overhead = ((current + bytes) as f64 * overhead) as usize;
                if with_overhead > limit { None } else { Some(current + bytes) }
            }
        );

        match result {
            Ok(_) => Ok(()),
            Err(current) => {
                self.child_tripped.fetch_add(1, Ordering::Relaxed);
                warn!("CB child tripped: wanted {}B, current {}B, limit {}B", bytes, current, limit);
                Err(CircuitBreakError { bytes_wanted: bytes, bytes_limit: limit, current_used: current })
            }
        }
    }

    /// Node-level check: jemalloc total allocated + bytes > node_limit?
    fn check_node(&self, bytes: usize) -> Result<(), CircuitBreakError> {
        let limit = self.node_limit.load(Ordering::Relaxed);
        if limit == 0 {
            return Ok(());
        }
        let total = self.total_used_bytes();
        if total + bytes > limit {
            self.node_tripped.fetch_add(1, Ordering::Relaxed);
            warn!("CB node tripped: total {}B + wanted {}B > limit {}B", total, bytes, limit);
            return Err(CircuitBreakError { bytes_wanted: bytes, bytes_limit: limit, current_used: total });
        }
        Ok(())
    }

    /// Java parent check via upcall.
    fn check_parent(&self, bytes: usize) -> Result<(), CircuitBreakError> {
        let fn_ptr = CHECK_PARENT_CALLBACK.load(Ordering::Acquire);
        if fn_ptr.is_null() {
            return Ok(());
        }
        let check: CheckParentFn = unsafe { std::mem::transmute(fn_ptr) };
        let result = unsafe { check(bytes as i64) };
        if result != 0 {
            self.parent_tripped.fetch_add(1, Ordering::Relaxed);
            warn!("CB parent tripped (Java upcall): wanted {}B", bytes);
            return Err(CircuitBreakError {
                bytes_wanted: bytes,
                bytes_limit: self.node_limit.load(Ordering::Relaxed),
                current_used: self.total_used_bytes(),
            });
        }
        Ok(())
    }

    /// Total Rust-side memory from jemalloc. Falls back to request_used_bytes if unavailable.
    pub fn total_used_bytes(&self) -> usize {
        let jemalloc = native_bridge_common::allocator::allocated_bytes();
        if jemalloc > 0 { jemalloc as usize } else { self.request_used_bytes.load(Ordering::Relaxed) }
    }

    // --- Dynamic updates ---

    pub fn set_child_limit(&self, limit: usize) {
        self.child_limit.store(limit, Ordering::Release);
    }

    pub fn set_node_limit(&self, limit: usize) {
        self.node_limit.store(limit, Ordering::Release);
    }

    pub fn set_overhead(&self, overhead_millionths: u64) {
        self.overhead_millionths.store(overhead_millionths, Ordering::Release);
    }

    // --- Stats ---

    pub fn stats(&self) -> BreakerStats {
        BreakerStats {
            child_limit: self.child_limit.load(Ordering::Relaxed),
            node_limit: self.node_limit.load(Ordering::Relaxed),
            request_used_bytes: self.request_used_bytes.load(Ordering::Relaxed),
            total_used_bytes: self.total_used_bytes(),
            child_tripped: self.child_tripped.load(Ordering::Relaxed),
            node_tripped: self.node_tripped.load(Ordering::Relaxed),
            parent_tripped: self.parent_tripped.load(Ordering::Relaxed),
            overhead_millionths: self.overhead_millionths.load(Ordering::Relaxed),
        }
    }

    fn overhead(&self) -> f64 {
        self.overhead_millionths.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }

    fn now_ns(&self) -> u64 {
        START_TIME.elapsed().as_nanos() as u64
    }
}

// ---------------------------------------------------------------------------
// CircuitBreakError
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CircuitBreakError {
    pub bytes_wanted: usize,
    pub bytes_limit: usize,
    pub current_used: usize,
}

impl CircuitBreakError {
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

#[derive(Debug, Clone)]
pub struct BreakerStats {
    pub child_limit: usize,
    pub node_limit: usize,
    pub request_used_bytes: usize,
    pub total_used_bytes: usize,
    pub child_tripped: u64,
    pub node_tripped: u64,
    pub parent_tripped: u64,
    pub overhead_millionths: u64,
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

    fn make_breaker(child_limit: usize, node_limit: usize, overhead: f64) -> NativeCircuitBreaker {
        NativeCircuitBreaker {
            child_limit: AtomicUsize::new(child_limit),
            node_limit: AtomicUsize::new(node_limit),
            request_used_bytes: AtomicUsize::new(0),
            overhead_millionths: AtomicU64::new((overhead * 1_000_000.0) as u64),
            child_tripped: AtomicU64::new(0),
            node_tripped: AtomicU64::new(0),
            parent_tripped: AtomicU64::new(0),
            last_node_check_ns: AtomicU64::new(0),
            last_node_check_ok: std::sync::atomic::AtomicBool::new(true),
        }
    }

    #[test]
    fn test_child_check_trips() {
        let b = make_breaker(1000, 0, 1.0); // node disabled
        assert!(b.check_and_reserve(500).is_ok());
        assert!(b.check_and_reserve(500).is_ok());
        assert!(b.check_and_reserve(1).is_err());
        assert_eq!(b.stats().child_tripped, 1);
    }

    #[test]
    fn test_child_with_overhead() {
        let b = make_breaker(1000, 0, 2.0);
        assert!(b.check_and_reserve(400).is_ok()); // 400*2=800 < 1000
        assert!(b.check_and_reserve(200).is_err()); // (400+200)*2=1200 > 1000
    }

    #[test]
    fn test_release() {
        let b = make_breaker(1000, 0, 1.0);
        assert!(b.check_and_reserve(900).is_ok());
        b.release(900);
        assert!(b.check_and_reserve(900).is_ok());
    }

    #[test]
    fn test_disabled_when_limit_zero() {
        let b = make_breaker(0, 0, 1.0);
        assert!(b.check_and_reserve(999_999).is_ok());
    }

    #[test]
    fn test_concurrent() {
        let b = Arc::new(make_breaker(1_000_000, 0, 1.0));
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let b = Arc::clone(&b);
                thread::spawn(move || {
                    let mut ok = 0usize;
                    for _ in 0..1000 { if b.check_and_reserve(1).is_ok() { ok += 1; } }
                    ok
                })
            })
            .collect();
        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total, 10_000);
        assert_eq!(b.stats().request_used_bytes, 10_000);
    }

    #[test]
    fn test_error_encoding() {
        let err = CircuitBreakError { bytes_wanted: 1024, bytes_limit: 512, current_used: 256 };
        let s = err.to_encoded_string();
        assert!(s.starts_with("CB:1024:512:256:"));
        assert!(s.contains("native_request"));
    }
}
