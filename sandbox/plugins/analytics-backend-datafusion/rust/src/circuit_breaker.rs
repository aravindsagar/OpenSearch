/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Native circuit breaker for DataFusion query memory.
//!
//! Two-level check on every allocation:
//! - Level 1 (request): `(request_used + N) × overhead > request_limit`
//! - Level 2 (node): `cached_jemalloc_total + N > node_limit`
//!
//! A background Tokio task refreshes the jemalloc cache every second.

use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use log::{debug, warn};
use once_cell::sync::OnceCell;

static BREAKER: OnceCell<NativeCircuitBreaker> = OnceCell::new();

/// Initialize the breaker. The jemalloc refresh timer is managed separately
/// by `native_mem_stats` (started during runtime init).
pub fn init(request_limit: usize, node_limit: usize, overhead_millionths: u64) -> Result<(), &'static str> {
    debug!("Initializing NativeCircuitBreaker: request_limit={}B, node_limit={}B, overhead={:.3}",
        request_limit, node_limit, overhead_millionths as f64 / 1_000_000.0);

    BREAKER.set(NativeCircuitBreaker {
        request_limit: AtomicUsize::new(request_limit),
        node_limit: AtomicUsize::new(node_limit),
        request_used_bytes: AtomicUsize::new(0),
        overhead_millionths: AtomicU64::new(overhead_millionths),
        child_tripped: AtomicU64::new(0),
        node_tripped: AtomicU64::new(0),
    }).map_err(|_| "circuit breaker already initialized")?;

    Ok(())
}

/// Get the breaker instance.
pub fn get() -> Option<&'static NativeCircuitBreaker> {
    BREAKER.get()
}

// ---------------------------------------------------------------------------
// NativeCircuitBreaker
// ---------------------------------------------------------------------------

pub struct NativeCircuitBreaker {
    request_limit: AtomicUsize,
    node_limit: AtomicUsize,
    request_used_bytes: AtomicUsize,
    overhead_millionths: AtomicU64,
    child_tripped: AtomicU64,
    node_tripped: AtomicU64,
}

impl NativeCircuitBreaker {
    /// Check both levels and reserve bytes.
    pub fn check_and_reserve(&self, bytes: usize) -> Result<(), CircuitBreakError> {
        // Level 1: request check
        self.check_request(bytes)?;

        // Level 2: node check (cached jemalloc)
        if let Err(e) = self.check_node(bytes) {
            self.request_used_bytes.fetch_sub(bytes, Ordering::Relaxed);
            return Err(e);
        }

        Ok(())
    }

    /// Release bytes (called on shrink).
    pub fn release(&self, bytes: usize) {
        self.request_used_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Level 1: (request_used + bytes) × overhead > request_limit?
    fn check_request(&self, bytes: usize) -> Result<(), CircuitBreakError> {
        let limit = self.request_limit.load(Ordering::Relaxed);
        if limit == 0 { return Ok(()); }
        let overhead = self.overhead();

        let result = self.request_used_bytes.fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current| {
            let with_overhead = ((current + bytes) as f64 * overhead) as usize;
            if with_overhead > limit { None } else { Some(current + bytes) }
        });

        match result {
            Ok(_) => Ok(()),
            Err(current) => {
                self.child_tripped.fetch_add(1, Ordering::Relaxed);
                warn!("CB request tripped: wanted {}B, current {}B, limit {}B", bytes, current, limit);
                Err(CircuitBreakError { bytes_wanted: bytes, bytes_limit: limit, current_used: current })
            }
        }
    }

    /// Level 2: cached_jemalloc_total + bytes > node_limit?
    fn check_node(&self, bytes: usize) -> Result<(), CircuitBreakError> {
        let limit = self.node_limit.load(Ordering::Relaxed);
        if limit == 0 { return Ok(()); }
        let total = crate::native_mem_stats::allocated_bytes();
        if total + bytes > limit {
            self.node_tripped.fetch_add(1, Ordering::Relaxed);
            warn!("CB node tripped: total {}B + wanted {}B > limit {}B", total, bytes, limit);
            return Err(CircuitBreakError { bytes_wanted: bytes, bytes_limit: limit, current_used: total });
        }
        Ok(())
    }

    // --- Stats (polled by Java via FFM downcall) ---

    pub fn request_used_bytes(&self) -> usize {
        self.request_used_bytes.load(Ordering::Relaxed)
    }

    pub fn total_used_bytes(&self) -> usize {
        crate::native_mem_stats::allocated_bytes()
    }

    pub fn child_tripped(&self) -> u64 {
        self.child_tripped.load(Ordering::Relaxed)
    }

    pub fn node_tripped(&self) -> u64 {
        self.node_tripped.load(Ordering::Relaxed)
    }

    // --- Dynamic updates ---

    pub fn set_request_limit(&self, limit: usize) {
        self.request_limit.store(limit, Ordering::Release);
    }

    pub fn set_node_limit(&self, limit: usize) {
        self.node_limit.store(limit, Ordering::Release);
    }

    pub fn set_overhead(&self, overhead_millionths: u64) {
        self.overhead_millionths.store(overhead_millionths, Ordering::Release);
    }

    fn overhead(&self) -> f64 {
        self.overhead_millionths.load(Ordering::Relaxed) as f64 / 1_000_000.0
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
    /// Encode for FFM error propagation. Java splits on ":" with limit 4.
    pub fn to_encoded_string(&self) -> String {
        format!("CB:{}:{}:{}:{}", self.bytes_wanted, self.bytes_limit, self.current_used, self)
    }
}

impl fmt::Display for CircuitBreakError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[native_request] Data too large, data would be [{}/{}], which is larger than the limit of [{}/{}]",
            self.current_used + self.bytes_wanted, human_bytes(self.current_used + self.bytes_wanted),
            self.bytes_limit, human_bytes(self.bytes_limit))
    }
}

fn human_bytes(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = 1024 * KB;
    const GB: usize = 1024 * MB;
    if bytes >= GB { format!("{:.1}gb", bytes as f64 / GB as f64) }
    else if bytes >= MB { format!("{:.1}mb", bytes as f64 / MB as f64) }
    else if bytes >= KB { format!("{:.1}kb", bytes as f64 / KB as f64) }
    else { format!("{}b", bytes) }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn make_breaker(request_limit: usize, node_limit: usize, overhead: f64) -> NativeCircuitBreaker {
        NativeCircuitBreaker {
            request_limit: AtomicUsize::new(request_limit),
            node_limit: AtomicUsize::new(node_limit),
            request_used_bytes: AtomicUsize::new(0),
            overhead_millionths: AtomicU64::new((overhead * 1_000_000.0) as u64),
            child_tripped: AtomicU64::new(0),
            node_tripped: AtomicU64::new(0),
        }
    }

    #[test]
    fn test_request_check_trips() {
        let b = make_breaker(1000, 0, 1.0);
        assert!(b.check_and_reserve(1000).is_ok());
        assert!(b.check_and_reserve(1).is_err());
        assert_eq!(b.child_tripped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_request_with_overhead() {
        let b = make_breaker(1000, 0, 2.0);
        assert!(b.check_and_reserve(400).is_ok()); // 400*2=800 < 1000
        assert!(b.check_and_reserve(200).is_err()); // (400+200)*2=1200 > 1000
    }

    #[test]
    fn test_node_check_trips() {
        let b = make_breaker(0, 500, 1.0); // request disabled, node at 500
        crate::native_mem_stats::set_allocated_bytes_for_test(400);
        assert!(b.check_and_reserve(50).is_ok()); // 400+50=450 < 500
        assert!(b.check_and_reserve(100).is_ok()); // 400+100=500, NOT > 500, passes
        assert!(b.check_and_reserve(101).is_err()); // 400+101=501 > 500, trips
        assert_eq!(b.node_tripped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_node_check_trips_correctly() {
        let b = make_breaker(0, 500, 1.0);
        crate::native_mem_stats::set_allocated_bytes_for_test(450);
        assert!(b.check_and_reserve(51).is_err()); // 450+51=501 > 500
        assert_eq!(b.node_tripped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_release() {
        let b = make_breaker(1000, 0, 1.0);
        assert!(b.check_and_reserve(900).is_ok());
        b.release(900);
        assert!(b.check_and_reserve(900).is_ok());
    }

    #[test]
    fn test_concurrent() {
        let b = Arc::new(make_breaker(1_000_000, 0, 1.0));
        let handles: Vec<_> = (0..10).map(|_| {
            let b = Arc::clone(&b);
            thread::spawn(move || {
                let mut ok = 0usize;
                for _ in 0..1000 { if b.check_and_reserve(1).is_ok() { ok += 1; } }
                ok
            })
        }).collect();
        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total, 10_000);
        assert_eq!(b.request_used_bytes.load(Ordering::Relaxed), 10_000);
    }

    #[test]
    fn test_error_encoding() {
        let err = CircuitBreakError { bytes_wanted: 1024, bytes_limit: 512, current_used: 256 };
        let s = err.to_encoded_string();
        assert!(s.starts_with("CB:1024:512:256:"));
        assert!(s.contains("native_request"));
    }
}
