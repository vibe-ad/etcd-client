//! Opt-in request-level failover, gated behind the `failover` feature.
//!
//! Mirrors etcd's Go `clientv3` retry semantics. This module holds the pure
//! decision logic (policy, error classification, backoff pacing). The retry
//! loop itself lives on [`crate::Client`] so it can reuse `refresh_token` for
//! re-authentication. Idempotent (`Repeatable`) RPCs retry on any `Unavailable`.
//! Mutating (`NonRepeatable`) RPCs retry only when the request provably never
//! reached a server, preserving write-at-most-once.

use crate::error::Error;
use std::error::Error as StdError;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tonic::{Code, Status};

/// Whether an RPC may be safely re-issued.
#[derive(Clone, Copy)]
pub(crate) enum RetryPolicy {
    /// Idempotent: safe to retry on any transient error.
    Repeatable,
    /// Mutating: retry only when the request provably never reached a server.
    NonRepeatable,
}

/// How to react to a failed attempt.
pub(crate) enum Decision {
    Retry,
    RefreshToken,
    Stop,
}

/// Failover tuning, derived from `ConnectOptions`. Cheap to clone.
#[derive(Clone)]
pub(crate) struct RetryConfig {
    /// Total attempts including the first. `<= 1` disables retry.
    pub(crate) max_attempts: u32,
    /// Base wait between retry rounds.
    backoff_wait: Duration,
    /// Jitter as a fraction of `backoff_wait` (e.g. 0.10 for +/-10%).
    jitter: f64,
    /// Live endpoint count, used to pace backoff by quorum. Shared with every
    /// clone so `add_endpoint` / `remove_endpoint` re-pace the whole client.
    endpoint_count: Arc<AtomicUsize>,
    /// Auto-reconnect a broken watch stream, resuming from the last revision.
    pub(crate) watch_reconnect: bool,
    /// Auto-reconnect a broken lease keep-alive stream.
    pub(crate) lease_reconnect: bool,
}

impl RetryConfig {
    pub(crate) fn new(
        max_attempts: u32,
        backoff_wait: Duration,
        jitter: f64,
        endpoint_count: usize,
        watch_reconnect: bool,
        lease_reconnect: bool,
    ) -> Self {
        Self {
            max_attempts,
            backoff_wait,
            jitter,
            endpoint_count: Arc::new(AtomicUsize::new(endpoint_count.max(1))),
            watch_reconnect,
            lease_reconnect,
        }
    }

    /// A config with retry effectively disabled (single attempt).
    pub(crate) fn disabled() -> Self {
        Self::new(1, Duration::from_millis(25), 0.10, 1, false, false)
    }

    /// Backoff before `attempt` (0-indexed). Mirrors etcd's
    /// `roundRobinQuorumBackoff`: only sleep once per quorum of attempts so
    /// retries sweep a quorum of endpoints quickly, then pause.
    pub(crate) fn backoff(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }
        let quorum = (self.endpoint_count.load(Ordering::Relaxed) / 2 + 1) as u32;
        if attempt % quorum == 0 {
            jittered(self.backoff_wait, self.jitter)
        } else {
            Duration::ZERO
        }
    }

    /// Tracks a successful `add_endpoint`, so backoff paces against the live
    /// endpoint count. `max_attempts` stays as derived at connect time.
    pub(crate) fn endpoint_added(&self) {
        self.endpoint_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Tracks a successful `remove_endpoint`, never dropping below one.
    pub(crate) fn endpoint_removed(&self) {
        let _ = self
            .endpoint_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n > 1).then(|| n - 1)
            });
    }

    /// Backoff for a stream reconnect attempt: exponential from `backoff_wait`,
    /// capped, so a long outage does not busy-loop the reconnect task.
    pub(crate) fn reconnect_backoff(&self, attempt: u32) -> Duration {
        let cap = Duration::from_secs(5);
        let shift = attempt.min(8);
        let grown = self.backoff_wait.saturating_mul(1u32 << shift);
        jittered(grown.min(cap), self.jitter)
    }
}

/// Decide how to react to a failed unary attempt.
pub(crate) fn classify(err: &Error, policy: RetryPolicy) -> Decision {
    let Error::GRpcStatus(status) = err else {
        return Decision::Stop;
    };
    // Auth-token errors do not share one gRPC code (an invalid token is
    // `Unauthenticated`, a stale auth revision or empty user name is
    // `InvalidArgument`), so match on the message like etcd's `shouldRefreshToken`
    // instead of gating on the code.
    if is_auth_token_error(status) {
        return Decision::RefreshToken;
    }
    match status.code() {
        Code::Unavailable => match policy {
            RetryPolicy::Repeatable => Decision::Retry,
            // Only retry a mutating RPC when we can prove it never reached a
            // server, otherwise a retry could apply the write twice.
            RetryPolicy::NonRepeatable if is_not_sent(status) => Decision::Retry,
            RetryPolicy::NonRepeatable => Decision::Stop,
        },
        // A per-attempt timeout or transport cancellation means this endpoint is
        // unresponsive (e.g. a black-holed node). Fail an idempotent RPC over to
        // another endpoint. A mutating RPC must stop: the write may have applied
        // before the deadline fired.
        Code::DeadlineExceeded | Code::Cancelled => match policy {
            RetryPolicy::Repeatable => Decision::Retry,
            RetryPolicy::NonRepeatable => Decision::Stop,
        },
        _ => Decision::Stop,
    }
}

/// etcd server messages that mean the auth token should be refreshed and the
/// call retried (see `rpctypes` in etcd).
fn is_auth_token_error(status: &Status) -> bool {
    let msg = status.message();
    msg.contains("invalid auth token")
        || msg.contains("revision of auth store is old")
        || msg.contains("user name is empty")
}

/// True when the error proves the request never reached a server, so retrying a
/// mutating RPC cannot double-apply it. Two signals, both strictly pre-send:
///   1. h2 `REFUSED_STREAM` (gRPC's "server never began the RPC").
///   2. A connection-establishment error (the stream was never opened).
///
/// Deliberately excludes mid-stream errors like `ConnectionReset` / `TimedOut`:
/// those can occur after the server applied the write, so a write must not be
/// retried on them. Matches etcd, which retries mutating RPCs only when no
/// connection was available.
fn is_not_sent(status: &Status) -> bool {
    let mut source: Option<&(dyn StdError + 'static)> = Some(status);
    while let Some(err) = source {
        if let Some(h2) = err.downcast_ref::<h2::Error>() {
            if h2.reason() == Some(h2::Reason::REFUSED_STREAM) {
                return true;
            }
        }
        if let Some(io) = err.downcast_ref::<std::io::Error>() {
            use std::io::ErrorKind::*;
            if matches!(
                io.kind(),
                ConnectionRefused | HostUnreachable | NetworkUnreachable | AddrNotAvailable
            ) {
                return true;
            }
        }
        source = err.source();
    }
    false
}

/// Apply +/- `fraction` jitter to `base`. Uses `RandomState` for a seed so we
/// avoid pulling in an rng crate. Only called on the cold retry path.
fn jittered(base: Duration, fraction: f64) -> Duration {
    if fraction <= 0.0 {
        return base;
    }
    use std::hash::{BuildHasher, Hasher};
    let r = std::hash::RandomState::new().build_hasher().finish();
    let unit = (r as f64 / u64::MAX as f64) * 2.0 - 1.0; // [-1, 1)
    base.mul_f64((1.0 + fraction * unit).max(0.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err(code: Code, msg: &str) -> Error {
        Error::GRpcStatus(Status::new(code, msg))
    }

    #[test]
    fn repeatable_retries_unavailable() {
        assert!(matches!(
            classify(&err(Code::Unavailable, "x"), RetryPolicy::Repeatable),
            Decision::Retry
        ));
    }

    #[test]
    fn nonrepeatable_stops_on_bare_unavailable() {
        assert!(matches!(
            classify(&err(Code::Unavailable, "x"), RetryPolicy::NonRepeatable),
            Decision::Stop
        ));
    }

    #[test]
    fn auth_token_error_refreshes() {
        assert!(matches!(
            classify(
                &err(Code::Unauthenticated, "etcdserver: invalid auth token"),
                RetryPolicy::Repeatable
            ),
            Decision::RefreshToken
        ));
    }

    #[test]
    fn other_unauthenticated_stops() {
        assert!(matches!(
            classify(
                &err(Code::Unauthenticated, "permission denied"),
                RetryPolicy::Repeatable
            ),
            Decision::Stop
        ));
    }

    #[test]
    fn auth_token_error_refreshes_regardless_of_code() {
        // Stale auth revision and empty user name arrive as InvalidArgument, not
        // Unauthenticated, yet both must still trigger a token refresh.
        for msg in [
            "etcdserver: revision of auth store is old",
            "etcdserver: user name is empty",
        ] {
            assert!(matches!(
                classify(&err(Code::InvalidArgument, msg), RetryPolicy::NonRepeatable),
                Decision::RefreshToken
            ));
        }
    }

    #[test]
    fn timeout_fails_over_only_when_repeatable() {
        for code in [Code::DeadlineExceeded, Code::Cancelled] {
            assert!(matches!(
                classify(&err(code, "x"), RetryPolicy::Repeatable),
                Decision::Retry
            ));
            assert!(matches!(
                classify(&err(code, "x"), RetryPolicy::NonRepeatable),
                Decision::Stop
            ));
        }
    }

    #[test]
    fn non_transient_and_non_grpc_stop() {
        for code in [Code::InvalidArgument, Code::NotFound, Code::AlreadyExists] {
            assert!(matches!(
                classify(&err(code, "x"), RetryPolicy::Repeatable),
                Decision::Stop
            ));
        }
        assert!(matches!(
            classify(&Error::EndpointsNotManaged, RetryPolicy::Repeatable),
            Decision::Stop
        ));
    }

    #[test]
    fn refused_stream_is_not_sent() {
        let h2_err: h2::Error = h2::Reason::REFUSED_STREAM.into();
        let st = Status::from_error(Box::new(h2_err));
        assert!(is_not_sent(&st));
    }

    #[test]
    fn connect_refused_is_proof_of_not_sent_but_reset_is_not() {
        let refused = Status::from_error(Box::new(std::io::Error::from(
            std::io::ErrorKind::ConnectionRefused,
        )));
        assert!(is_not_sent(&refused));
        let reset = Status::from_error(Box::new(std::io::Error::from(
            std::io::ErrorKind::ConnectionReset,
        )));
        assert!(!is_not_sent(&reset));
    }

    #[test]
    fn backoff_paces_by_quorum() {
        let cfg = RetryConfig::new(10, Duration::from_millis(25), 0.0, 3, true, true);
        assert_eq!(cfg.backoff(0), Duration::ZERO);
        assert_eq!(cfg.backoff(1), Duration::ZERO);
        assert_eq!(cfg.backoff(2), Duration::from_millis(25));
        assert_eq!(cfg.backoff(3), Duration::ZERO);
    }

    #[test]
    fn single_endpoint_backs_off_every_retry() {
        // quorum is 1, so every attempt past the first pauses.
        let cfg = RetryConfig::new(5, Duration::from_millis(25), 0.0, 1, false, false);
        assert_eq!(cfg.backoff(0), Duration::ZERO);
        assert_eq!(cfg.backoff(1), Duration::from_millis(25));
        assert_eq!(cfg.backoff(2), Duration::from_millis(25));
    }

    #[test]
    fn reconnect_backoff_grows_then_caps() {
        let cfg = RetryConfig::new(10, Duration::from_millis(25), 0.0, 1, true, true);
        assert_eq!(cfg.reconnect_backoff(0), Duration::from_millis(25));
        assert_eq!(cfg.reconnect_backoff(1), Duration::from_millis(50));
        assert_eq!(cfg.reconnect_backoff(2), Duration::from_millis(100));
        // The shift saturates at 8 and the result is capped at 5s.
        assert_eq!(cfg.reconnect_backoff(8), Duration::from_secs(5));
        assert_eq!(cfg.reconnect_backoff(100), Duration::from_secs(5));
    }

    #[test]
    fn backoff_tracks_live_endpoint_count() {
        let cfg = RetryConfig::new(10, Duration::from_millis(25), 0.0, 1, false, false);
        // Quorum of 1: every retry pauses.
        assert_eq!(cfg.backoff(1), Duration::from_millis(25));
        cfg.endpoint_added();
        cfg.endpoint_added();
        // Quorum of 2: the odd attempts sweep, the even ones pause.
        assert_eq!(cfg.backoff(1), Duration::ZERO);
        assert_eq!(cfg.backoff(2), Duration::from_millis(25));
        cfg.endpoint_removed();
        cfg.endpoint_removed();
        // Clamped at one, so the pacing is back to pausing on every retry.
        cfg.endpoint_removed();
        assert_eq!(cfg.backoff(1), Duration::from_millis(25));
    }

    #[test]
    fn endpoint_count_is_shared_across_clones() {
        let cfg = RetryConfig::new(10, Duration::from_millis(25), 0.0, 1, false, false);
        let clone = cfg.clone();
        cfg.endpoint_added();
        cfg.endpoint_added();
        assert_eq!(clone.backoff(1), Duration::ZERO);
    }

    #[test]
    fn disabled_config_is_single_attempt() {
        assert_eq!(RetryConfig::disabled().max_attempts, 1);
    }
}
