//! Shared retry timing for external boundaries.

use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/// Exponential retry delay with bounded jitter.
#[derive(Clone, Debug)]
pub(crate) struct Backoff {
    attempt: u32,
    base: Duration,
    cap: Duration,
}

impl Backoff {
    /// Start a backoff sequence.
    #[must_use]
    pub(crate) const fn new(base: Duration, cap: Duration) -> Self {
        Self {
            attempt: 0,
            base,
            cap,
        }
    }

    /// Return the next delay, honoring a provider-supplied minimum.
    pub(crate) fn next(&mut self, retry_after: Option<Duration>) -> Duration {
        let multiplier = 1_u32.checked_shl(self.attempt.min(31)).unwrap_or(u32::MAX);
        let exponential = self.base.saturating_mul(multiplier).min(self.cap);
        self.attempt = self.attempt.saturating_add(1);
        let floor = retry_after.unwrap_or(Duration::ZERO).max(exponential);
        jitter(floor, self.cap.max(floor))
    }
}

fn jitter(delay: Duration, cap: Duration) -> Duration {
    if delay.is_zero() {
        return delay;
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    // Add 0-25% jitter. Provider Retry-After remains a strict lower bound.
    let extra = delay / 4 * (nanos % 1_000) / 1_000;
    delay.saturating_add(extra).min(cap.max(delay))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_backoff_starts_at_one_second_and_caps() {
        let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_mins(2));
        for expected in [1, 2, 4, 8, 16, 32, 64, 120, 120] {
            let delay = backoff.next(None);
            assert!(delay >= Duration::from_secs(expected));
            assert!(delay <= Duration::from_millis(expected * 1_250));
        }
    }

    #[test]
    fn retry_after_is_a_strict_minimum() {
        let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_mins(2));
        assert!(backoff.next(Some(Duration::from_secs(90))) >= Duration::from_secs(90));
    }
}
