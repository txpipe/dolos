//! Bounded patience for an external that fails in bursts.
//!
//! A registry round trip and an aggregator fetch fail the same way — briefly,
//! and for reasons neither end can classify — so the policy that absorbs them
//! is one policy rather than one per caller.

use std::time::Duration;

/// Attempts a transient-prone external gets before its failure is fatal.
///
/// Bounded on purpose. The preprod G1 backfill measured why the retry exists —
/// four container exits between 06:39Z and 08:53Z on 2026-08-23, each one
/// paying a store restore, a window re-download and the in-flight epoch's
/// re-replay for what a half-minute wait would have absorbed — and the same
/// measurement is why it is not open-ended: a misconfigured aggregator or a
/// repository the credentials cannot read has to keep failing, and keep
/// failing while whoever launched the run is still watching.
pub const RETRY_ATTEMPTS: u32 = 4;

/// The first wait between attempts; each later one doubles it. Three waits of
/// 5s, 10s and 20s put the ceiling at 35 seconds of patience.
pub const RETRY_BASE_DELAY: Duration = Duration::from_secs(5);

/// Sleep, unless a shutdown is requested first. `false` means it was.
///
/// Sliced rather than slept in one call so a signal that arrives during a
/// backoff is honoured at the next slice instead of at the end of the wait —
/// the driver's whole shutdown budget is a container's SIGTERM grace period,
/// which a 20-second sleep would eat.
fn sleep_unless_aborted(delay: Duration, abort: &dyn Fn() -> bool) -> bool {
    const SLICE: Duration = Duration::from_millis(250);

    let mut left = delay;

    while !left.is_zero() {
        if abort() {
            return false;
        }

        let slice = left.min(SLICE);
        std::thread::sleep(slice);
        left -= slice;
    }

    !abort()
}

/// Run `op`, retrying a failure with exponential backoff, then let it be fatal.
///
/// The last attempt's error is returned untouched, so every caller keeps the
/// diagnostic it had before the retry was wrapped around it — the retry moves
/// where the fatal path is reached, never what it says. Nothing here decides a
/// failure is transient: the classification the alternative would need does not
/// exist at these seams (an aggregator's errors arrive as opaque strings), and
/// guessing it wrong reinstates exactly the fatal exits this is here to absorb.
/// What bounds the patience is [`RETRY_ATTEMPTS`], not a judgement about the
/// error.
///
/// `abort` is polled between and during the waits, so a shutdown ends the run
/// on the failure in hand rather than after the remaining backoff. Callers with
/// no shutdown to observe pass `&|| false`.
///
/// Only for operations that are safe to simply run again: reads, and downloads
/// whose destination is rewritten from the same arguments.
pub fn transient<T, E, F>(what: &str, abort: &dyn Fn() -> bool, op: F) -> Result<T, E>
where
    F: FnMut() -> Result<T, E>,
    E: std::fmt::Display,
{
    bounded(what, RETRY_ATTEMPTS, RETRY_BASE_DELAY, abort, op)
}

/// [`transient`] with its two constants spelled out, so a caller under test can
/// exercise the loop without waiting out a real backoff.
pub fn bounded<T, E, F>(
    what: &str,
    attempts: u32,
    base_delay: Duration,
    abort: &dyn Fn() -> bool,
    mut op: F,
) -> Result<T, E>
where
    F: FnMut() -> Result<T, E>,
    E: std::fmt::Display,
{
    let mut delay = base_delay;

    for attempt in 1..attempts {
        match op() {
            Ok(value) => return Ok(value),
            Err(err) => {
                if abort() {
                    return Err(err);
                }

                tracing::warn!(
                    what,
                    attempt,
                    remaining = attempts - attempt,
                    backoff_secs = delay.as_secs(),
                    error = %err,
                    "transient failure; retrying",
                );

                if !sleep_unless_aborted(delay, abort) {
                    return Err(err);
                }

                delay = delay.saturating_mul(2);
            }
        }
    }

    op()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The retry loop, with the real backoff replaced by none of it.
    fn retried<T, E: std::fmt::Display>(
        abort: &dyn Fn() -> bool,
        op: impl FnMut() -> Result<T, E>,
    ) -> Result<T, E> {
        bounded("a test", RETRY_ATTEMPTS, Duration::ZERO, abort, op)
    }

    #[test]
    fn a_call_that_succeeds_is_made_once() {
        let mut calls = 0;

        let result: Result<u8, String> = retried(&|| false, || {
            calls += 1;
            Ok(7)
        });

        assert_eq!(result.unwrap(), 7);
        assert_eq!(calls, 1, "a success must not be retried");
    }

    #[test]
    fn a_transient_failure_is_absorbed() {
        let mut calls = 0;

        let result: Result<u8, String> = retried(&|| false, || {
            calls += 1;

            if calls < 3 {
                Err("the aggregator hung up".to_owned())
            } else {
                Ok(7)
            }
        });

        assert_eq!(result.unwrap(), 7);
        assert_eq!(calls, 3, "the loop stops at the first success");
    }

    #[test]
    fn patience_is_bounded_and_the_last_error_is_the_one_raised() {
        let mut calls = 0;

        let result: Result<u8, String> = retried(&|| false, || {
            calls += 1;
            Err(format!("attempt {calls} failed"))
        });

        assert_eq!(
            calls, RETRY_ATTEMPTS as usize,
            "a persistent failure must still reach the fatal path",
        );

        assert_eq!(
            result.unwrap_err(),
            format!("attempt {RETRY_ATTEMPTS} failed"),
            "the caller keeps the diagnostic the final attempt produced",
        );
    }

    #[test]
    fn a_shutdown_ends_the_run_on_the_failure_in_hand() {
        let mut calls = 0;

        let result: Result<u8, String> = retried(&|| true, || {
            calls += 1;
            Err("interrupted".to_owned())
        });

        assert_eq!(calls, 1, "a requested shutdown is not waited out");
        assert_eq!(result.unwrap_err(), "interrupted");
    }

    #[test]
    fn a_shutdown_during_a_backoff_cuts_the_wait_short() {
        assert!(!sleep_unless_aborted(Duration::from_secs(60), &|| true));
        assert!(sleep_unless_aborted(Duration::ZERO, &|| false));
    }
}
