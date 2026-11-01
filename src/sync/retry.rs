//! Retries toward the upstream peer. The pull and submit stages count their
//! failed attempts in a row themselves: gasket neither counts nor backs off a
//! restarted session, and restarts its own backoff after every teardown.

use std::cell::Cell;
use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use dolos_core::config::RetryConfig;
use gasket::framework::WorkerError;
use pallas::network::facades;
use tracing::{error, warn};

use super::{SyncProgress, UpstreamFailure};

/// Why one attempt at the upstream peer failed.
#[derive(Debug, Clone)]
pub(super) struct Failure {
    cause: String,
    /// Whether it failed in the handshake, which a peer of another network
    /// refuses.
    handshake: bool,
}

impl Failure {
    pub fn new(what: &str, error: &(dyn StdError + 'static)) -> Self {
        Self::message(format!("{what}: {}", with_sources(error)))
    }

    pub fn message(cause: String) -> Self {
        Self {
            cause,
            handshake: false,
        }
    }

    /// A connection that failed, in its handshake or before it.
    pub fn connecting(error: &facades::Error) -> Self {
        let (detail, handshake) = match error {
            facades::Error::HandshakeProtocol(inner) => {
                (format!("{error}: {}", with_sources(inner)), true)
            }
            facades::Error::IncompatibleVersion => (error.to_string(), true),
            error => (with_sources(error), false),
        };

        Self {
            cause: format!("connecting: {detail}"),
            handshake,
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.cause)
    }
}

/// An error and every cause under it.
fn with_sources(error: &(dyn StdError + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();

    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }

    text
}

/// A stage's failed attempts in a row at the upstream peer. Attempt `n` in a
/// row waits `backoff(n)`, whatever failed; the one past `max_retries` gives
/// up, recording why.
pub(super) struct Attempts {
    stage: &'static str,
    peer_address: String,
    retry: RetryConfig,
    progress: Arc<SyncProgress>,
    failures: Cell<usize>,
}

impl Attempts {
    pub fn new(
        stage: &'static str,
        peer_address: String,
        retry: RetryConfig,
        progress: Arc<SyncProgress>,
    ) -> Self {
        Self {
            stage,
            peer_address,
            retry,
            progress,
            failures: Cell::new(0),
        }
    }

    /// The peer delivered something: the count starts again.
    pub fn reset(&self) {
        self.failures.set(0);
    }

    /// Count a failed attempt and wait its backoff; past the last retry,
    /// record why and end the stage.
    pub async fn failed(&self, failure: Failure) -> Result<(), WorkerError> {
        let failures = self.failures.get() + 1;
        self.failures.set(failures);

        let max_retries = self.retry.max_retries;

        if failures <= max_retries {
            let backoff = backoff(&self.retry, failures);

            warn!(
                stage = self.stage,
                failures,
                max_retries,
                ?backoff,
                cause = %failure,
                "retrying the upstream peer"
            );

            tokio::time::sleep(backoff).await;
            return Ok(());
        }

        error!(stage = self.stage, failures, cause = %failure, "giving up on the upstream peer");

        let attempts = if failures == 1 {
            String::new()
        } else {
            format!(" after {failures} failed attempts in a row; the last")
        };

        self.progress.record_upstream_failure(UpstreamFailure {
            message: format!(
                "the {} stage gave up on the upstream peer {}{attempts}: {failure}",
                self.stage, self.peer_address
            ),
            handshake: failure.handshake,
        });

        Err(WorkerError::Panic)
    }
}

/// Retry `n` waits `backoff_unit_seconds × backoff_factor^n`, at most
/// `max_backoff_seconds`.
pub(super) fn backoff(retry: &RetryConfig, n: usize) -> Duration {
    let n = u32::try_from(n).unwrap_or(u32::MAX);
    let units = u64::from(retry.backoff_factor).saturating_pow(n);
    let backoff = retry.backoff_unit_seconds.saturating_mul(units);

    Duration::from_secs(backoff.min(retry.max_backoff_seconds))
}
