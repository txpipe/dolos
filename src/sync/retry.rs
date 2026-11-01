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
use pallas::network::miniprotocols::{blockfetch, chainsync, handshake, txsubmission};
use pallas::network::multiplexer;
use tracing::{error, warn};

use super::{SyncProgress, UpstreamFailure};

/// Why one attempt at the upstream peer failed.
#[derive(Debug, Clone)]
pub(super) struct Failure {
    cause: String,
    /// Whether the peer refused the handshake, as a peer of another network
    /// does.
    refused: bool,
}

impl Failure {
    pub fn new(what: &str, error: &(dyn StdError + 'static)) -> Self {
        Self::message(format!("{what}: {}", with_sources(error)))
    }

    pub fn message(cause: String) -> Self {
        Self {
            cause,
            refused: false,
        }
    }

    /// A connection that failed, in its handshake or before it. Only a
    /// refusal is the peer's answer; a connection that drops during the
    /// handshake fails like any other.
    pub fn connecting(error: &facades::Error) -> Self {
        let (detail, refused) = match error {
            facades::Error::HandshakeProtocol(inner) => {
                (format!("{error}: {}", with_sources(inner)), false)
            }
            facades::Error::IncompatibleVersion => (error.to_string(), true),
            error => (with_sources(error), false),
        };

        Self {
            cause: format!("connecting: {detail}"),
            refused,
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.cause)
    }
}

/// An error and every cause under it, the multiplexer's included, which
/// pallas's mini-protocol errors hold without naming it as their source.
fn with_sources(error: &(dyn StdError + 'static)) -> String {
    let mut text = error.to_string();
    let mut last = error;

    while let Some(cause) = last.source() {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        last = cause;
    }

    if let Some(plexer) = plexer_error(last) {
        text.push_str(": ");
        text.push_str(&describe_plexer(plexer));
    }

    text
}

fn plexer_error<'a>(error: &'a (dyn StdError + 'static)) -> Option<&'a multiplexer::Error> {
    if let Some(chainsync::ClientError::Plexer(inner)) = error.downcast_ref() {
        return Some(inner);
    }

    if let Some(blockfetch::ClientError::Plexer(inner)) = error.downcast_ref() {
        return Some(inner);
    }

    if let Some(txsubmission::Error::Plexer(inner)) = error.downcast_ref() {
        return Some(inner);
    }

    if let Some(handshake::Error::Plexer(inner)) = error.downcast_ref() {
        return Some(inner);
    }

    None
}

/// What the multiplexer says, with the detail its own message leaves out.
fn describe_plexer(error: &multiplexer::Error) -> String {
    match error {
        multiplexer::Error::BearerIo(io) => format!("the connection failed: {io}"),
        multiplexer::Error::EmptyBearer | multiplexer::Error::AgentDequeue => {
            "the connection closed".into()
        }
        multiplexer::Error::Decoding(detail) => format!("a message did not decode: {detail}"),
        multiplexer::Error::Encoding(detail) => format!("a message did not encode: {detail}"),
        error => error.to_string(),
    }
}

/// A stage's failed attempts in a row at the upstream peer. Attempt `n` in a
/// row waits `backoff(n)`, whatever failed; the one past `max_retries` gives
/// up, recording why. A pull session that delivers between two attempts
/// starts the count again: the peer serves the sync, and giving up on it
/// would not help.
pub(super) struct Attempts {
    stage: &'static str,
    peer_address: String,
    retry: RetryConfig,
    progress: Arc<SyncProgress>,
    failures: Cell<usize>,
    /// The pull stage's deliveries when the last attempt failed.
    deliveries: Cell<u64>,
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
            deliveries: Cell::new(0),
        }
    }

    /// The peer delivered something: the count starts again.
    pub fn reset(&self) {
        self.failures.set(0);
    }

    /// Count a failed attempt and wait its backoff; past the last retry,
    /// record why and end the stage.
    pub async fn failed(&self, failure: Failure) -> Result<(), WorkerError> {
        let deliveries = self.progress.deliveries();

        if self.deliveries.replace(deliveries) != deliveries {
            self.reset();
        }

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
            refused: failure.refused,
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
