//! What the Ouroboros accept loops do after a server facade fails to accept.

use std::{io::ErrorKind, time::Duration};

use pallas::network::facades;

/// How long an accept loop waits after a listener-level failure before it
/// accepts again, so a listener that keeps failing (out of file descriptors,
/// say) is not polled in a busy loop.
pub(crate) const LISTENER_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Which side of an accept failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AcceptFailure {
    /// One connection failed: a missed deadline, a failed handshake or a peer
    /// that went away. The next connection can be accepted at once.
    Connection,
    /// The listener could not accept, or the cause is not recognised. Accepting
    /// again at once would likely fail the same way.
    Listener,
}

impl AcceptFailure {
    /// Classify the error a server facade's `accept_with_timeout` returned.
    pub(crate) fn of(error: &facades::Error) -> Self {
        // Only the bearer's own accept and socket setup return `ConnectFailure`
        // with something other than the deadline's `TimedOut`.
        let facades::Error::ConnectFailure(io) = error else {
            return Self::Connection;
        };

        match io.kind() {
            ErrorKind::TimedOut
            | ErrorKind::ConnectionAborted
            | ErrorKind::ConnectionReset
            | ErrorKind::BrokenPipe
            | ErrorKind::NotConnected
            | ErrorKind::InvalidInput
            | ErrorKind::Interrupted => Self::Connection,
            _ => Self::Listener,
        }
    }

    /// Wait before the next accept when the listener failed; return at once
    /// otherwise.
    pub(crate) async fn settle(self) {
        if self == Self::Listener {
            tokio::time::sleep(LISTENER_RETRY_DELAY).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use pallas::network::miniprotocols::handshake;

    use super::*;

    fn io_failure(error: io::Error) -> AcceptFailure {
        AcceptFailure::of(&facades::Error::ConnectFailure(error))
    }

    #[test]
    fn a_missed_handshake_deadline_is_a_connection_failure() {
        let error = io::Error::new(ErrorKind::TimedOut, "handshake not completed");
        assert_eq!(io_failure(error), AcceptFailure::Connection);
    }

    #[test]
    fn a_failed_or_refused_handshake_is_a_connection_failure() {
        let protocol = facades::Error::HandshakeProtocol(handshake::Error::InvalidInbound);
        assert_eq!(AcceptFailure::of(&protocol), AcceptFailure::Connection);

        let version = facades::Error::IncompatibleVersion;
        assert_eq!(AcceptFailure::of(&version), AcceptFailure::Connection);
    }

    #[test]
    fn a_peer_gone_before_setup_is_a_connection_failure() {
        for kind in [
            ErrorKind::ConnectionAborted,
            ErrorKind::ConnectionReset,
            ErrorKind::InvalidInput,
        ] {
            assert_eq!(io_failure(kind.into()), AcceptFailure::Connection, "{kind}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn running_out_of_descriptors_or_buffers_is_a_listener_failure() {
        use nix::errno::Errno;

        for errno in [Errno::EMFILE, Errno::ENFILE, Errno::ENOBUFS, Errno::ENOMEM] {
            let error = io::Error::from_raw_os_error(errno as i32);
            assert_eq!(io_failure(error), AcceptFailure::Listener, "{errno}");
        }
    }

    #[test]
    fn an_unrecognised_accept_error_is_a_listener_failure() {
        let error = io::Error::other("unexpected");
        assert_eq!(io_failure(error), AcceptFailure::Listener);
    }

    #[tokio::test(start_paused = true)]
    async fn only_a_listener_failure_waits_before_the_next_accept() {
        let start = tokio::time::Instant::now();
        AcceptFailure::Connection.settle().await;
        assert_eq!(start.elapsed(), Duration::ZERO);

        AcceptFailure::Listener.settle().await;
        assert_eq!(start.elapsed(), LISTENER_RETRY_DELAY);
    }
}
