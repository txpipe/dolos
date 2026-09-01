//! # Driver machinery for the Stelae protocol
//!
//! [`stelae`] is the protocol: framing, canonicalization, digests and the
//! naming rules. A *profile* — `dolos-snapshot` is this workspace's one — says
//! what a stele contains. Between them sits the work every publisher and every
//! restorer does whatever it is moving: sizing a volume before a run starts,
//! counting layers and records for whoever is watching, reading where a node
//! stands against a repository's newest stele, keying a layer by its kind and
//! scope.
//!
//! None of that is protocol and none of it is profile, so it lives here rather
//! than in either. No type here is a node's or a profile's — the boundary this
//! crate keeps is the same one `stelae` keeps, and the manifest states it.
//!
//! ## Module map
//!
//! - [`preflight`] — the free-space policy a publish and a restore share.
//! - [`reporting`] — the layer and record arithmetic behind
//!   [`stelae::progress`].
//! - [`digests`] — the codec for the `digests` layer kind. Its records are
//!   sha256 over immutable-database files, which is a Cardano shape described
//!   in Cardano words; the code depends on nothing but this crate and the
//!   protocol, which is why it sits here.
//! - [`profile`] — [`DriverProfile`], the little a lifecycle has to ask a
//!   profile that the protocol's own trait deliberately does not answer.
//! - [`predecessor`] — the publish a publish follows, and what it may carry
//!   forward from it.
//! - [`publish`] — the chained-publish lifecycle against a repository, behind
//!   the `oci` feature because that is where a repository lives.
//! - [`retry`] — the bounded patience a run spends on an external that fails in
//!   bursts.
//! - [`Standing`] — where a node stands against a repository's latest stele.
//! - [`scope_key`] — the pair that identifies one layer.

pub mod digests;
pub mod predecessor;
pub mod preflight;
pub mod profile;
#[cfg(feature = "oci")]
pub mod publish;
pub mod reporting;
pub mod retry;

pub use predecessor::{First, Predecessor};
pub use profile::DriverProfile;

/// Errors raised by the driver.
///
/// No variant carries a profile's or a node's types: a driver failure is about
/// a volume, a record's shape or a chain of sequences, and a profile wraps this
/// enum in its own rather than the other way round.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("stelae error: {0}")]
    Stelae(stelae::Error),

    /// A volume that cannot hold what the run is about to put on it, refused
    /// before the run starts.
    ///
    /// Raised only from a number that was actually measured against free space
    /// that was actually read — everything else warns and proceeds. One
    /// variant for both directions because it is one policy; see
    /// [`preflight`]. There is deliberately no flag that overrides it: a
    /// scratch directory pointed at a bigger volume is the escape hatch.
    #[error("not enough space: {0}")]
    NotEnoughSpace(String),

    #[error("malformed {kind} record: {reason}")]
    MalformedRecord { kind: &'static str, reason: String },

    #[error("{kind} records are out of order: {reason}")]
    OutOfOrder { kind: &'static str, reason: String },

    /// A field of the inscription a profile owns — `position` or a layer's
    /// `scope` — is not a shape that canonicalizes.
    #[error("the inscription's {field} is not the shape this profile writes: {reason}")]
    MalformedInscription { field: String, reason: String },

    /// A predecessor describing a different dataset than the one being
    /// published, as [`DriverProfile::check_same_dataset`] judged it.
    ///
    /// The two identities are the profile's own — this crate carries the
    /// numbers and composes no sentence about what they name — so a profile
    /// that spells the refusal itself keeps the message it always had, the way
    /// every other shared refusal here does.
    #[error("this stele describes dataset {found}, but this node is configured for {expected}")]
    DatasetMismatch { expected: u64, found: u64 },

    /// A publish that would not extend the repository's chain. Raised by
    /// [`stelae::inscription::history_for`] and carried here unchanged.
    #[error(
        "this repository's latest stele is sequence {latest} and this publish is sequence \
         {publishing}: {reason}"
    )]
    HistoryBreak {
        latest: u64,
        publishing: u64,
        reason: String,
    },
}

/// Protocol refusals this crate also names keep their own variant rather than
/// arriving wrapped.
///
/// [`stelae::codec`] raises `MalformedRecord` and
/// [`stelae::inscription::history_for`] raises `HistoryBreak`; both were this
/// crate's errors before they moved down into the protocol, and both are
/// matched on by callers. Flattening keeps the variant a caller matches and the
/// message an operator reads exactly what they were, at the cost of a `match`
/// arm per shared refusal — which is the direction a move that is supposed to
/// change nothing observable should pay in.
impl From<stelae::Error> for Error {
    fn from(error: stelae::Error) -> Self {
        match error {
            stelae::Error::MalformedRecord { kind, reason } => {
                Self::MalformedRecord { kind, reason }
            }
            stelae::Error::HistoryBreak {
                latest,
                publishing,
                reason,
            } => Self::HistoryBreak {
                latest,
                publishing,
                reason,
            },
            other => Self::Stelae(other),
        }
    }
}

impl Error {
    pub(crate) fn out_of_order(kind: &'static str, reason: impl Into<String>) -> Self {
        Self::OutOfOrder {
            kind,
            reason: reason.into(),
        }
    }

    pub(crate) fn malformed_inscription(
        field: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self::MalformedInscription {
            field: field.into(),
            reason: reason.into(),
        }
    }
}

/// Where a node stands relative to the newest stele already published.
///
/// The comparison a publisher on a timer needs *before* anything is built, and
/// both halves of it are already in hand: the sequence a repository's latest
/// stele carries, and the sequence the node's cursor derived. Without it the
/// ordinary case — nothing has closed since last time — arrives as the
/// [`Error::HistoryBreak`] refusal a skipped sequence does, and a job on a
/// timer cannot tell the two apart.
///
/// A pure comparison over two numbers rather than a method on a transport, so
/// the cases can be checked without one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Standing {
    /// Nothing has been published; this stele would start the chain.
    Empty,
    /// The published chain has already reached this node. Not an error: a
    /// publisher whose node has not entered a new epoch has nothing to do.
    UpToDate { latest: u64 },
    /// The chain ends exactly one sequence back; this stele extends it.
    Next { latest: u64 },
    /// The node is further ahead than one sequence, so a publish would leave a
    /// gap. `distance` is how far — the number the refusal reports alongside
    /// both sequences, because "you skipped some" and "you skipped forty" are
    /// different incidents.
    Ahead { latest: u64, distance: u64 },
}

impl Standing {
    /// Read a node at `sequence` against a repository whose latest stele is
    /// `latest`.
    pub fn read(latest: Option<u64>, sequence: u64) -> Self {
        let Some(latest) = latest else {
            return Self::Empty;
        };

        match sequence.checked_sub(latest) {
            None | Some(0) => Self::UpToDate { latest },
            Some(1) => Self::Next { latest },
            Some(distance) => Self::Ahead { latest, distance },
        }
    }

    /// Whether a publish should go ahead.
    pub fn publishable(&self) -> bool {
        matches!(self, Self::Empty | Self::Next { .. })
    }
}

/// The pair that identifies one layer: its kind, and the canonical encoding of
/// its profile-owned scope.
///
/// Canonical rather than [`serde_json::Value`] equality, because two scopes are
/// one layer exactly when they are the same bytes inside the canonical
/// document — the only sense of "the same scope" the protocol has.
///
/// One function rather than three, and that is the point of it being here
/// instead of beside any one caller. Every table keyed this way is compared
/// against another table keyed this way: the predecessor's inheritable layers
/// against what a publish asks for, an interrupted publish's record against the
/// same, a reproduction's layers against the published ones. Three copies of
/// four lines would agree until one of them was corrected, and the failure that
/// follows is silent — a layer rebuilt instead of inherited, or a divergence
/// reported between two documents that say the same thing.
pub fn scope_key(kind: &str, scope: &serde_json::Value) -> Result<(String, String), Error> {
    let canonical = stelae::inscription::canonical_json(scope)?;

    let canonical = String::from_utf8(canonical)
        .map_err(|e| Error::malformed_inscription("layer scope", e.to_string()))?;

    Ok((kind.to_owned(), canonical))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The four readings of a repository a publisher on a timer meets, and the
    /// one that used to arrive as a refusal.
    #[test]
    fn a_repository_is_read_as_empty_current_next_or_ahead() {
        assert_eq!(Standing::read(None, 500), Standing::Empty);

        // The ordinary case for a job that runs more often than epochs close.
        assert_eq!(
            Standing::read(Some(500), 500),
            Standing::UpToDate { latest: 500 }
        );

        // And a node genuinely behind the repository, which is up to date in
        // the only sense this comparison is for: there is nothing to publish.
        assert_eq!(
            Standing::read(Some(501), 500),
            Standing::UpToDate { latest: 501 }
        );

        assert_eq!(
            Standing::read(Some(499), 500),
            Standing::Next { latest: 499 }
        );

        assert_eq!(
            Standing::read(Some(497), 500),
            Standing::Ahead {
                latest: 497,
                distance: 3
            }
        );

        for standing in [Standing::Empty, Standing::Next { latest: 1 }] {
            assert!(standing.publishable(), "{standing:?}");
        }

        for standing in [
            Standing::UpToDate { latest: 1 },
            Standing::Ahead {
                latest: 1,
                distance: 2,
            },
        ] {
            assert!(!standing.publishable(), "{standing:?}");
        }
    }
}
