//! The contracts this release reads: the `compatibility` a configuration
//! declares, and the identity a persistent store records when it is created.

use std::{fmt::Display, path::PathBuf};

use crate::config::{Compatibility, MIGRATION_GUIDE_URL};

/// The `compatibility` values a configuration may declare, the current one
/// first. A persistent store records the current one when it is created, and
/// this release opens only stores that record one of them.
pub const SUPPORTED_CONTRACTS: &[&str] = &["v2-alpha.1"];

/// Whether this release opens a store that records `identity`.
pub fn reads_store_identity(identity: &str) -> bool {
    SUPPORTED_CONTRACTS.contains(&identity)
}

/// A persistent store this release refuses to open: one holding data with no
/// identity, written before stores recorded one, or one created under a
/// contract this release does not read. No configuration changes the answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncompatibleStore {
    /// Which store: `state` or `archive`.
    pub store: &'static str,
    pub path: PathBuf,
    /// The identity the store records; `None` for a store holding data
    /// without one.
    pub found: Option<String>,
}

impl Display for IncompatibleStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the {} store at {} ", self.store, self.path.display())?;

        match &self.found {
            Some(found) => write!(f, "records the compatibility identity `{found}`")?,
            None => f.write_str("holds data but records no compatibility identity")?,
        }

        write!(
            f,
            "; this dolos opens stores that record `{}`. Restore it from a stele (`dolos \
             bootstrap stelae --force`), or replay an ImmutableDB over it (`dolos bootstrap \
             immutabledb --force`); see {MIGRATION_GUIDE_URL}",
            Compatibility::current()
        )
    }
}

impl std::error::Error for IncompatibleStore {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_refusal_names_what_it_found_and_the_remedy() {
        for found in [None, Some("v1".to_string())] {
            let message = IncompatibleStore {
                store: "state",
                path: PathBuf::from("data/state"),
                found: found.clone(),
            }
            .to_string();

            assert!(message.contains("data/state"), "{message}");
            assert!(message.contains(SUPPORTED_CONTRACTS[0]), "{message}");
            assert!(
                message.contains("dolos bootstrap immutabledb --force"),
                "{message}"
            );
            assert!(
                message.contains("dolos bootstrap stelae --force"),
                "{message}"
            );
            assert!(message.contains(MIGRATION_GUIDE_URL), "{message}");

            match found {
                Some(found) => assert!(message.contains(&format!("`{found}`")), "{message}"),
                None => assert!(message.contains("no compatibility identity"), "{message}"),
            }
        }
    }

    #[test]
    fn only_the_supported_contracts_are_read() {
        assert!(reads_store_identity(Compatibility::current().as_str()));
        assert!(!reads_store_identity("v1"));
        assert!(!reads_store_identity(""));
    }
}
