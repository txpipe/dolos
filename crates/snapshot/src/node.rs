//! What a node's own configuration says about reaching a registry.
//!
//! Which identity a node reads a repository as, and where it stages the bytes
//! it moves, are the node's policy rather than the transport's — but they are
//! read off `dolos.toml` the same way by every command that opens a repository,
//! so they are answered here instead of in each one.

use std::path::{Path, PathBuf};

use dolos_core::config::StorageConfig;

/// The directory a registry transfer stages in when the operator names none.
///
/// A child of `storage.path` rather than a sibling of it: on a host where the
/// data lives on a dedicated mount, a sibling is on the *parent* filesystem —
/// the small root volume this default exists to keep bytes off.
pub const STELE_SCRATCH_DIR: &str = "scratch";

/// Where this node stages the layers of a registry transfer.
///
/// An explicit `--scratch-dir` is taken literally, relative paths included:
/// it is resolved against the working directory like every other path a binary
/// takes from a command line.
pub fn scratch_dir(config: &StorageConfig, chosen: Option<&Path>) -> PathBuf {
    match chosen {
        Some(dir) => dir.to_path_buf(),
        None => config.path.join(STELE_SCRATCH_DIR),
    }
}

#[cfg(feature = "oci")]
mod auth {
    use dolos_core::config::StelaeConfig;

    use crate::{registry::Auth, Error};

    /// The password that goes with the official registry's published user.
    ///
    /// Not a secret: it is a read-only account on a public registry, and the
    /// only thing it buys over anonymous access is a rate limit that a shared
    /// NAT does not exhaust. It is compiled in rather than written into every
    /// generated `dolos.toml` so that a node whose config names the user and no
    /// password authenticates as that user rather than as nobody.
    ///
    /// Rotating the pair is a change here and in `dolos init`'s
    /// `OFFICIAL_REGISTRY_USER` — paired with a release and comms, because
    /// generated configs and released binaries carry the old one until their
    /// nodes update.
    pub const OFFICIAL_REGISTRY_PASSWORD: &str = "7214892e36157f4051677b51526382cc96693d45eda4e4cd";

    /// Which identity this node reads a registry as.
    ///
    /// The environment overrides the file by the ordinary configuration route —
    /// `DOLOS_STELAE_REGISTRY_USER` / `DOLOS_STELAE_REGISTRY_PASSWORD` reach
    /// `[stelae.registry]` like any other setting — so the two sources an
    /// operator sees are the two the configuration has: a consumer's published
    /// user in `dolos.toml`, and a publisher's real credentials exported into
    /// the environment and never written down. The configured user is still the
    /// fallback: it is read-only, so authenticating with it fails a push at the
    /// registry rather than a step earlier, which is the honest place for
    /// "these credentials cannot publish" to be said.
    ///
    /// Two refusals, because both are operator mistakes worth a sentence rather
    /// than a precedence rule:
    ///
    /// - **a token and a user together** — two identities, and which was meant
    ///   is not something to guess at. On a registry whose credentials carry
    ///   different capabilities, guessing is the difference between a publish
    ///   and a 403 nobody can explain.
    /// - **a password with no user** — a secret that arrived with nobody to be.
    ///   It is a typo or half an export, and the half that arrived cannot be
    ///   sent on its own.
    pub fn registry_auth(config: &StelaeConfig) -> Result<Auth, Error> {
        let Some(registry) = &config.registry else {
            return Ok(Auth::Anonymous);
        };

        if registry.token.is_some() && registry.user.is_some() {
            return Err(Error::AmbiguousRegistryIdentity);
        }

        if let Some(token) = &registry.token {
            return Ok(Auth::Bearer(token.clone()));
        }

        match &registry.user {
            Some(user) => Ok(Auth::Basic {
                user: user.clone(),
                // A user and no password means the official registry's.
                password: registry
                    .password
                    .clone()
                    .unwrap_or_else(|| OFFICIAL_REGISTRY_PASSWORD.to_owned()),
            }),
            // A password with nobody to be. Anonymous would be the quiet answer
            // and the wrong one: the operator supplied a secret and it would go
            // unused.
            None if registry.password.is_some() => Err(Error::OrphanRegistryPassword),
            None => Ok(Auth::Anonymous),
        }
    }
}

#[cfg(feature = "oci")]
pub use auth::{registry_auth, OFFICIAL_REGISTRY_PASSWORD};

#[cfg(test)]
mod tests {
    use super::*;

    fn storage(path: &str) -> StorageConfig {
        let document = format!(
            "version = \"v3\"\npath = {}\n",
            toml::Value::String(path.to_owned()),
        );

        toml::from_str(&document).expect("a storage section with a path")
    }

    #[test]
    fn staging_defaults_inside_the_storage_path_and_takes_a_named_one_literally() {
        let config = storage("/var/lib/dolos/data");

        assert_eq!(
            scratch_dir(&config, None),
            PathBuf::from("/var/lib/dolos/data/scratch"),
        );

        for named in ["/mnt/big/staging", "staging", "../staging"] {
            assert_eq!(
                scratch_dir(&config, Some(Path::new(named))),
                PathBuf::from(named),
                "{named}",
            );
        }
    }

    #[cfg(feature = "oci")]
    mod auth {
        use dolos_core::config::{StelaeConfig, StelaeRegistryConfig};

        use crate::{
            node::{registry_auth, OFFICIAL_REGISTRY_PASSWORD},
            registry::Auth,
        };

        fn config(registry: Option<StelaeRegistryConfig>) -> StelaeConfig {
            StelaeConfig { registry }
        }

        fn basic(user: &str, password: Option<&str>) -> StelaeRegistryConfig {
            StelaeRegistryConfig {
                user: Some(user.to_owned()),
                password: password.map(str::to_owned),
                token: None,
            }
        }

        /// The three shapes `[stelae.registry]` can name, and the one it names
        /// by saying nothing.
        #[test]
        fn the_section_names_a_user_a_token_or_nobody() {
            assert_eq!(registry_auth(&config(None)).unwrap(), Auth::Anonymous);

            assert_eq!(
                registry_auth(&config(Some(basic("dolos-reader", Some("published"))))).unwrap(),
                Auth::Basic {
                    user: "dolos-reader".to_owned(),
                    password: "published".to_owned(),
                }
            );

            let bearer = StelaeRegistryConfig {
                token: Some("ghp_x".to_owned()),
                ..Default::default()
            };

            assert_eq!(
                registry_auth(&config(Some(bearer))).unwrap(),
                Auth::Bearer("ghp_x".to_owned())
            );
        }

        /// A user with no password is the shape `dolos init` seeds: the file
        /// says who, the binary says with what.
        #[test]
        fn a_seeded_user_takes_the_compiled_in_password() {
            assert_eq!(
                registry_auth(&config(Some(basic("dolos-reader", None)))).unwrap(),
                Auth::Basic {
                    user: "dolos-reader".to_owned(),
                    password: OFFICIAL_REGISTRY_PASSWORD.to_owned(),
                }
            );
        }

        #[test]
        fn two_identities_at_once_are_refused() {
            let both = StelaeRegistryConfig {
                user: Some("dolos-reader".to_owned()),
                password: None,
                token: Some("ghp_x".to_owned()),
            };

            let message = registry_auth(&config(Some(both))).unwrap_err().to_string();

            assert!(message.contains("token"), "{message}");
            assert!(message.contains("user"), "{message}");
        }

        /// A password with nobody to be. Anonymous would be the quiet answer
        /// and the wrong one: the operator supplied a secret and it
        /// would go unused.
        #[test]
        fn a_password_with_no_user_is_refused() {
            let orphan = StelaeRegistryConfig {
                password: Some("full-access".to_owned()),
                ..Default::default()
            };

            let message = registry_auth(&config(Some(orphan)))
                .unwrap_err()
                .to_string();

            assert!(message.contains("password"), "{message}");
            assert!(message.contains("user"), "{message}");
        }
    }
}
