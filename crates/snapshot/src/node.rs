//! What a node says about reaching a registry.
//!
//! Which identity a node reads a repository as, and where it stages the bytes
//! it moves, are the node's policy rather than the transport's — but every
//! command that opens a repository answers them the same way, so they are
//! answered here instead of in each one.

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

mod auth {
    use crate::{registry::Auth, Error};

    /// The password that goes with the official registry's published user.
    ///
    /// Not a secret: it is a read-only account on a public registry, and the
    /// only thing it buys over anonymous access is a rate limit that a shared
    /// NAT does not exhaust. It is compiled in rather than handed to every
    /// invocation so that naming the user and no password authenticates as
    /// that user rather than as nobody.
    ///
    /// Rotating the pair is a change here and in the catalog's
    /// `dolos_cardano::networks` stelae user — paired with a release and
    /// comms, because released binaries carry the old one until their nodes
    /// update.
    pub const OFFICIAL_REGISTRY_PASSWORD: &str = "7214892e36157f4051677b51526382cc96693d45eda4e4cd";

    /// The registry credentials an invocation names: its arguments, its
    /// environment and any credentials file, already merged by the command.
    ///
    /// Nothing here reads the environment or a file; the command decides its
    /// sources and hands the result over as a value.
    #[derive(Clone, Default, PartialEq, Eq)]
    pub struct RegistryAuth {
        /// The identity to authenticate as, sent with a password as HTTP
        /// Basic.
        pub user: Option<String>,
        /// Omitted for the official registry's published user, whose
        /// password is [`OFFICIAL_REGISTRY_PASSWORD`].
        pub password: Option<String>,
        /// A bearer token, for a registry that issues them rather than
        /// accepting a pair. Mutually exclusive with `user`.
        pub token: Option<String>,
    }

    impl RegistryAuth {
        pub fn from_inputs(
            user: Option<String>,
            password: Option<String>,
            token: Option<String>,
        ) -> Self {
            Self {
                user,
                password,
                token,
            }
        }
    }

    /// Names the user and never a secret.
    ///
    /// Inputs end up in diagnostics; a derived `Debug` here would put a
    /// publisher's credentials in whatever a bug report happens to include.
    impl std::fmt::Debug for RegistryAuth {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let redacted = |value: &Option<String>| value.as_ref().map(|_| "<redacted>");

            f.debug_struct("RegistryAuth")
                .field("user", &self.user)
                .field("password", &redacted(&self.password))
                .field("token", &redacted(&self.token))
                .finish()
        }
    }

    /// Which identity this node reads a registry as.
    ///
    /// A user with no password takes [`OFFICIAL_REGISTRY_PASSWORD`]: the
    /// official user is read-only, so authenticating with it fails a push at
    /// the registry rather than a step earlier, which is the honest place for
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
    pub fn registry_auth(inputs: &RegistryAuth) -> Result<Auth, Error> {
        if inputs.token.is_some() && inputs.user.is_some() {
            return Err(Error::AmbiguousRegistryIdentity);
        }

        if let Some(token) = &inputs.token {
            return Ok(Auth::Bearer(token.clone()));
        }

        match &inputs.user {
            Some(user) => Ok(Auth::Basic {
                user: user.clone(),
                password: inputs
                    .password
                    .clone()
                    .unwrap_or_else(|| OFFICIAL_REGISTRY_PASSWORD.to_owned()),
            }),
            None if inputs.password.is_some() => Err(Error::OrphanRegistryPassword),
            None => Ok(Auth::Anonymous),
        }
    }
}

pub use auth::{registry_auth, RegistryAuth, OFFICIAL_REGISTRY_PASSWORD};

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

    mod auth {
        use crate::{
            node::{registry_auth, RegistryAuth, OFFICIAL_REGISTRY_PASSWORD},
            registry::Auth,
        };

        fn basic(user: &str, password: Option<&str>) -> RegistryAuth {
            RegistryAuth::from_inputs(Some(user.to_owned()), password.map(str::to_owned), None)
        }

        /// The three identities the inputs can name, and the one they name
        /// by saying nothing.
        #[test]
        fn the_inputs_name_a_user_a_token_or_nobody() {
            assert_eq!(
                registry_auth(&RegistryAuth::default()).unwrap(),
                Auth::Anonymous
            );

            assert_eq!(
                registry_auth(&basic("dolos-reader", Some("published"))).unwrap(),
                Auth::Basic {
                    user: "dolos-reader".to_owned(),
                    password: "published".to_owned(),
                }
            );

            let bearer = RegistryAuth::from_inputs(None, None, Some("ghp_x".to_owned()));

            assert_eq!(
                registry_auth(&bearer).unwrap(),
                Auth::Bearer("ghp_x".to_owned())
            );
        }

        /// A user with no password takes the compiled-in one: the invocation
        /// says who, the binary says with what.
        #[test]
        fn a_user_alone_takes_the_compiled_in_password() {
            assert_eq!(
                registry_auth(&basic("dolos-reader", None)).unwrap(),
                Auth::Basic {
                    user: "dolos-reader".to_owned(),
                    password: OFFICIAL_REGISTRY_PASSWORD.to_owned(),
                }
            );
        }

        #[test]
        fn two_identities_at_once_are_refused() {
            let both = RegistryAuth::from_inputs(
                Some("dolos-reader".to_owned()),
                None,
                Some("ghp_x".to_owned()),
            );

            let message = registry_auth(&both).unwrap_err().to_string();

            assert!(message.contains("token"), "{message}");
            assert!(message.contains("user"), "{message}");
        }

        #[test]
        fn a_password_with_no_user_is_refused() {
            let orphan = RegistryAuth::from_inputs(None, Some("full-access".to_owned()), None);

            let message = registry_auth(&orphan).unwrap_err().to_string();

            assert!(message.contains("password"), "{message}");
            assert!(message.contains("user"), "{message}");
        }

        #[test]
        fn debug_names_the_user_and_never_a_secret() {
            let inputs = RegistryAuth::from_inputs(
                Some("publisher".to_owned()),
                Some("full-access".to_owned()),
                Some("ghp_x".to_owned()),
            );

            let printed = format!("{inputs:?}");

            assert!(printed.contains("publisher"), "{printed}");
            assert!(!printed.contains("full-access"), "{printed}");
            assert!(!printed.contains("ghp_x"), "{printed}");
        }
    }
}
