//! The credentials a command reads a stele registry with.
//!
//! Inputs of the invocation, never of `dolos.toml`: the user from
//! `--registry-user` or `DOLOS_STELAE_USER`, and the secret from
//! `DOLOS_STELAE_PASSWORD` / `DOLOS_STELAE_TOKEN` or a credentials file. A
//! secret is never an argument, where it would land in shell history and
//! process listings.
//!
//! What the inputs authenticate as is
//! [`dolos_snapshot::node::registry_auth`]'s; what is here is where they come
//! from, and the one default this binary adds: the official repository of the
//! configured network is read as its published read-only user.

use std::path::{Path, PathBuf};

use dolos_cardano::networks::KnownNetwork;
use dolos_snapshot::{
    node::{self, RegistryAuth},
    registry::{Auth, Repository},
};
use miette::{Context as _, IntoDiagnostic as _};

pub const PASSWORD_ENV: &str = "DOLOS_STELAE_PASSWORD";
pub const TOKEN_ENV: &str = "DOLOS_STELAE_TOKEN";

#[derive(Debug, Clone, Default, clap::Args)]
pub struct CredentialArgs {
    /// the user to read the registry as; defaults to the published read-only
    /// user when the repository is the configured network's official one. The
    /// password comes from DOLOS_STELAE_PASSWORD or --registry-credentials-file
    #[arg(long, env = "DOLOS_STELAE_USER", value_name = "USER")]
    pub registry_user: Option<String>,

    /// a TOML file naming the registry secret as `password` or `token`; it
    /// takes the place of DOLOS_STELAE_PASSWORD / DOLOS_STELAE_TOKEN
    #[arg(long, value_name = "PATH")]
    pub registry_credentials_file: Option<PathBuf>,
}

/// The secret half of the credentials, as a credentials file carries it.
///
/// No `Debug`, so that it cannot be printed.
#[derive(serde::Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Secrets {
    password: Option<String>,
    token: Option<String>,
}

impl CredentialArgs {
    /// How this invocation reads `repository`.
    pub fn auth(
        &self,
        repository: &Repository,
        network: Option<&KnownNetwork>,
    ) -> miette::Result<Auth> {
        let inputs = self.inputs(repository, network, |name| std::env::var(name).ok())?;

        node::registry_auth(&inputs).map_err(|error| self.explain(error))
    }

    /// The profile crate's refusal, with which of this invocation's inputs to
    /// change: the token may have come from the environment or from a file.
    fn explain(&self, error: dolos_snapshot::Error) -> miette::Report {
        let help = match (&error, &self.registry_credentials_file) {
            (dolos_snapshot::Error::AmbiguousRegistryIdentity, Some(path)) => format!(
                "drop the token in {}, or --registry-user / DOLOS_STELAE_USER",
                path.display()
            ),
            (dolos_snapshot::Error::AmbiguousRegistryIdentity, None) => {
                format!("drop {TOKEN_ENV}, or --registry-user / DOLOS_STELAE_USER")
            }
            (dolos_snapshot::Error::OrphanRegistryPassword, _) => {
                "pass --registry-user or set DOLOS_STELAE_USER".to_owned()
            }
            _ => return miette::Report::from_err(error),
        };

        miette::miette!(help = help, "{error}")
    }

    fn inputs(
        &self,
        repository: &Repository,
        network: Option<&KnownNetwork>,
        env: impl Fn(&str) -> Option<String>,
    ) -> miette::Result<RegistryAuth> {
        let secrets = match &self.registry_credentials_file {
            Some(path) => read_secrets(path)?,
            None => Secrets {
                password: env(PASSWORD_ENV),
                token: env(TOKEN_ENV),
            },
        };

        // An empty value is no secret, whichever source it came from.
        let password = secrets.password.filter(|value| !value.is_empty());
        let token = secrets.token.filter(|value| !value.is_empty());

        let user = match (&self.registry_user, &token) {
            (Some(user), _) => Some(user.clone()),
            (None, None) => official_user(repository, network).map(str::to_owned),
            (None, Some(_)) => None,
        };

        Ok(RegistryAuth::from_inputs(user, password, token))
    }
}

/// Reads a credentials file, saying where it failed and never what it holds.
///
/// A parse error would quote the offending line, and that line is a secret.
fn read_secrets(path: &Path) -> miette::Result<Secrets> {
    let context = || format!("reading the registry credentials in {}", path.display());

    let text = std::fs::read_to_string(path)
        .into_diagnostic()
        .with_context(context)?;

    ::config::Config::builder()
        .add_source(::config::File::from_str(&text, ::config::FileFormat::Toml))
        .build()
        .and_then(|file| file.try_deserialize())
        .map_err(|_| {
            miette::miette!(
                "{} is not a TOML file naming only `password` or `token`",
                path.display()
            )
        })
        .with_context(context)
}

/// The catalog's read-only user, for the configured network's official
/// repository and no other: its password is compiled in, and has no business
/// reaching a registry that is not the one it was published for.
fn official_user(repository: &Repository, network: Option<&KnownNetwork>) -> Option<&'static str> {
    let network = network?;
    let official: Repository = network.stelae.registry.parse().ok()?;

    (official.registry() == repository.registry() && !network.stelae.user.is_empty())
        .then_some(network.stelae.user)
}

#[cfg(test)]
mod tests {
    use dolos_cardano::networks;
    use dolos_snapshot::node::OFFICIAL_REGISTRY_PASSWORD;

    use super::*;

    fn repo(raw: &str) -> Repository {
        raw.parse().unwrap()
    }

    fn official() -> Repository {
        repo(networks::MAINNET.stelae.registry)
    }

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn auth(
        args: &CredentialArgs,
        repository: &Repository,
        network: Option<&KnownNetwork>,
        env: impl Fn(&str) -> Option<String>,
    ) -> Auth {
        let inputs = args.inputs(repository, network, env).unwrap();
        node::registry_auth(&inputs).unwrap()
    }

    #[test]
    fn the_official_repository_is_read_as_the_published_user() {
        assert_eq!(
            auth(
                &CredentialArgs::default(),
                &official(),
                Some(&networks::MAINNET),
                no_env
            ),
            Auth::Basic {
                user: networks::MAINNET.stelae.user.to_owned(),
                password: OFFICIAL_REGISTRY_PASSWORD.to_owned(),
            }
        );
    }

    #[test]
    fn another_registry_or_a_custom_network_is_read_anonymously() {
        let mirror = repo("oci://ghcr.io/txpipe/dolos-snapshots/mainnet");

        assert_eq!(
            auth(
                &CredentialArgs::default(),
                &mirror,
                Some(&networks::MAINNET),
                no_env
            ),
            Auth::Anonymous
        );
        assert_eq!(
            auth(&CredentialArgs::default(), &official(), None, no_env),
            Auth::Anonymous
        );
    }

    #[test]
    fn explicit_inputs_win_over_the_published_user() {
        let args = CredentialArgs {
            registry_user: Some("publisher".to_owned()),
            ..Default::default()
        };
        let env = |name: &str| (name == PASSWORD_ENV).then(|| "full-access".to_owned());

        assert_eq!(
            auth(&args, &official(), Some(&networks::MAINNET), env),
            Auth::Basic {
                user: "publisher".to_owned(),
                password: "full-access".to_owned(),
            }
        );

        let env = |name: &str| (name == TOKEN_ENV).then(|| "ghp_x".to_owned());

        assert_eq!(
            auth(
                &CredentialArgs::default(),
                &official(),
                Some(&networks::MAINNET),
                env
            ),
            Auth::Bearer("ghp_x".to_owned())
        );
    }

    /// The secret is read from the environment and appears in nothing the
    /// inputs print.
    #[test]
    fn an_environment_secret_is_read_and_never_echoed() {
        let args = CredentialArgs {
            registry_user: Some("publisher".to_owned()),
            ..Default::default()
        };
        let env = |name: &str| (name == PASSWORD_ENV).then(|| "s3cret-from-env".to_owned());

        let inputs = args
            .inputs(&official(), Some(&networks::MAINNET), env)
            .unwrap();

        assert_eq!(inputs.password.as_deref(), Some("s3cret-from-env"));

        let printed = format!("{inputs:?} {args:?}");
        assert!(!printed.contains("s3cret-from-env"), "{printed}");
    }

    #[test]
    fn an_empty_variable_is_no_secret() {
        for variable in [TOKEN_ENV, PASSWORD_ENV] {
            let env = |name: &str| (name == variable).then(String::new);

            assert_eq!(
                auth(
                    &CredentialArgs::default(),
                    &official(),
                    Some(&networks::MAINNET),
                    env
                ),
                Auth::Basic {
                    user: networks::MAINNET.stelae.user.to_owned(),
                    password: OFFICIAL_REGISTRY_PASSWORD.to_owned(),
                },
                "{variable}"
            );
        }
    }

    /// An empty value in a credentials file counts as unset, as an empty
    /// variable does: no empty bearer, no empty password.
    #[test]
    fn an_empty_file_value_is_no_secret() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.toml");

        let args = CredentialArgs {
            registry_credentials_file: Some(path.clone()),
            ..Default::default()
        };

        std::fs::write(&path, "token = \"\"\n").unwrap();

        assert_eq!(
            auth(&args, &official(), Some(&networks::MAINNET), no_env),
            Auth::Basic {
                user: networks::MAINNET.stelae.user.to_owned(),
                password: OFFICIAL_REGISTRY_PASSWORD.to_owned(),
            }
        );

        let args = CredentialArgs {
            registry_user: Some("publisher".to_owned()),
            ..args
        };
        std::fs::write(&path, "password = \"\"\n").unwrap();

        let inputs = args
            .inputs(&official(), Some(&networks::MAINNET), no_env)
            .unwrap();
        assert_eq!(inputs.password, None);
        assert_eq!(inputs.token, None);
    }

    #[test]
    fn a_credentials_file_takes_the_place_of_the_environment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.toml");
        std::fs::write(&path, "token = \"from-file\"\n").unwrap();

        let args = CredentialArgs {
            registry_credentials_file: Some(path.clone()),
            ..Default::default()
        };
        let env = |name: &str| (name == PASSWORD_ENV).then(|| "from-env".to_owned());

        assert_eq!(
            auth(&args, &official(), Some(&networks::MAINNET), env),
            Auth::Bearer("from-file".to_owned())
        );

        std::fs::write(&path, "secret = \"misspelled\"\n").unwrap();
        let message = format!(
            "{:?}",
            args.inputs(&official(), Some(&networks::MAINNET), no_env)
                .err()
                .unwrap()
        );
        assert!(message.contains("credentials"), "{message}");
        assert!(!message.contains("misspelled"), "{message}");
    }

    /// A file that does not parse fails naming its path; the line the parser
    /// would quote is the secret.
    #[test]
    fn a_malformed_credentials_file_is_never_echoed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.toml");

        let args = CredentialArgs {
            registry_credentials_file: Some(path.clone()),
            ..Default::default()
        };

        for document in [
            "token = ghp_FAKESECRET123\n",
            "password = \"ghp_FAKESECRET123\n",
            "token = [\"ghp_FAKESECRET123\"]\n",
        ] {
            std::fs::write(&path, document).unwrap();

            let message = format!(
                "{:?}",
                args.inputs(&official(), Some(&networks::MAINNET), no_env)
                    .err()
                    .unwrap()
            );

            assert!(message.contains("credentials.toml"), "{message}");
            assert!(!message.contains("FAKESECRET"), "{message}");
        }
    }

    /// Which token to drop depends on where it came from.
    #[test]
    fn an_ambiguous_identity_names_where_the_token_came_from() {
        let _guard = crate::common::ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        std::env::remove_var(TOKEN_ENV);
        std::env::remove_var(PASSWORD_ENV);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.toml");
        std::fs::write(&path, "token = \"from-file\"\n").unwrap();

        let args = CredentialArgs {
            registry_user: Some("publisher".to_owned()),
            registry_credentials_file: Some(path),
        };

        let message = format!(
            "{:?}",
            args.auth(&official(), Some(&networks::MAINNET))
                .err()
                .unwrap()
        );

        assert!(message.contains("credentials.toml"), "{message}");
        assert!(!message.contains(TOKEN_ENV), "{message}");
        assert!(!message.contains("from-file"), "{message}");
    }

    /// A token from the environment is the variable to drop.
    #[test]
    fn an_ambiguous_identity_from_the_environment_names_the_variable() {
        let _guard = crate::common::ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        std::env::remove_var(PASSWORD_ENV);
        std::env::set_var(TOKEN_ENV, "from-env");

        let args = CredentialArgs {
            registry_user: Some("publisher".to_owned()),
            ..Default::default()
        };
        let result = args.auth(&official(), Some(&networks::MAINNET));

        std::env::remove_var(TOKEN_ENV);

        let message = format!("{:?}", result.err().unwrap());

        assert!(message.contains(&format!("drop {TOKEN_ENV}")), "{message}");
        assert!(!message.contains("from-env"), "{message}");
    }

    /// A password with no user to pair it with says how to name one.
    #[test]
    fn an_orphan_password_says_how_to_name_the_user() {
        let _guard = crate::common::ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        std::env::remove_var(TOKEN_ENV);
        std::env::set_var(PASSWORD_ENV, "orphan");

        let mirror = repo("oci://registry.example/stelae/mainnet");
        let result = CredentialArgs::default().auth(&mirror, Some(&networks::MAINNET));

        std::env::remove_var(PASSWORD_ENV);

        let message = format!("{:?}", result.err().unwrap());

        assert!(
            message.contains("pass --registry-user or set DOLOS_STELAE_USER"),
            "{message}"
        );
        assert!(!message.contains("orphan"), "{message}");
    }

    /// The documented variable names, through the real environment: a typo in
    /// either constant would pass every test that injects one.
    #[test]
    fn the_secret_variables_are_read_from_the_environment() {
        let _guard = crate::common::ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let args = CredentialArgs {
            registry_user: Some("publisher".to_owned()),
            ..Default::default()
        };

        std::env::remove_var("DOLOS_STELAE_TOKEN");
        std::env::set_var("DOLOS_STELAE_PASSWORD", "full-access");
        let basic = args.auth(&official(), Some(&networks::MAINNET));
        std::env::remove_var("DOLOS_STELAE_PASSWORD");

        assert_eq!(
            basic.unwrap(),
            Auth::Basic {
                user: "publisher".to_owned(),
                password: "full-access".to_owned(),
            }
        );

        std::env::set_var("DOLOS_STELAE_TOKEN", "ghp_x");
        let bearer = CredentialArgs::default().auth(&official(), Some(&networks::MAINNET));
        std::env::remove_var("DOLOS_STELAE_TOKEN");

        assert_eq!(bearer.unwrap(), Auth::Bearer("ghp_x".to_owned()));
    }

    #[test]
    fn the_user_flag_reads_its_environment_variable() {
        #[derive(clap::Parser)]
        struct Cli {
            #[command(flatten)]
            args: CredentialArgs,
        }

        let _guard = crate::common::ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        std::env::set_var("DOLOS_STELAE_USER", "publisher");
        let parsed = <Cli as clap::Parser>::try_parse_from(["cmd"]);
        std::env::remove_var("DOLOS_STELAE_USER");

        assert_eq!(
            parsed.unwrap().args.registry_user.as_deref(),
            Some("publisher")
        );
    }
}
