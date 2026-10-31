//! `dolos service`: `dolos daemon` as a native OS service, one per named
//! instance. systemd on Linux, the user's launchd agents on macOS.
//!
//! `install` reads the configuration it installs; `uninstall` reads one only
//! to derive a name it is not given. Both read the file alone, as the service
//! does: the shell's `DOLOS_` overrides do not reach it. With `--system`, they
//! read only a file `--config` names: under `sudo`, the working directory and
//! any `dolos.toml` in it are the invoking user's.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use dolos::service::{self, Name, ServiceBackend, ServiceError};
use dolos_core::config::{RootConfig, DEFAULT_CONFIG_FILE};
use miette::{Context as _, IntoDiagnostic as _};

use crate::output::{FormatArgs, Usage};

#[derive(Debug, Subcommand)]
pub enum Command {
    /// registers `dolos daemon` with this configuration to start at login, or
    /// at boot with `--system`
    Install(InstallArgs),

    /// stops and unregisters one instance, keeping its configuration and data
    Uninstall(UninstallArgs),
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Args)]
pub struct InstallArgs {
    /// The instance's name, 1 to 48 of a-z, 0-9 and -; without it,
    /// `{chain.protocol}-{chain.network}` from the configuration
    #[arg(value_parser = parse_name)]
    name: Option<Name>,

    /// A system unit, started at boot and installed as root (Linux only), of
    /// the file `--config` names; without it, the user's, started at login
    #[arg(long)]
    system: bool,

    /// Start it now as well
    #[arg(long)]
    start: bool,

    #[command(flatten)]
    format: FormatArgs,
}

#[derive(Debug, clap::Args)]
pub struct UninstallArgs {
    /// The instance's name, which needs no configuration; without it, derived
    /// from the configuration as `install` derives it
    #[arg(value_parser = parse_name)]
    name: Option<Name>,

    /// A system unit, removed as root (Linux only); without NAME, derived from
    /// the file `--config` names
    #[arg(long)]
    system: bool,

    #[command(flatten)]
    format: FormatArgs,
}

fn parse_name(value: &str) -> Result<Name, String> {
    Name::parse(value).map_err(|error| error.to_string())
}

#[cfg(target_os = "linux")]
fn backend(system: bool) -> miette::Result<Box<dyn ServiceBackend>> {
    let scope = if system {
        service::Scope::System
    } else {
        service::Scope::User
    };

    Ok(Box::new(
        service::Systemd::live(scope).map_err(miette::Report::from_err)?,
    ))
}

#[cfg(target_os = "macos")]
fn backend(system: bool) -> miette::Result<Box<dyn ServiceBackend>> {
    if system {
        return Err(Usage::new(
            "launchd agents are per user: `--system` is not supported on macOS",
            Some("leave out `--system` to install a LaunchAgent".into()),
        )
        .into());
    }

    Ok(Box::new(
        service::Launchd::live().map_err(miette::Report::from_err)?,
    ))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn backend(_system: bool) -> miette::Result<Box<dyn ServiceBackend>> {
    Err(miette::miette!(
        help = "`dolos service` installs systemd units on Linux and launchd agents on macOS",
        "services are not supported on this platform"
    ))
}

fn derived(config: &RootConfig) -> miette::Result<Name> {
    Name::derive(config).map_err(|error| {
        Usage::new(error.to_string(), Some("pass the instance's NAME".into())).into()
    })
}

/// The error, with the `sudo` command for one that needs root; `config` is
/// the `--config` given.
fn refused(config: Option<&Path>) -> impl FnOnce(ServiceError) -> miette::Report + '_ {
    move |error| match error {
        ServiceError::NeedsRoot => {
            miette::miette!(
                help = format!("run it as root: {}", as_root(config)),
                "{error}"
            )
        }
        error => miette::Report::from_err(error),
    }
}

/// This command line under `sudo`, naming the binary by the path its system
/// unit records and `config`, the `--config` given, by the path the unit
/// records: a sudo rule copied from it pins both.
fn as_root(config: Option<&Path>) -> String {
    let binary = binary()
        .and_then(|binary| service::binary_for(service::Scope::System, &binary).into_diagnostic())
        .map(PathBuf::into_os_string)
        .unwrap_or_else(|_| "dolos".into());
    let config =
        config.map(|config| service::without_dots(config).unwrap_or_else(|_| config.into()));

    sudo(binary, std::env::args_os().skip(1), config.as_deref())
}

/// `sudo`, `binary` and `args` as one shell line, with `config` as the value
/// of a `--config` among `args`.
fn sudo(
    binary: OsString,
    args: impl IntoIterator<Item = OsString>,
    config: Option<&Path>,
) -> String {
    let mut words = vec![binary];
    let mut args = args.into_iter();

    while let Some(word) = args.next() {
        let text = word.to_string_lossy();
        let rest = text == "--";
        let separate = text == "--config" || text == "-c";
        let given = separate || text.starts_with("--config=") || text.starts_with("-c");

        if rest {
            words.push(word);
            words.extend(args.by_ref());
        } else if given {
            if separate {
                args.next();
            }

            if let Some(config) = config {
                words.extend(["--config".into(), config.into()]);
            }
        } else {
            words.push(word);
        }
    }

    let words: Vec<String> = words
        .iter()
        .map(|word| service::shell_quote(&word.to_string_lossy()).into_owned())
        .collect();

    format!("sudo {}", words.join(" "))
}

/// The configuration file to read in `scope`: `--config`, else
/// `./dolos.toml`, which a system scope never reads. Under `sudo`, the
/// working directory is the invoker's, and root would read whatever file
/// they put there by that name, printing a line of it when it does not parse.
fn config_file<'a>(
    explicit: Option<&'a Path>,
    scope: service::Scope,
    help: &str,
) -> miette::Result<&'a Path> {
    if explicit.is_none() && scope == service::Scope::System {
        return Err(Usage::new(
            format!("`--system` reads no default {DEFAULT_CONFIG_FILE}: name the configuration with `--config`"),
            Some(help.into()),
        )
        .into());
    }

    Ok(crate::common::config_file(explicit))
}

/// The configuration as the service reads it: the file alone.
fn load(config_file: &Path) -> miette::Result<RootConfig> {
    let ignored = dolos_core::config::override_variables().unwrap_or_default();

    if !ignored.is_empty() {
        eprintln!(
            "warning: the service reads {} alone; ignoring {} from this shell",
            config_file.display(),
            ignored.join(", ")
        );
    }

    crate::common::load_file_config(config_file)
        .map_err(miette::Report::from_err)
        .context("loading configuration")
}

/// The running binary, by the path it was run as.
fn binary() -> miette::Result<PathBuf> {
    let exe = std::env::current_exe().into_diagnostic()?;
    // Only a relative path needs it, and it may have been removed.
    let cwd = std::env::current_dir().ok();
    let invoked = std::env::args_os().next();
    let path = std::env::var_os("PATH");

    service::locate_binary(invoked.as_deref(), path.as_deref(), cwd.as_deref(), &exe)
        .into_diagnostic()
        .context("locating the dolos binary")
}

fn install(explicit: Option<&Path>, args: &InstallArgs) -> miette::Result<()> {
    let backend = backend(args.system)?;
    let config_file = config_file(
        explicit,
        backend.scope(),
        "name the file the unit runs, as `dolos --config <file> service install --system`",
    )?;
    let config = load(config_file)?;
    crate::common::check_network(&config).context("loading configuration")?;

    let name = match &args.name {
        Some(name) => name.clone(),
        None => derived(&config)?,
    };

    let request = service::Install {
        name,
        binary: binary()?,
        config_file: config_file.to_path_buf(),
        config: &config,
        start: args.start,
    };

    let installed = service::install(backend.as_ref(), &request).map_err(refused(explicit))?;

    for warning in &installed.warnings {
        eprintln!("warning: {warning}");
    }

    crate::output::emit(&installed, &args.format)
}

fn uninstall(explicit: Option<&Path>, args: &UninstallArgs) -> miette::Result<()> {
    let backend = backend(args.system)?;

    let name = match &args.name {
        Some(name) => name.clone(),
        None => {
            let config_file = config_file(
                explicit,
                backend.scope(),
                "pass the instance's NAME, or the file to derive it from with `--config`",
            )?;

            derived(&load(config_file)?)?
        }
    };

    let uninstalled = service::uninstall(backend.as_ref(), &name).map_err(refused(explicit))?;

    crate::output::emit(&uninstalled, &args.format)
}

/// Runs `args`; `explicit_config` is the `--config` given.
pub fn run(explicit_config: Option<&Path>, args: &Args) -> miette::Result<()> {
    match &args.command {
        Command::Install(x) => install(explicit_config, x),
        Command::Uninstall(x) => uninstall(explicit_config, x),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_sudo_command_names_the_configuration_by_the_path_given() {
        let config = Path::new("/srv/preview/dolos.toml");
        let hint = |args: &[&str], config| {
            sudo(
                "/opt/dolos/bin/dolos".into(),
                args.iter().map(OsString::from),
                config,
            )
        };

        for given in [
            &["--config", "dolos.toml"][..],
            &["-c", "dolos.toml"],
            &["--config=dolos.toml"],
            &["-cdolos.toml"],
            &["-c=dolos.toml"],
        ] {
            let args: Vec<&str> = given
                .iter()
                .copied()
                .chain(["service", "install", "--system"])
                .collect();

            assert_eq!(
                hint(&args, Some(config)),
                "sudo /opt/dolos/bin/dolos --config /srv/preview/dolos.toml service install --system",
                "{given:?}"
            );
        }

        // Where it was given.
        assert_eq!(
            hint(&["service", "install", "--system", "-c", "x", "--start"], Some(config)),
            "sudo /opt/dolos/bin/dolos service install --system --config /srv/preview/dolos.toml --start"
        );

        // After `--`, the instance's name.
        assert_eq!(
            hint(&["service", "uninstall", "--system", "--", "-c"], None),
            "sudo /opt/dolos/bin/dolos service uninstall --system -- -c"
        );

        assert_eq!(
            hint(
                &["-c", "x", "service", "install", "--system"],
                Some(Path::new("/srv/my node/dolos.toml"))
            ),
            "sudo /opt/dolos/bin/dolos --config '/srv/my node/dolos.toml' service install --system"
        );
    }
}
