//! `dolos service`: `dolos daemon` as a native OS service, one per named
//! instance. systemd on Linux, the user's launchd agents on macOS.
//!
//! `install` reads the configuration it installs; `uninstall` reads one only
//! to derive a name it is not given.

use std::path::Path;

use clap::{Parser, Subcommand};
use dolos::service::{self, Name, ServiceBackend, ServiceError};
use dolos_core::config::RootConfig;
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

    /// A system unit, started at boot and installed as root (Linux only);
    /// without it, the user's, started at login
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

    /// A system unit, removed as root (Linux only)
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

    Ok(Box::new(service::Systemd::live(scope).map_err(refused)?))
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

    Ok(Box::new(service::Launchd::live().map_err(refused)?))
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

fn refused(error: ServiceError) -> miette::Report {
    match error {
        ServiceError::NeedsRoot => {
            miette::miette!(help = format!("run it as root: {}", as_root()), "{error}")
        }
        error => miette::Report::from_err(error),
    }
}

/// This command line under `sudo`, naming the binary by its path.
fn as_root() -> String {
    let binary = std::env::current_exe()
        .map(|path| path.into_os_string())
        .unwrap_or_else(|_| "dolos".into());

    let words: Vec<String> = std::iter::once(binary)
        .chain(std::env::args_os().skip(1))
        .map(|word| service::shell_quote(&word.to_string_lossy()).into_owned())
        .collect();

    format!("sudo {}", words.join(" "))
}

fn install(config_file: &Path, args: &InstallArgs) -> miette::Result<()> {
    let backend = backend(args.system)?;
    let config = crate::common::load_and_check_config(config_file)?;

    let name = match &args.name {
        Some(name) => name.clone(),
        None => derived(&config)?,
    };

    let binary = std::env::current_exe()
        .into_diagnostic()
        .context("locating the dolos binary")?;

    let request = service::Install {
        name,
        binary,
        config_file: config_file.to_path_buf(),
        config: &config,
        start: args.start,
    };

    let installed = service::install(backend.as_ref(), &request).map_err(refused)?;

    for warning in &installed.warnings {
        eprintln!("warning: {warning}");
    }

    crate::output::emit(&installed, &args.format)
}

fn uninstall(config_file: &Path, args: &UninstallArgs) -> miette::Result<()> {
    let backend = backend(args.system)?;

    let name = match &args.name {
        Some(name) => name.clone(),
        None => {
            let config = crate::common::load_config(Some(config_file))
                .map_err(miette::Report::from_err)
                .context("loading configuration")?;

            derived(&config)?
        }
    };

    let uninstalled = service::uninstall(backend.as_ref(), &name).map_err(refused)?;

    crate::output::emit(&uninstalled, &args.format)
}

pub fn run(config_file: &Path, args: &Args) -> miette::Result<()> {
    match &args.command {
        Command::Install(x) => install(config_file, x),
        Command::Uninstall(x) => uninstall(config_file, x),
    }
}
