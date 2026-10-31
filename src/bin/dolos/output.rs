//! How commands report: a result on stdout, as text or JSON; progress, logs
//! and errors on stderr; and an exit code a script can branch on.

use std::fmt::Display;
use std::io::Write as _;

use miette::IntoDiagnostic;
use serde::Serialize;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum Format {
    #[default]
    Text,
    Json,
}

#[derive(Debug, Clone, Default, clap::Args)]
pub struct FormatArgs {
    /// How to print the result: text for people, JSON for scripts
    #[arg(long, value_enum, default_value_t = Format::Text)]
    pub format: Format,
}

/// A command's result, printed as text by its `Display` and as JSON by its
/// `Serialize`.
pub trait Report: Display {
    fn to_json(&self) -> serde_json::Result<serde_json::Value>;
}

impl<T: Serialize + Display> Report for T {
    fn to_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(self)
    }
}

/// Write `report` to stdout as `args` asks: JSON as one object on one line.
pub fn emit(report: &dyn Report, args: &FormatArgs) -> miette::Result<()> {
    let mut stdout = std::io::stdout().lock();

    match args.format {
        Format::Text => writeln!(stdout, "{report}").into_diagnostic(),
        Format::Json => {
            let json = report.to_json().into_diagnostic()?;
            writeln!(stdout, "{json}").into_diagnostic()
        }
    }
}

/// The process exit codes. Clap exits 2 itself on a malformed command line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ExitCode {
    Ok = 0,
    Error = 1,
    /// The command line asks for something the command won't do as given.
    Usage = 2,
    /// The command ran and found the node or its data unhealthy.
    #[allow(
        dead_code,
        reason = "the first check that reports it lands with the sync group"
    )]
    Unhealthy = 3,
}

impl ExitCode {
    /// The code for a command's failure.
    pub fn of(report: &miette::Report) -> Self {
        if report.downcast_ref::<Usage>().is_some() {
            Self::Usage
        } else if report.downcast_ref::<Unhealthy>().is_some() {
            Self::Unhealthy
        } else {
            Self::Error
        }
    }
}

impl From<ExitCode> for std::process::ExitCode {
    fn from(code: ExitCode) -> Self {
        Self::from(code as u8)
    }
}

/// A command line the command refuses as given; exits 2.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct Usage {
    message: String,
    help: Option<String>,
}

impl miette::Diagnostic for Usage {
    fn help<'a>(&'a self) -> Option<Box<dyn Display + 'a>> {
        self.help
            .as_ref()
            .map(|help| Box::new(help) as Box<dyn Display + 'a>)
    }
}

/// What a command found wrong with the node or its data; exits 3.
#[allow(
    dead_code,
    reason = "the first check that reports it lands with the sync group"
)]
#[derive(Debug, thiserror::Error, miette::Diagnostic)]
#[error("{0}")]
pub struct Unhealthy(pub String);

/// `--yes`, for a command that destroys data.
#[allow(
    dead_code,
    reason = "the first destructive command ported lands with the data group"
)]
#[derive(Debug, Clone, Default, clap::Args)]
pub struct ConfirmArgs {
    /// Go ahead with what the command destroys
    #[arg(long)]
    pub yes: bool,
}

#[allow(
    dead_code,
    reason = "the first destructive command ported lands with the data group"
)]
impl ConfirmArgs {
    /// Refuse, naming `what` would be destroyed, unless `--yes` was given.
    pub fn require(&self, what: &str) -> Result<(), Usage> {
        if self.yes {
            return Ok(());
        }

        Err(Usage {
            message: format!("this would destroy {what}"),
            help: Some("pass `--yes` to go ahead".into()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refused_confirmation_exits_with_usage() {
        let error = ConfirmArgs { yes: false }
            .require("the state store at ./data/state")
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("the state store at ./data/state"),
            "{error}"
        );
        assert_eq!(ExitCode::of(&miette::Report::new(error)), ExitCode::Usage);
        assert!(ConfirmArgs { yes: true }.require("anything").is_ok());
    }

    #[test]
    fn exit_codes_follow_the_failure() {
        assert_eq!(
            ExitCode::of(&miette::Report::new(Unhealthy("behind".into()))),
            ExitCode::Unhealthy
        );
        assert_eq!(ExitCode::of(&miette::miette!("failed")), ExitCode::Error);
        assert_eq!(
            ExitCode::of(
                &miette::Report::new(Usage {
                    message: "no".into(),
                    help: None,
                })
                .wrap_err("in context")
            ),
            ExitCode::Usage
        );
    }
}
