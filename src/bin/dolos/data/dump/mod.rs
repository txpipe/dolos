//! `dolos data dump`: records from the state, the archive's logs or its
//! blocks, bounded by `--limit`, as text or as one JSON object per line.

use clap::{Parser, Subcommand, ValueEnum};
use dolos_core::config::RootConfig;

use crate::common::Stores;
use crate::output::{Format, FormatArgs, Usage};

mod blocks;
mod logs;
mod state;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// dumps the entities of a state namespace
    State(state::Args),
    /// dumps the records of a log namespace
    Logs(logs::Args),
    /// dumps the archive's blocks
    Blocks(blocks::Args),
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    match &args.command {
        Command::State(x) => state::run(config, x),
        Command::Logs(x) => logs::run(config, x),
        Command::Blocks(x) => blocks::run(config, x),
    }
}

/// The dataset as it is on disk, opened without writing to it.
fn observe(config: &RootConfig) -> miette::Result<Stores> {
    Ok(crate::common::open_existing_stores(
        config,
        dolos::storage::Access::Observe,
    )?)
}

/// How a text dump lays out its rows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
pub enum Layout {
    /// a table
    #[default]
    Table,
    /// comma-separated rows under cardano-db-sync's column names
    Dbsync,
}

/// How many records a dump prints.
#[derive(Debug, Clone, clap::Args)]
pub struct Bounds {
    /// Print at most this many records; 0 prints every one
    #[arg(long, value_name = "N", default_value_t = 100)]
    pub limit: usize,

    /// Skip this many records first
    #[arg(long, value_name = "N", default_value_t = 0)]
    pub offset: usize,
}

impl Bounds {
    pub fn apply<I: Iterator>(&self, records: I) -> std::iter::Take<std::iter::Skip<I>> {
        let limit = match self.limit {
            0 => usize::MAX,
            limit => limit,
        };

        records.skip(self.offset).take(limit)
    }
}

/// How a dump of a namespace prints its records.
#[derive(Debug, Clone, clap::Args)]
pub struct Output {
    #[command(flatten)]
    pub format: FormatArgs,

    /// How text output lays out its rows
    #[arg(long, value_enum, default_value_t = Layout::Table)]
    pub layout: Layout,
}

impl Output {
    fn checked(&self) -> Result<&Self, Usage> {
        if self.format.format == Format::Json && self.layout != Layout::Table {
            return Err(Usage::new(
                "`--layout` lays out text, and `--format json` prints none",
                None,
            ));
        }

        Ok(self)
    }

    /// Logs on stderr: warnings beside a table, errors only beside rows a
    /// script reads.
    fn setup_tracing(&self) -> miette::Result<()> {
        match (self.format.format, self.layout) {
            (Format::Text, Layout::Table) => crate::common::setup_tracing_warnings(),
            _ => crate::common::setup_tracing_error_only(),
        }
    }
}

/// A namespace the dump does not know, naming those it does.
fn unknown_namespace(kind: &str, name: &str, known: &[&str]) -> Usage {
    Usage::new(
        format!("no {kind} namespace {name}"),
        Some(format!("one of: {}", known.join(", "))),
    )
}
