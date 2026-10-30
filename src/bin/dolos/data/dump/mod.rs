//! `dolos data dump`: records from the state, the archive's logs or its
//! blocks, bounded by `--limit`, as text or as one JSON object per line.

use clap::{Parser, Subcommand, ValueEnum};
use dolos_core::config::RootConfig;

use crate::output::{Format, FormatArgs, Usage};
use dolos::storage::{Access, ArchiveStoreBackend, StateStoreBackend};

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

/// The state as it is on disk, opened without writing to it.
fn observe_state(config: &RootConfig) -> miette::Result<StateStoreBackend> {
    Ok(dolos::storage::open_existing_state(
        config,
        Access::Observe,
    )?)
}

/// The archive as it is on disk, opened without writing to it: a dataset
/// that holds only an archive dumps its blocks and logs.
fn observe_archive(config: &RootConfig) -> miette::Result<ArchiveStoreBackend> {
    Ok(dolos::storage::open_existing_archive(
        config,
        Access::Observe,
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
    fn limit(&self) -> usize {
        match self.limit {
            0 => usize::MAX,
            limit => limit,
        }
    }

    pub fn apply<I: Iterator>(&self, records: I) -> std::iter::Take<std::iter::Skip<I>> {
        records.skip(self.offset).take(self.limit())
    }

    /// [`Bounds::apply`] over records that can fail to read: `--offset`
    /// skips only records read, so a failure inside it still reaches the
    /// caller.
    pub fn apply_fallible<T, E>(
        &self,
        records: impl Iterator<Item = Result<T, E>>,
    ) -> impl Iterator<Item = Result<T, E>> {
        let mut skipping = self.offset;

        records
            .filter(move |record| match record {
                Ok(_) if skipping > 0 => {
                    skipping -= 1;
                    false
                }
                _ => true,
            })
            .take(self.limit())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_offset_skips_records_but_not_failures() {
        let bounds = Bounds {
            limit: 2,
            offset: 3,
        };

        let records = [Ok(0), Err("torn"), Ok(1), Ok(2), Ok(3), Ok(4)];
        let bounded: Vec<_> = bounds.apply_fallible(records.into_iter()).collect();
        assert_eq!(bounded, [Err("torn"), Ok(3)]);

        let records = [Ok(0), Ok(1), Ok(2), Ok(3), Ok(4), Ok::<_, &str>(5)];
        let bounded: Vec<_> = bounds.apply_fallible(records.into_iter()).collect();
        assert_eq!(bounded, [Ok(3), Ok(4)]);
    }
}
