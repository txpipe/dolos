//! Publishing this node's data as a Stelae snapshot.
//!
//! Dolos's own word is "snapshot"; the protocol's is "stele". The translation
//! happens here and nowhere else — see `solution/stelae` and
//! `adrs/004_stelae_snapshots.md`.
//!
//! Only `publish` exists so far. `digest`, `verify`, `inspect` and `sign` are
//! the publisher-productization slice, and restore is its own.
//!
//! Publishing into a registry is behind the `registry` feature, which is
//! default-off; `--repo` in a build without it is a refusal naming the feature.

use clap::{Parser, Subcommand};
use dolos_core::config::RootConfig;

mod publish;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// writes a stele to a local directory or an OCI repository
    Publish(publish::Args),
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    match &args.command {
        Command::Publish(x) => publish::run(config, x),
    }
}
