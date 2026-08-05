//! `dolos bootstrap stelae` — rebuild a node from a Stelae snapshot.
//!
//! A sibling of [`super::snapshot`] rather than a mode of it. The two share a
//! goal and nothing else: one unpacks a gzip tar of the storage engines' own
//! files over the data directory, the other reads a set of deterministic CBOR
//! layers through the store traits. Their flags, their failure modes and their
//! trust stories are all different, so folding the second into the first would
//! have meant a command whose options only make sense in pairs.
//!
//! Everything below this module is `dolos_snapshot::restore`, which is generic
//! over the store traits. What only this module knows is the node: its network
//! magic, which comes from genesis and never from a file an operator can edit,
//! and its `sync.max_history`, which bounds how much chain history a restore
//! bothers to read.

use std::path::PathBuf;

use dolos_core::config::RootConfig;
use miette::{Context as _, IntoDiagnostic as _};

#[derive(Debug, clap::Args, Clone)]
pub struct Args {
    /// Where to restore from, as a URL. Today only `file://DIR`, naming a stele
    /// directory written by `dolos snapshot publish --output-dir`.
    #[arg(long)]
    pub source: Source,
}

impl Args {
    pub fn inquire() -> miette::Result<Self> {
        let source = inquire::Text::new("where is the stele?")
            .with_help_message("a directory written by `dolos snapshot publish --output-dir`")
            .with_placeholder("file:///var/lib/dolos/stele")
            .prompt()
            .into_diagnostic()?;

        Ok(Self {
            source: source.parse().map_err(|e: String| miette::miette!("{e}"))?,
        })
    }
}

/// Where a `--source` points.
///
/// The scheme is what selects a restore path, which is why this is parsed
/// rather than sniffed: a directory that happens to look like a stele and a URL
/// that says it is one are different claims, and only the second is the
/// operator's.
///
/// Parsed by clap rather than inside [`run`], so an unusable source is refused
/// before `--force` has cleared anything. The flags that decide what to do with
/// existing data are handled a layer above this command, and a source rejected
/// any later would have cost the operator the node they still had.
#[derive(Debug, Clone)]
pub enum Source {
    /// A stele directory on this filesystem.
    Dir(PathBuf),
}

impl std::str::FromStr for Source {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        // `file:///abs/path` is the spelled-out form and leaves a leading slash
        // behind, which is the absolute path. `file://relative/path` is the one
        // an operator actually types, and leaves a relative one. Both work, and
        // neither is guessed at: what follows the scheme is the path.
        if let Some(path) = raw.strip_prefix("file://") {
            if path.is_empty() {
                return Err(format!("{raw:?} names no directory"));
            }

            return Ok(Self::Dir(PathBuf::from(path)));
        }

        if raw.starts_with("oci://") {
            return Err(
                "registry sources are not implemented yet; publish to a directory with \
                 `dolos snapshot publish --output-dir` and restore from it with \
                 `--source file://DIR`"
                    .to_owned(),
            );
        }

        Err(format!(
            "{raw:?} is not a stele source; the only scheme implemented today is `file://DIR`"
        ))
    }
}

fn restore_dir(config: &RootConfig, dir: &std::path::Path) -> miette::Result<()> {
    let root = crate::common::ensure_storage_path(config)
        .into_diagnostic()
        .context("creating the storage directory")?;

    let stores = crate::common::open_data_stores(config)
        .into_diagnostic()
        .context("opening the data stores")?;

    let genesis = crate::common::open_genesis_files(&config.genesis)?;

    let (plan, summary) = dolos_snapshot::restore::restore_dir(
        dir,
        u64::from(genesis.network_magic()),
        config.sync.max_history,
        &root,
        &stores.archive,
        &stores.state,
        &stores.indexes,
    )
    .into_diagnostic()
    .context("restoring the stele")?;

    println!(
        "network:  {} ({})",
        plan.position.network.name(),
        plan.position.network.magic()
    );
    println!("cursor:   {}", plan.position.point);
    println!("sequence: {}", plan.sequence);

    if plan.skipped_epochs > 0 {
        println!(
            "epochs:   {} restored, {} skipped by sync.max_history",
            plan.epochs.len(),
            plan.skipped_epochs,
        );
    } else {
        println!("epochs:   {}", plan.epochs.len());
    }

    println!(
        "restored: {} blocks, {} logs, {} index records, {} entities, {} utxos",
        summary.blocks, summary.logs, summary.index_records, summary.entities, summary.utxos,
    );

    Ok(())
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    match &args.source {
        Source::Dir(dir) => restore_dir(config, dir),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_source_names_a_directory() {
        for (raw, expected) in [
            ("file:///var/lib/dolos/stele", "/var/lib/dolos/stele"),
            ("file://stele", "stele"),
            ("file://./stele", "./stele"),
        ] {
            let Source::Dir(dir) = raw.parse::<Source>().unwrap();
            assert_eq!(dir, PathBuf::from(expected), "{raw:?}");
        }
    }

    #[test]
    fn an_unimplemented_or_unknown_scheme_is_refused() {
        for raw in [
            "oci://ghcr.io/txpipe/dolos-snapshots/mainnet",
            "https://example.invalid/snapshot",
            "/var/lib/dolos/stele",
            "file://",
            "",
        ] {
            assert!(raw.parse::<Source>().is_err(), "{raw:?}");
        }
    }
}
