use dolos::storage::{ArchiveStoreBackend, IndexStoreBackend, StateStoreBackend};
use dolos_core::config::RootConfig;
use dolos_redb3::TableFootprint;
use miette::{bail, IntoDiagnostic as _};
use serde_json::json;

#[derive(Debug, clap::Args)]
pub struct Args {}

fn footprint_to_json(footprint: &TableFootprint) -> serde_json::Value {
    let stats = &footprint.stats;

    json!({
        "rows": footprint.rows,
        "bytes_per_row": footprint.bytes_per_row(),
        "leaf_fill": footprint.leaf_fill(),
        "stored_bytes": stats.stored_bytes(),
        "fragmented_bytes": stats.fragmented_bytes(),
        "leaf_pages": stats.leaf_pages(),
        "metadata_bytes": stats.metadata_bytes(),
        "tree_height": stats.tree_height(),
        "branch_pages": stats.branch_pages(),
    })
}

pub fn run(config: &RootConfig, _args: &Args) -> miette::Result<()> {
    let stores = crate::common::open_data_stores(config)?;

    let mut json = serde_json::Map::new();

    // Every store is reported if and only if it is on redb; a backend that is
    // not gets left out rather than aborting the command. The live backend set
    // puts state and indexes on fjall and leaves the archive on redb, so
    // aborting on the first non-redb store would report nothing at all on a
    // normally configured node.
    if let StateStoreBackend::Redb(state) = &stores.state {
        let stats = state.utxoset_stats().into_diagnostic()?;

        json.insert("state".to_string(), section(stats));
    }

    if let IndexStoreBackend::Redb(indexes) = &stores.indexes {
        let stats = indexes.utxo_index_stats().into_diagnostic()?;

        json.insert("indexes".to_string(), section(stats));
    }

    match &stores.archive {
        ArchiveStoreBackend::Redb(archive) | ArchiveStoreBackend::LogsOnly(archive) => {
            let stats = archive.stats().into_diagnostic()?;

            let tables = stats
                .into_iter()
                .map(|(name, footprint)| (name, footprint_to_json(&footprint)));

            json.insert(
                "archive".to_string(),
                serde_json::Value::Object(tables.collect()),
            );
        }
        // The fjall archive's on-disk footprint is reported per keyspace by
        // `dolos data bench-archive`; redb's page-level TableStats have no
        // LSM equivalent here.
        ArchiveStoreBackend::Fjall(_) | ArchiveStoreBackend::NoOp(_) => (),
    }

    if json.is_empty() {
        bail!("stats command is only available for redb backends, none are configured");
    }

    println!("{}", serde_json::to_string_pretty(&json).unwrap());

    Ok(())
}

fn section<'a>(
    stats: impl IntoIterator<Item = (&'a str, dolos_redb3::redb::TableStats)>,
) -> serde_json::Value {
    let tables = stats.into_iter().map(|(name, stats)| {
        (
            name.to_string(),
            footprint_to_json(&TableFootprint::new(None, stats)),
        )
    });

    serde_json::Value::Object(tables.collect())
}
