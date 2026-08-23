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

    // Each engine reports the shape it actually has: redb gets per-table
    // B-tree footprints (rows, leaf fill, page counts), fjall gets per-keyspace
    // disk footprints — LSM trees have no leaf-fill analogue and faking one
    // would mislead. Stores on neither engine are left out rather than
    // aborting the command.
    match &stores.state {
        StateStoreBackend::Redb(state) => {
            let stats = state.utxoset_stats().into_diagnostic()?;

            json.insert("state".to_string(), redb_section(stats));
        }
        StateStoreBackend::Fjall(state) => {
            json.insert("state".to_string(), fjall_section(state.disk_usage()));
        }
        StateStoreBackend::Memory(_) => (),
    }

    match &stores.indexes {
        IndexStoreBackend::Redb(indexes) => {
            let stats = indexes.utxo_index_stats().into_diagnostic()?;

            json.insert("indexes".to_string(), redb_section(stats));
        }
        IndexStoreBackend::Fjall(indexes) => {
            json.insert("indexes".to_string(), fjall_section(indexes.disk_usage()));
        }
        IndexStoreBackend::Memory(_) | IndexStoreBackend::NoOp(_) => (),
    }

    match &stores.archive {
        ArchiveStoreBackend::Redb(archive) => {
            let stats = archive.stats().into_diagnostic()?;

            let tables = stats
                .into_iter()
                .map(|(name, footprint)| (name, footprint_to_json(&footprint)))
                .collect();

            json.insert(
                "archive".to_string(),
                json!({ "engine": "redb", "tables": serde_json::Value::Object(tables) }),
            );
        }
        ArchiveStoreBackend::Fjall(archive) => {
            json.insert("archive".to_string(), fjall_section(archive.disk_usage()));
        }
        ArchiveStoreBackend::LogsOnly(_) | ArchiveStoreBackend::NoOp(_) => (),
    }

    if json.is_empty() {
        bail!("no persistent store backends are configured, nothing to report");
    }

    println!("{}", serde_json::to_string_pretty(&json).unwrap());

    Ok(())
}

fn redb_section<'a>(
    stats: impl IntoIterator<Item = (&'a str, dolos_redb3::redb::TableStats)>,
) -> serde_json::Value {
    let tables: serde_json::Map<_, _> = stats
        .into_iter()
        .map(|(name, stats)| {
            (
                name.to_string(),
                footprint_to_json(&TableFootprint::new(None, stats)),
            )
        })
        .collect();

    json!({ "engine": "redb", "tables": serde_json::Value::Object(tables) })
}

fn fjall_section(usage: Vec<(&'static str, u64, std::path::PathBuf)>) -> serde_json::Value {
    let keyspaces: serde_json::Map<_, _> = usage
        .into_iter()
        .map(|(name, bytes, path)| {
            (
                name.to_string(),
                json!({ "disk_bytes": bytes, "path": path.display().to_string() }),
            )
        })
        .collect();

    json!({ "engine": "fjall", "keyspaces": serde_json::Value::Object(keyspaces) })
}
