use dolos::storage::{IndexStoreBackend, StateStoreBackend};
use dolos_core::config::RootConfig;
use miette::bail;
use serde_json::json;

#[derive(Debug, clap::Args)]
pub struct Args {}

fn stats_to_json(stats: &dolos_redb3::redb::TableStats) -> serde_json::Value {
    json!({
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

    // Stats command only works with redb backends
    let state = match &stores.state {
        StateStoreBackend::Redb(s) => s,
        StateStoreBackend::Fjall(_) => {
            bail!("stats command is only available for redb state backend")
        }
    };

    let indexes = match &stores.indexes {
        IndexStoreBackend::Redb(s) => s,
        IndexStoreBackend::Fjall(_) => {
            bail!("stats command is only available for redb index backend")
        }
        IndexStoreBackend::NoOp(_) => {
            bail!("stats command is not available for noop index backend")
        }
    };

    let mut stats = state.utxoset_stats().unwrap();
    let index_stats = indexes.utxo_index_stats().unwrap();
    stats.extend(index_stats);

    let mut json = serde_json::Map::new();

    stats.iter().for_each(|(key, value)| {
        json.insert(key.to_string(), stats_to_json(value));
    });

    println!("{}", serde_json::to_string_pretty(&json).unwrap());

    Ok(())
}
