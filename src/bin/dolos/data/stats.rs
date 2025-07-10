use miette::IntoDiagnostic;
use serde_json::json;

#[derive(Debug, clap::Args)]
pub struct Args {}

fn stats_to_json(stats: &dolos_redb::redb::TableStats) -> serde_json::Value {
    json!({
        "stored_bytes": stats.stored_bytes(),
        "fragmented_bytes": stats.fragmented_bytes(),
        "leaf_pages": stats.leaf_pages(),
        "metadata_bytes": stats.metadata_bytes(),
        "tree_height": stats.tree_height(),
        "branch_pages": stats.branch_pages(),
    })
}

pub fn run(config: &crate::Config, _args: &Args) -> miette::Result<()> {
    let (_, state, _) = crate::common::setup_data_stores(config)?;

    let state: dolos_redb::state::LedgerStore = state.try_into().into_diagnostic()?;

    let stats = state.stats().unwrap();

    let mut json = serde_json::Map::new();

    stats.iter().for_each(|(key, value)| {
        json.insert(key.to_string(), stats_to_json(value));
    });

    println!("{}", serde_json::to_string_pretty(&json).unwrap());

    Ok(())
}
