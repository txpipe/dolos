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

pub fn run(config: &crate::Config, _args: &Args) -> miette::Result<()> {
    let stores = crate::common::setup_data_stores(config)?;

    let state = stores.state;

    let stats = state.utxoset_stats().unwrap();

    let mut json = serde_json::Map::new();

    stats.iter().for_each(|(key, value)| {
        json.insert(key.to_string(), stats_to_json(value));
    });

    println!("{}", serde_json::to_string_pretty(&json).unwrap());

    Ok(())
}
