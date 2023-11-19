use std::path::Path;

use miette::IntoDiagnostic;

use pallas::{
    ledger::traverse::{tx, MultiEraBlock},
    storage::rolldb::chain,
};

fn dump_txs(chain: &chain::Store) -> miette::Result<()> {
    for header in chain.crawl() {
        let (slot, hash) = header.into_diagnostic()?;
        println!("dumping {slot}");

        let block = chain.get_block(hash).into_diagnostic()?.unwrap();
        let block = MultiEraBlock::decode(&block).into_diagnostic()?;

        for tx in block.txs() {
            let cbor = hex::encode(tx.encode());
            let path = format!("{}.tx", tx.hash());
            std::fs::write(Path::new(&path), cbor).into_diagnostic()?;
        }
    }

    Ok(())
}

#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(config: &super::Config, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let (wal, chain, ledger) = crate::common::open_data_stores(config)?;

    if let Some((slot, hash)) = wal.find_tip().unwrap() {
        println!("found WAL tip");
        println!("slot: {slot}, hash: {hash}");
    } else {
        println!("WAL is empty");
    }

    println!("---");

    if let Some((slot, hash)) = chain.find_tip().unwrap() {
        println!("found chain tip");
        println!("slot: {slot}, hash: {hash}");
    } else {
        println!("chain is empty");
    }

    println!("---");

    if let Some((slot, hash)) = ledger.cursor().unwrap() {
        println!("found ledger tip");
        println!("slot: {slot}, hash: {hash}");
    } else {
        println!("chain is empty");
    }

    // WIP utility to dump tx data for debugging purposes. Should be implemented as
    // a subcommand.

    // dump_txs(&chain)?;

    Ok(())
}
