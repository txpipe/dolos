use std::path::Path;

use dolos::ledger::{pparams::Genesis, ChainPoint, PParamsBody};
use itertools::Itertools;
use miette::IntoDiagnostic;

use pallas::{
    ledger::traverse::{Era, MultiEraBlock, MultiEraUpdate},
    storage::rolldb::chain,
};

#[allow(dead_code)]
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

    if let Some(ChainPoint(slot, hash)) = ledger.cursor().unwrap() {
        println!("found ledger tip");
        println!("slot: {slot}, hash: {hash}");
    } else {
        println!("chain is empty");
    }

    println!("---");

    let byron = pallas::ledger::configs::byron::from_file(&config.byron.path).unwrap();
    let shelley = pallas::ledger::configs::shelley::from_file(&config.shelley.path).unwrap();
    let alonzo = pallas::ledger::configs::alonzo::from_file(&config.alonzo.path).unwrap();

    let data: Vec<_> = ledger.get_pparams(46153027).into_diagnostic()?;

    let updates = data
        .iter()
        .map(|PParamsBody(era, cbor)| -> miette::Result<MultiEraUpdate> {
            let era = Era::try_from(*era).into_diagnostic()?;
            MultiEraUpdate::decode_for_era(era, cbor).into_diagnostic()
        })
        .try_collect()?;

    let merged = dolos::ledger::pparams::fold_pparams(
        Genesis {
            byron: &byron,
            shelley: &shelley,
            alonzo: &alonzo,
        },
        updates,
        500,
    );

    dbg!(merged);

    // WIP utility to dump tx data for debugging purposes. Should be implemented as
    // a subcommand.

    // dump_txs(&chain)?;

    Ok(())
}
