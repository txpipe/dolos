use itertools::Itertools;
use miette::IntoDiagnostic;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraUpdate};
use std::{path::Path, sync::Arc};

use dolos::{
    ledger::{ChainPoint, EraCbor},
    wal::{redb::WalStore, RawBlock, ReadUtils, WalReader as _},
};

#[allow(dead_code)]
fn dump_txs(chain: &WalStore) -> miette::Result<()> {
    let blocks = chain
        .crawl_from(None)
        .into_diagnostic()?
        .filter_forward()
        .into_blocks()
        .flatten();

    for block in blocks {
        let RawBlock { slot, body, .. } = block;

        println!("dumping {slot}");

        let block = MultiEraBlock::decode(&body).into_diagnostic()?;

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

pub fn run(config: &crate::Config, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let genesis = Arc::new(crate::common::open_genesis_files(&config.genesis)?);
    let (wal, ledger, _) = crate::common::open_data_stores(config, &genesis)?;

    if let Some((seq, point)) = wal.crawl_from(None).unwrap().next() {
        println!("found WAL start");
        println!("seq: {seq}, point: {point:?}");
    } else {
        println!("WAL is empty");
    }

    if let Some((seq, point)) = wal.find_tip().unwrap() {
        println!("found WAL tip");
        println!("seq: {seq}, point: {point:?}");
    } else {
        println!("WAL is empty");
    }

    println!("---");

    if let Some(ChainPoint(slot, hash)) = ledger.cursor().unwrap() {
        println!("found ledger tip");
        println!("slot: {slot}, hash: {hash}");
    } else {
        println!("ledger is empty");
    }

    // println!("---");

    // let byron =
    // pallas::ledger::configs::byron::from_file(&config.byron.path).unwrap();
    // let shelley =
    // pallas::ledger::configs::shelley::from_file(&config.shelley.path).unwrap();
    // let alonzo =
    // pallas::ledger::configs::alonzo::from_file(&config.alonzo.path).unwrap();

    // let data: Vec<_> = ledger.get_pparams(46153027).into_diagnostic()?;

    // let updates = data
    //     .iter()
    //     .map(|PParamsBody(era, cbor)| -> miette::Result<MultiEraUpdate> {
    //         let era = Era::try_from(*era).into_diagnostic()?;
    //         MultiEraUpdate::decode_for_era(era, cbor).into_diagnostic()
    //     })
    //     .try_collect()?;

    // let merged = dolos::ledger::pparams::fold_pparams(
    //     Genesis {
    //         byron: &byron,
    //         shelley: &shelley,
    //         alonzo: &alonzo,
    //     },
    //     updates,
    //     500,
    // );

    //dbg!(merged);

    println!("---");

    let curr_point = ledger
        .cursor()
        .into_diagnostic()?
        .ok_or(miette::miette!("Uninitialized ledger."))?;

    let updates: Vec<_> = ledger.get_pparams(curr_point.0).into_diagnostic()?;

    let updates: Vec<_> = updates
        .iter()
        .map(|EraCbor(era, cbor)| MultiEraUpdate::decode_for_era(*era, cbor))
        .try_collect()
        .into_diagnostic()?;

    let genesis = crate::common::open_genesis_files(&config.genesis)?;

    let eras = dolos::ledger::pparams::fold(&genesis, &updates);

    println!("{:?}", eras);

    println!("---");

    for utxo in ledger
        .get_utxo_by_address(
            &hex::decode("6059184749e2d67a7ea2ca31ef48fc0befb3c3fad5a88af7264531ae07").unwrap(),
        )
        .into_diagnostic()?
    {
        println!("{}:{}", hex::encode(utxo.0), utxo.1)
    }

    // WIP utility to dump tx data for debugging purposes. Should be implemented as
    // a subcommand.

    // dump_txs(&chain)?;

    Ok(())
}
