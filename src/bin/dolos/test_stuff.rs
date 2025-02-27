use itertools::Itertools;
use std::sync::Arc;

use dolos::{
    ledger::pparams::{self, EraSummary},
    wal::{ChainPoint, ReadUtils, WalReader},
};
use pallas::{
    crypto::hash::Hash,
    ledger::traverse::{wellknown::GenesisValues, MultiEraBlock, MultiEraUpdate},
};

/// Resolve epoch, epoch slot and block time using Genesis values.
pub fn resolve_time_from_genesis(slot: &u64, summary: &EraSummary) -> (u64, u64, u64) {
    let era_slot = slot - summary.start.slot;
    let era_epoch = era_slot / summary.pparams.epoch_length();
    let epoch_slot = era_slot % summary.pparams.epoch_length();
    let epoch = summary.start.epoch + era_epoch;
    let time = summary.start.timestamp.timestamp() as u64
        + (slot - summary.start.slot) * summary.pparams.slot_length();
    (epoch, epoch_slot, time)
}

#[tokio::main]
pub async fn run(config: super::Config) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let slot = 4924780;

    let (wal, ledger) = crate::common::open_data_stores(&config)?;
    let genesis = Arc::new(crate::common::open_genesis_files(&config.genesis)?);

    let tip = ledger.cursor().unwrap();
    let rx = ledger.db().read_transaction()?;
    let updates = ledger
        .get_pparams_with_slot(tip.map(|t| t.0).unwrap_or_default())
        .unwrap()
        .into_iter()
        .map(|eracbor| MultiEraUpdate::try_from(eracbor).unwrap())
        .collect::<Vec<MultiEraUpdate>>();
    let summary = pparams::fold_with_hacks(&genesis, &updates, slot);

    let (epoch, epoch_slot, block_time) =
        resolve_time_from_genesis(&slot, summary.era_for_slot(slot));

    for update in updates.into_iter().sorted_by_key(|u| u.epoch()) {
        let new_version = update.all_proposed_protocol_version();
        let new_block = update.byron_proposed_block_version();
        if !new_version.is_empty() {
            println!("Update: {}", update.epoch());
            println!("version: {:?}", update.all_proposed_protocol_version());
        }

        if new_block.is_some() {
            println!("Update: {}", update.epoch());
            println!("version: {:?}", new_block);
        }
    }

    // println!("epoch: {}", epoch);
    // println!("epoch_slot: {}", epoch_slot);
    // println!("block_time: {}", block_time);

    // println!("epoch   version  slot");
    // for era in summary.past {
    //     println!(
    //         "{}      {}     {}",
    //         era.start.epoch,
    //         era.start.slot,
    //         era.pparams.protocol_version()
    //     );
    // }

    let first_208_block = ChainPoint::Specific(
        4492800,
        Hash::from(
            hex::decode("aa83acbf5904c0edfe4d79b3689d3d00fcfc553cf360fd2229b98d464c28e9de")
                .unwrap()
                .as_slice(),
        ),
    );
    let logseq = wal.locate_point(&first_208_block).unwrap();
    for maybe_raw in wal.crawl_from(logseq).unwrap().into_blocks() {
        if let Some(raw) = maybe_raw {
            let block = MultiEraBlock::decode(&raw.body).unwrap();
            if let Some(update) = block.update() {
                println!("Update found");
                println!("update epoch: {}", update.epoch());
                println!("block epoch: {:?}", block.epoch(&GenesisValues::mainnet()));
                println!("block slot: {:?}", block.slot());
                println!("raw slot: {:?}", raw.slot);
                break;
            }
        }
    }

    Ok(())
}
