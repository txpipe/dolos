//! Blockfrost serves a synthetic genesis block per network. Dolos stores none,
//! so it is built here from the genesis config.

use axum::http::StatusCode;
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_core::{ArchiveStore as _, Domain};
use pallas::{
    interop::hardano::configs::{byron, shelley},
    ledger::traverse::MultiEraBlock,
};

use crate::Facade;

use super::{GENESIS_HASH_MAINNET, GENESIS_HASH_PREPROD, GENESIS_HASH_PREVIEW};

pub struct GenesisBlock {
    pub hash: &'static str,
    pub time: i32,
    pub next_block: &'static str,
}

const PREPROD: GenesisBlock = GenesisBlock {
    hash: GENESIS_HASH_PREPROD,
    time: 1654041600,
    next_block: "9ad7ff320c9cf74e0f5ee78d22a85ce42bb0a487d0506bf60cfb5a91ea4497d2",
};

const PREVIEW: GenesisBlock = GenesisBlock {
    hash: GENESIS_HASH_PREVIEW,
    time: 1666656000,
    next_block: "268ae601af8f9214804735910a3301881fbe0eec9936db7d1fb9fc39e93d1e37",
};

const MAINNET: GenesisBlock = GenesisBlock {
    hash: GENESIS_HASH_MAINNET,
    time: 1506203091,
    next_block: "89d9b5a5b8ddc8d7e5a6795e9774d97faf1efea59b2caf7eaf9f8c5b32059df4",
};

pub fn genesis_for_domain<D: Domain>(domain: &Facade<D>) -> Option<&'static GenesisBlock> {
    match domain.genesis().shelley.network_magic {
        Some(1) => Some(&PREPROD),
        Some(2) => Some(&PREVIEW),
        Some(764824073) => Some(&MAINNET),
        _ => None,
    }
}

pub fn is_genesis_hash<D: Domain>(domain: &Facade<D>, hash: &[u8]) -> Result<bool, StatusCode> {
    let Some(genesis) = genesis_for_domain(domain) else {
        return Ok(false);
    };

    let genesis_hash = hex::decode(genesis.hash).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(hash == genesis_hash.as_slice())
}

/// `None` on a network without a known genesis block.
pub fn genesis_block<D: Domain>(domain: &Facade<D>) -> Result<Option<BlockContent>, StatusCode> {
    let Some(genesis) = genesis_for_domain(domain) else {
        return Ok(None);
    };

    let (_, tip) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let confirmations = MultiEraBlock::decode(&tip)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .number() as i32;

    let byron_utxos = byron::genesis_utxos(&domain.genesis().byron);
    let shelley_utxos = shelley::shelley_utxos(&domain.genesis().shelley);

    let output = byron_utxos.iter().map(|(_, _, x)| *x).sum::<u64>()
        + shelley_utxos.iter().map(|(_, _, x)| *x).sum::<u64>();

    Ok(Some(BlockContent {
        time: genesis.time,
        height: None,
        hash: genesis.hash.to_string(),
        slot: None,
        epoch: None,
        epoch_slot: None,
        slot_leader: "Genesis slot leader".to_string(),
        size: 0,
        tx_count: (byron_utxos.len() + shelley_utxos.len()) as i32,
        output: Some(output.to_string()),
        fees: Some("0".to_string()),
        block_vrf: None,
        op_cert: None,
        op_cert_counter: None,
        previous_block: None,
        next_block: Some(genesis.next_block.to_string()),
        confirmations,
    }))
}

/// Blockfrost links the first real block back to the genesis block.
pub fn set_genesis_previous_block<D: Domain>(domain: &Facade<D>, block: &mut BlockContent) {
    if block.height.is_some_and(|x| x > 1) {
        return;
    }

    let Some(genesis) = genesis_for_domain(domain) else {
        return;
    };

    if block.hash == genesis.hash {
        return;
    }

    if block.previous_block.is_none() {
        block.previous_block = Some(genesis.hash.to_string());
    }
}
