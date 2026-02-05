use axum::http::StatusCode;
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_core::{ArchiveStore as _, Domain};
use pallas::ledger::{
    configs::{byron, shelley},
    traverse::MultiEraBlock,
};

use crate::Facade;

pub const GENESIS_HASH_PREVIEW: &str =
    "83de1d7302569ad56cf9139a41e2e11346d4cb4a31c00142557b6ab3fa550761";
pub const GENESIS_HASH_PREPROD: &str =
    "d4b8de7a11d929a323373cbab6c1a9bdc931beffff11db111cf9d57356ee1937";
pub const GENESIS_HASH_MAINNET: &str =
    "5f20df933584822601f9e3f8c024eb5eb252fe8cefb24d1317dc3d432e940ebb";

pub fn genesis_hash_for_domain<D: Domain>(domain: &Facade<D>) -> Option<&'static str> {
    match domain.genesis().shelley.network_magic {
        Some(1) => Some(GENESIS_HASH_PREPROD),
        Some(2) => Some(GENESIS_HASH_PREVIEW),
        Some(764824073) => Some(GENESIS_HASH_MAINNET),
        _ => None,
    }
}

pub fn is_genesis_hash_for_domain<D: Domain>(
    domain: &Facade<D>,
    hash: &[u8],
) -> Result<bool, StatusCode> {
    let Some(genesis_hash) = genesis_hash_for_domain(domain) else {
        return Ok(false);
    };

    let genesis_hash = hex::decode(genesis_hash).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(hash == genesis_hash.as_slice())
}

pub fn genesis_block_for_domain<D: Domain>(
    domain: &Facade<D>,
) -> Result<Option<BlockContent>, StatusCode> {
    match domain.genesis().shelley.network_magic {
        Some(1) => Ok(Some(genesis_block_preprod(domain)?)),
        Some(2) => Ok(Some(genesis_block_preview(domain)?)),
        Some(764824073) => Ok(Some(genesis_block_mainnet(domain)?)),
        _ => Ok(None),
    }
}

pub fn genesis_block_preview<D: Domain>(domain: &Facade<D>) -> Result<BlockContent, StatusCode> {
    let confirmations = MultiEraBlock::decode(
        &domain
            .archive()
            .get_tip()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?
            .1,
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .header()
    .number() as i32;

    let byron_utxos = byron::genesis_utxos(&domain.genesis().byron);
    let shelley_utxos = shelley::shelley_utxos(&domain.genesis().shelley);

    Ok(BlockContent {
        time: 1666656000,
        height: None,
        hash: GENESIS_HASH_PREVIEW.to_string(),
        slot: None,
        epoch: None,
        epoch_slot: None,
        slot_leader: "Genesis slot leader".to_string(),
        size: 0,
        tx_count: (byron_utxos.len() + shelley_utxos.len()) as i32,
        output: Some(
            (byron_utxos.iter().map(|(_, _, x)| *x).sum::<u64>()
                + shelley_utxos.iter().map(|(_, _, x)| *x).sum::<u64>())
            .to_string(),
        ),
        fees: Some("0".to_string()),
        block_vrf: None,
        op_cert: None,
        op_cert_counter: None,
        previous_block: None,
        next_block: Some(
            "268ae601af8f9214804735910a3301881fbe0eec9936db7d1fb9fc39e93d1e37".to_string(),
        ),
        confirmations,
    })
}

pub fn genesis_block_preprod<D: Domain>(domain: &Facade<D>) -> Result<BlockContent, StatusCode> {
    let confirmations = MultiEraBlock::decode(
        &domain
            .archive()
            .get_tip()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?
            .1,
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .header()
    .number() as i32;

    let byron_utxos = byron::genesis_utxos(&domain.genesis().byron);
    let shelley_utxos = shelley::shelley_utxos(&domain.genesis().shelley);

    Ok(BlockContent {
        time: 1654041600,
        height: None,
        hash: GENESIS_HASH_PREPROD.to_string(),
        slot: None,
        epoch: None,
        epoch_slot: None,
        slot_leader: "Genesis slot leader".to_string(),
        size: 0,
        tx_count: (byron_utxos.len() + shelley_utxos.len()) as i32,
        output: Some(
            (byron_utxos.iter().map(|(_, _, x)| *x).sum::<u64>()
                + shelley_utxos.iter().map(|(_, _, x)| *x).sum::<u64>())
            .to_string(),
        ),
        fees: Some("0".to_string()),
        block_vrf: None,
        op_cert: None,
        op_cert_counter: None,
        previous_block: None,
        next_block: Some(
            "9ad7ff320c9cf74e0f5ee78d22a85ce42bb0a487d0506bf60cfb5a91ea4497d2".to_string(),
        ),
        confirmations,
    })
}

pub fn genesis_block_mainnet<D: Domain>(domain: &Facade<D>) -> Result<BlockContent, StatusCode> {
    let confirmations = MultiEraBlock::decode(
        &domain
            .archive()
            .get_tip()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?
            .1,
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .header()
    .number() as i32;

    let byron_utxos = byron::genesis_utxos(&domain.genesis().byron);
    let shelley_utxos = shelley::shelley_utxos(&domain.genesis().shelley);

    Ok(BlockContent {
        time: 1506203091,
        height: None,
        hash: GENESIS_HASH_MAINNET.to_string(),
        slot: None,
        epoch: None,
        epoch_slot: None,
        slot_leader: "Genesis slot leader".to_string(),
        size: 0,
        tx_count: (byron_utxos.len() + shelley_utxos.len()) as i32,
        output: Some(
            (byron_utxos.iter().map(|(_, _, x)| *x).sum::<u64>()
                + shelley_utxos.iter().map(|(_, _, x)| *x).sum::<u64>())
            .to_string(),
        ),
        fees: Some("0".to_string()),
        block_vrf: None,
        op_cert: None,
        op_cert_counter: None,
        previous_block: None,
        next_block: Some("89d9b5a5b8ddc8d7e5a6795e9774d97faf1efea59b2caf7eaf9f8c5b32059df4".to_string()),
        confirmations,
    })
}

pub fn maybe_set_genesis_previous_block<D: Domain>(domain: &Facade<D>, block: &mut BlockContent) {
    if block.height.is_some_and(|x| x > 1) {
        return;
    }

    let Some(genesis_hash) = genesis_hash_for_domain(domain) else {
        return;
    };

    if block.hash == genesis_hash {
        return;
    }

    if block.previous_block.is_none() {
        block.previous_block = Some(genesis_hash.to_string());
    }
}
