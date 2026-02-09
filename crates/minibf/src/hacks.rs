use axum::http::StatusCode;
use blockfrost_openapi::models::tx_content_utxo::TxContentUtxo;
use blockfrost_openapi::models::{block_content::BlockContent, tx_content::TxContent};
use dolos_cardano::indexes::AsyncCardanoQueryExt;
use dolos_core::{ArchiveStore as _, Domain, TxoRef};
use pallas::crypto::hash::Hash;
use pallas::ledger::{
    configs::{byron, shelley},
    primitives::{alonzo, byron as byron_primitives, conway},
    traverse::MultiEraBlock,
    traverse::MultiEraOutput,
};

use crate::mapping::{IntoModel, UtxoOutputModelBuilder};
use crate::Facade;

pub const GENESIS_HASH_PREVIEW: &str =
    "83de1d7302569ad56cf9139a41e2e11346d4cb4a31c00142557b6ab3fa550761";
pub const GENESIS_HASH_PREPROD: &str =
    "d4b8de7a11d929a323373cbab6c1a9bdc931beffff11db111cf9d57356ee1937";
pub const GENESIS_HASH_MAINNET: &str =
    "5f20df933584822601f9e3f8c024eb5eb252fe8cefb24d1317dc3d432e940ebb";

pub struct GenesisBlockMetadata {
    pub hash: &'static str,
    pub time: i32,
}

enum GenesisOutputBody<'a> {
    Byron(byron_primitives::TxOut),
    Shelley(Box<conway::TransactionOutput<'a>>),
}

impl<'a> GenesisOutputBody<'a> {
    fn as_output(&self) -> MultiEraOutput<'_> {
        match self {
            GenesisOutputBody::Byron(body) => MultiEraOutput::from_byron(body),
            GenesisOutputBody::Shelley(body) => MultiEraOutput::from_conway(body.as_ref()),
        }
    }
}

struct GenesisTxOutput<'a> {
    tx_hash: Hash<32>,
    output: GenesisOutputBody<'a>,
}

impl<'a> GenesisTxOutput<'a> {
    fn as_output(&self) -> MultiEraOutput<'_> {
        self.output.as_output()
    }
}

struct GenesisTxModel<'a> {
    block: GenesisBlockMetadata,
    output: GenesisTxOutput<'a>,
    consumed_by: Option<Hash<32>>,
}

pub fn genesis_hash_for_domain<D: Domain>(domain: &Facade<D>) -> Option<&'static str> {
    match domain.genesis().shelley.network_magic {
        Some(1) => Some(GENESIS_HASH_PREPROD),
        Some(2) => Some(GENESIS_HASH_PREVIEW),
        Some(764824073) => Some(GENESIS_HASH_MAINNET),
        _ => None,
    }
}

pub fn genesis_block_metadata_for_domain<D: Domain>(
    domain: &Facade<D>,
) -> Option<GenesisBlockMetadata> {
    match domain.genesis().shelley.network_magic {
        Some(1) => Some(GenesisBlockMetadata {
            hash: GENESIS_HASH_PREPROD,
            time: 1654041600,
        }),
        Some(2) => Some(GenesisBlockMetadata {
            hash: GENESIS_HASH_PREVIEW,
            time: 1666656000,
        }),
        Some(764824073) => Some(GenesisBlockMetadata {
            hash: GENESIS_HASH_MAINNET,
            time: 1506203091,
        }),
        _ => None,
    }
}

fn genesis_tx_output_by_hash<D: Domain>(
    domain: &Facade<D>,
    hash: &[u8],
) -> Result<Option<GenesisTxOutput<'static>>, StatusCode> {
    let byron_utxos = byron::genesis_utxos(&domain.genesis().byron);
    for (tx, addr, amount) in byron_utxos {
        if tx.as_slice() == hash {
            let utxo_body = byron_primitives::TxOut {
                address: byron_primitives::Address {
                    payload: addr.payload,
                    crc: addr.crc,
                },
                amount,
            };
            return Ok(Some(GenesisTxOutput {
                tx_hash: tx,
                output: GenesisOutputBody::Byron(utxo_body),
            }));
        }
    }

    let shelley_utxos = shelley::shelley_utxos(&domain.genesis().shelley);
    for (tx, addr, amount) in shelley_utxos {
        if tx.as_slice() == hash {
            let utxo_body = alonzo::TransactionOutput {
                address: addr.to_vec().into(),
                amount: alonzo::Value::Coin(amount),
                datum_hash: None,
            };
            let utxo_body = conway::TransactionOutput::Legacy(utxo_body.into());
            return Ok(Some(GenesisTxOutput {
                tx_hash: tx,
                output: GenesisOutputBody::Shelley(Box::new(utxo_body)),
            }));
        }
    }

    Ok(None)
}

impl<'a> GenesisTxModel<'a> {
    fn new(block: GenesisBlockMetadata, output: GenesisTxOutput<'a>) -> Self {
        Self {
            block,
            output,
            consumed_by: None,
        }
    }

    fn with_consumed_by(self, consumed_by: Hash<32>) -> Self {
        Self {
            consumed_by: Some(consumed_by),
            ..self
        }
    }
}

impl<'a> IntoModel<TxContent> for GenesisTxModel<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<TxContent, StatusCode> {
        let output = self.output.as_output();
        let output_amount = output.value().into_model()?;

        Ok(TxContent {
            hash: self.output.tx_hash.to_string(),
            block: self.block.hash.to_string(),
            block_height: 0,
            block_time: self.block.time,
            slot: 0,
            index: 0,
            output_amount,
            fees: "0".to_string(),
            deposit: "0".to_string(),
            size: 0,
            invalid_before: None,
            invalid_hereafter: None,
            utxo_count: 1,
            withdrawal_count: 0,
            mir_cert_count: 0,
            delegation_count: 0,
            stake_cert_count: 0,
            pool_update_count: 0,
            pool_retire_count: 0,
            asset_mint_or_burn_count: 0,
            redeemer_count: 0,
            valid_contract: true,
        })
    }
}

impl<'a> IntoModel<TxContentUtxo> for GenesisTxModel<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<TxContentUtxo, StatusCode> {
        let output = self.output.as_output();
        let builder = UtxoOutputModelBuilder::from_output(self.output.tx_hash, 0, output);
        let builder = if let Some(consumed_by) = self.consumed_by {
            builder.with_consumed_by(consumed_by)
        } else {
            builder
        };
        let output = builder.into_model()?;

        Ok(TxContentUtxo {
            hash: self.output.tx_hash.to_string(),
            inputs: Vec::new(),
            outputs: vec![output],
        })
    }
}

pub fn genesis_tx_content_for_hash<D: Domain>(
    domain: &Facade<D>,
    hash: &[u8],
) -> Result<TxContent, StatusCode> {
    let Some(block_meta) = genesis_block_metadata_for_domain(domain) else {
        return Err(StatusCode::NOT_FOUND);
    };

    let Some(output) = genesis_tx_output_by_hash(domain, hash)? else {
        return Err(StatusCode::NOT_FOUND);
    };

    GenesisTxModel::new(block_meta, output).into_model()
}

pub async fn genesis_tx_utxos_for_hash<D>(
    domain: &Facade<D>,
    hash: &[u8],
) -> Result<TxContentUtxo, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let Some(block_meta) = genesis_block_metadata_for_domain(domain) else {
        return Err(StatusCode::NOT_FOUND);
    };

    let Some(output) = genesis_tx_output_by_hash(domain, hash)? else {
        return Err(StatusCode::NOT_FOUND);
    };

    let key: Vec<u8> = TxoRef(output.tx_hash, 0).into();
    let consumed_by = domain
        .query()
        .tx_by_spent_txo(&key)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let model = if let Some(consumed_by) = consumed_by {
        GenesisTxModel::new(block_meta, output).with_consumed_by(consumed_by)
    } else {
        GenesisTxModel::new(block_meta, output)
    };

    model.into_model()
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
        next_block: Some(
            "89d9b5a5b8ddc8d7e5a6795e9774d97faf1efea59b2caf7eaf9f8c5b32059df4".to_string(),
        ),
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
