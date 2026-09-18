use axum::http::StatusCode;
use blockfrost_openapi::models::tx_content::TxContent;
use blockfrost_openapi::models::tx_content_utxo::TxContentUtxo;
use dolos_cardano::indexes::AsyncCardanoQueryExt;
use dolos_core::{Domain, TxoRef};
use pallas::crypto::hash::Hash;
use pallas::interop::hardano::configs::{byron, shelley};
use pallas::ledger::{
    primitives::{alonzo, byron as byron_primitives, conway},
    traverse::MultiEraOutput,
};

use crate::mapping::{IntoModel, UtxoOutputModelBuilder};
use crate::Facade;

pub mod genesis_block;

use genesis_block::GenesisBlock;

pub const GENESIS_HASH_PREVIEW: &str =
    "83de1d7302569ad56cf9139a41e2e11346d4cb4a31c00142557b6ab3fa550761";
pub const GENESIS_HASH_PREPROD: &str =
    "d4b8de7a11d929a323373cbab6c1a9bdc931beffff11db111cf9d57356ee1937";
pub const GENESIS_HASH_MAINNET: &str =
    "5f20df933584822601f9e3f8c024eb5eb252fe8cefb24d1317dc3d432e940ebb";

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
    block: &'static GenesisBlock,
    output: GenesisTxOutput<'a>,
    consumed_by: Option<Hash<32>>,
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
    fn new(block: &'static GenesisBlock, output: GenesisTxOutput<'a>) -> Self {
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
            treasury_donation: "0".to_string(),
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
    let Some(block_meta) = genesis_block::genesis_for_domain(domain) else {
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
    let Some(block_meta) = genesis_block::genesis_for_domain(domain) else {
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

/// This module lists the epochs where the reference indexer reports a `null`
/// active stake, but Dolos computes a genesis-derived value. The reference
/// indexer is db-sync, which the Blockfrost API uses.
///
/// The minibf epoch mapper sums the per-pool `StakeLog`s to report the active
/// stake of a historical epoch. Dolos writes these logs from epoch
/// `first_shelley_epoch + 3` and later (see
/// `dolos_cardano::rupd::loading::RupdWork::relevant_epochs`). A look-ahead
/// makes Dolos report the sum one epoch earlier. Thus Dolos gives a value
/// continuously from that first epoch.
///
/// The early history of preprod has a gap in the stake snapshot. The reference
/// shows this gap, but Dolos does not. The reference reports active stake for
/// epochs 6-12. It reports `null` for epochs 13-28. It reports real values from
/// epoch 29 and later.
///
/// Epochs 0-5 have no `StakeLog` and already read `null`. Thus only epochs
/// 13-28 need an override.
///
/// A test against the live Blockfrost API shows this pattern. Preprod epoch 12
/// has a value. Epochs 13-28 are `null`. Epoch 29 has a value again.
///
/// Preview and mainnet have no such gap. Preview gives its first value at epoch
/// 2. Mainnet gives its first value at epoch 210. Thus their natural output
/// already matches the reference.
pub mod null_active_stake {
    use pallas::ledger::primitives::Epoch;

    /// This function returns `true` for an epoch that the reference reports as
    /// `null`, but Dolos holds a value.
    pub fn contains(magic: u32, epoch: Epoch) -> bool {
        match magic {
            1 => (13..=28).contains(&epoch), // preprod early stake-snapshot gap
            _ => false,
        }
    }
}
