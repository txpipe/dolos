use axum::{Json, http::StatusCode};
use blockfrost_openapi::models::{
    tx_content::TxContent, tx_content_cbor::TxContentCbor,
    tx_content_output_amount_inner::TxContentOutputAmountInner,
};
use dolos_cardano::pparams::ChainSummary;
use dolos_core::{BlockBody, TxOrder};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput};
use std::collections::HashMap;

pub type BlockAndTxOrder = (BlockBody, TxOrder);

macro_rules! try_into_or_500 {
    ($expr:expr) => {
        $expr
            .try_into()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    };
}

pub struct Context {
    pub pparams: ChainSummary,
}

pub trait IntoModel<T>
where
    T: serde::Serialize,
    Self: Sized,
{
    fn into_model(self, ctx: &Context) -> Result<T, StatusCode>;

    fn into_response(self, ctx: &Context) -> Result<Json<T>, StatusCode> {
        let tx = self.into_model(ctx)?;

        Ok(Json(tx))
    }
}

/// Resolve epoch, epoch slot and block time using Genesis values.
pub fn slot_time(slot: u64, summary: &ChainSummary) -> (u64, u64, u64) {
    let era = summary.era_for_slot(slot);

    let era_slot = slot - era.start.slot;
    let era_epoch = era_slot / era.pparams.epoch_length();
    let epoch_slot = era_slot % era.pparams.epoch_length();
    let epoch = era.start.epoch + era_epoch;
    let time = era.start.timestamp.timestamp() as u64
        + (slot - era.start.slot) * era.pparams.slot_length();

    (epoch, epoch_slot, time)
}

pub fn aggregate_amount<'a>(
    txouts: impl Iterator<Item = &'a MultiEraOutput<'a>>,
) -> Vec<TxContentOutputAmountInner> {
    let mut lovelace = 0;
    let mut by_asset: HashMap<String, u64> = HashMap::new();

    for txout in txouts {
        let value = txout.value();

        // Add lovelace amount
        lovelace += value.coin();

        // Add other assets
        for ma in value.assets() {
            for asset in ma.assets() {
                let unit = format!("{}{}", ma.policy(), hex::encode(asset.name()));
                let amount = asset.output_coin().unwrap_or_default();
                *by_asset.entry(unit).or_insert(0) += amount;
            }
        }
    }

    let lovelace = TxContentOutputAmountInner {
        unit: "lovelace".to_string(),
        quantity: lovelace.to_string(),
    };

    let mut assets: Vec<_> = by_asset
        .into_iter()
        .map(|(unit, quantity)| TxContentOutputAmountInner {
            unit,
            quantity: quantity.to_string(),
        })
        .collect();

    assets.sort_by_key(|a| a.unit.clone());

    std::iter::once(lovelace).chain(assets).collect()
}

impl IntoModel<TxContent> for BlockAndTxOrder {
    fn into_model(self, ctx: &Context) -> Result<TxContent, StatusCode> {
        let (block, tx_order) = self;

        let block = MultiEraBlock::decode(&block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let txs = block.txs();

        let tx = txs.get(tx_order).ok_or(StatusCode::NOT_FOUND)?;

        let txin = tx.inputs();

        let txouts = tx.outputs();

        let (_, _, block_time) = slot_time(block.slot(), &ctx.pparams);

        let tx = TxContent {
            hash: tx.hash().to_string(),
            block: block.hash().to_string(),
            block_height: try_into_or_500!(block.number()),
            slot: try_into_or_500!(block.slot()),
            index: try_into_or_500!(tx_order),
            output_amount: aggregate_amount(txouts.iter()),
            fees: tx.fee().map(|f| f.to_string()).unwrap_or_default(),
            size: try_into_or_500!(tx.size()),
            invalid_before: tx.validity_start().map(|v| v.to_string()),
            invalid_hereafter: tx.ttl().map(|v| v.to_string()),
            utxo_count: try_into_or_500!(txin.len() + txouts.len()),
            redeemer_count: try_into_or_500!(tx.redeemers().len()),
            valid_contract: tx.is_valid(),
            block_time: try_into_or_500!(block_time),
            withdrawal_count: 0,         // TODO
            mir_cert_count: 0,           // TODO
            delegation_count: 0,         // TODO
            stake_cert_count: 0,         // TODO
            pool_update_count: 0,        // TODO
            pool_retire_count: 0,        // TODO
            asset_mint_or_burn_count: 0, // TODO
            deposit: "0".to_string(),    // TODO
        };

        Ok(tx)
    }
}

impl IntoModel<TxContentCbor> for BlockAndTxOrder {
    fn into_model(self, _: &Context) -> Result<TxContentCbor, StatusCode> {
        let (block, tx_order) = self;

        let block = MultiEraBlock::decode(&block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let txs = block.txs();

        let tx = txs.get(tx_order).ok_or(StatusCode::NOT_FOUND)?;

        let cbor = tx.encode();

        let tx = TxContentCbor {
            cbor: hex::encode(&cbor),
        };

        Ok(tx)
    }
}
