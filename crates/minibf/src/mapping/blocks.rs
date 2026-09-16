use std::collections::{BTreeMap, BTreeSet};

use axum::http::StatusCode;
use blockfrost_openapi::models::{
    block_content::BlockContent, block_content_addresses_inner::BlockContentAddressesInner,
    block_content_addresses_inner_transactions_inner::BlockContentAddressesInnerTransactionsInner,
    block_content_txs_cbor_inner::BlockContentTxsCborInner,
};
use dolos_cardano::ChainSummary;
use itertools::Itertools as _;
use pallas::{
    crypto::hash::{Hash, Hasher},
    ledger::traverse::{MultiEraBlock, MultiEraHeader, MultiEraTx},
};

use super::{bech32_pool, IntoModel};

pub struct BlockModelBuilder<'a> {
    block: MultiEraBlock<'a>,
    chain: Option<&'a ChainSummary>,
    previous: Option<MultiEraBlock<'a>>,
    next: Option<MultiEraBlock<'a>>,
    tip: Option<MultiEraBlock<'a>>,
    touched_addresses: Option<Vec<(String, BTreeSet<String>)>>,
}

impl<'a> BlockModelBuilder<'a> {
    pub fn new(block: &'a [u8]) -> Result<Self, StatusCode> {
        let block = MultiEraBlock::decode(block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(Self {
            block,
            previous: None,
            next: None,
            tip: None,
            chain: None,
            touched_addresses: None,
        })
    }

    pub fn txs(&self) -> Vec<MultiEraTx<'_>> {
        self.block.txs()
    }

    /// Collect the addresses each tx touches by asking `collect` for every tx
    /// of the block, in block order. The builder drives the walk itself, so
    /// the sets cannot fall out of sync with the txs they describe.
    pub fn collect_touched_addresses_with<F>(mut self, mut collect: F) -> Result<Self, StatusCode>
    where
        F: FnMut(&MultiEraTx<'_>) -> Result<BTreeSet<String>, StatusCode>,
    {
        self.touched_addresses = Some(
            self.block
                .txs()
                .iter()
                .map(|tx| collect(tx).map(|addresses| (tx.hash().to_string(), addresses)))
                .try_collect()?,
        );

        Ok(self)
    }

    pub fn with_chain(self, chain: &'a ChainSummary) -> Self {
        Self {
            chain: Some(chain),
            ..self
        }
    }

    pub fn with_previous(self, previous: &'a [u8]) -> Result<Self, StatusCode> {
        let previous =
            MultiEraBlock::decode(previous).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(Self {
            previous: Some(previous),
            ..self
        })
    }

    pub fn with_next(self, next: &'a [u8]) -> Result<Self, StatusCode> {
        let next = MultiEraBlock::decode(next).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(Self {
            next: Some(next),
            ..self
        })
    }

    pub fn with_tip(self, tip: &'a [u8]) -> Result<Self, StatusCode> {
        let tip = MultiEraBlock::decode(tip).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(Self {
            tip: Some(tip),
            ..self
        })
    }

    pub fn previous_hash(&self) -> Option<Hash<32>> {
        self.block.header().previous_hash()
    }

    pub fn next_number(&self) -> u64 {
        self.block.number() + 1
    }

    fn format_block_vrf(&self) -> Result<Option<String>, StatusCode> {
        let header = self.block.header();

        let Some(key) = header.vrf_vkey() else {
            return Ok(None);
        };

        let hrp = bech32::Hrp::parse("vrf_vk").map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let out = bech32::encode::<bech32::Bech32>(hrp, key)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(Some(out))
    }

    fn format_slot_leader(&self) -> Result<Option<String>, StatusCode> {
        let header = self.block.header();

        let Some(use_bech32) = self.chain.map(|x| {
            let epoch = x.slot_epoch(self.block.slot()).0;
            epoch > x.first_shelley_epoch()
        }) else {
            return Ok(None);
        };

        match header.issuer_vkey() {
            Some(key) => {
                let hash: Hash<28> = Hasher::<224>::hash(key);

                if use_bech32 {
                    Ok(Some(bech32_pool(hash)?))
                } else {
                    Ok(Some(format!(
                        "ShelleyGenesis-{}",
                        hex::encode(hash.as_slice().first_chunk::<8>().unwrap())
                    )))
                }
            }
            None => match header {
                MultiEraHeader::Byron(byron_header) => {
                    let hash: Hash<32> =
                        Hasher::<256>::hash(&byron_header.consensus_data.1.as_slice()[..32]);

                    Ok(Some(format!(
                        "ByronGenesis-{}",
                        hex::encode(hash.as_slice().first_chunk::<8>().unwrap())
                    )))
                }
                MultiEraHeader::EpochBoundary(_) => {
                    Ok(Some("Epoch boundary slot leader".to_string()))
                }
                _ => unreachable!(), // Covered on Some case
            },
        }
    }

    fn format_ops_cert_data(&self) -> (Option<String>, Option<String>) {
        let header = self.block.header();

        match header {
            MultiEraHeader::ShelleyCompatible(x) => (
                Some(hex::encode(
                    x.header_body.operational_cert_hot_vkey.as_slice(),
                )),
                Some(x.header_body.operational_cert_sequence_number.to_string()),
            ),
            MultiEraHeader::BabbageCompatible(x) => (
                Some(hex::encode(
                    x.header_body
                        .operational_cert
                        .operational_cert_hot_vkey
                        .as_slice(),
                )),
                Some(
                    x.header_body
                        .operational_cert
                        .operational_cert_sequence_number
                        .to_string(),
                ),
            ),
            _ => (None, None),
        }
    }

    fn compute_total_fees(&self) -> String {
        let txs = self.block.txs();

        txs.iter()
            .map(|tx| {
                if tx.is_valid() {
                    tx.fee_or_compute()
                } else {
                    tx.total_collateral().unwrap_or_default()
                }
            })
            .sum::<u64>()
            .to_string()
    }

    fn compute_total_output(&self) -> String {
        let txs = self.block.txs();

        txs.iter()
            .map(|tx| {
                tx.produces()
                    .iter()
                    .map(|(_, o)| o.value().coin())
                    .sum::<u64>()
            })
            .sum::<u64>()
            .to_string()
    }
}

impl<'a> IntoModel<BlockContent> for BlockModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<BlockContent, StatusCode> {
        let block = &self.block;

        let (epoch, epoch_slot) = self
            .chain
            .as_ref()
            .map(|c| c.slot_epoch(block.slot()))
            .map(|(a, b)| (Some(a), Some(b)))
            .unwrap_or_default();

        let block_time = self
            .chain
            .as_ref()
            .map(|c| c.slot_time(block.slot()))
            .map(|x| Some(x as i32))
            .unwrap_or_default();

        let confirmations = self
            .tip
            .as_ref()
            .map(|x| x.number() - block.number())
            .map(|x| x as i32)
            .unwrap_or_default();

        let block_vrf = self.format_block_vrf()?;

        let slot_leader = self.format_slot_leader()?.unwrap_or_default();

        let next_block = self.next.as_ref().map(|x| x.hash().to_string());

        let previous_block = self.previous.as_ref().map(|x| x.hash().to_string());

        let (op_cert, op_cert_counter) = self.format_ops_cert_data();

        let output = self.compute_total_output();

        let fees = self.compute_total_fees();

        let out = BlockContent {
            hash: block.hash().to_string(),
            next_block,
            previous_block,
            epoch: epoch.map(|x| x as i32),
            epoch_slot: match epoch_slot.map(|x| x as i32) {
                Some(0) => {
                    if matches!(
                        self.block,
                        MultiEraBlock::EpochBoundary(_) | MultiEraBlock::Byron(_)
                    ) {
                        None
                    } else {
                        Some(0)
                    }
                }
                x => x,
            },
            time: block_time.unwrap_or_default(),
            slot: match block.slot() as i32 {
                0 => {
                    if matches!(
                        self.block,
                        MultiEraBlock::EpochBoundary(_) | MultiEraBlock::Byron(_)
                    ) {
                        None
                    } else {
                        Some(0)
                    }
                }
                x => Some(x),
            },
            height: match block.number() as i32 {
                0 => {
                    if matches!(
                        self.block,
                        MultiEraBlock::EpochBoundary(_) | MultiEraBlock::Byron(_)
                    ) {
                        None
                    } else {
                        Some(0)
                    }
                }
                x => Some(x),
            },
            tx_count: block.txs().len() as i32,
            size: block.body_size().unwrap_or(block.size()) as i32,
            confirmations,
            slot_leader,
            block_vrf,
            op_cert,
            op_cert_counter,
            output: match output.as_str() {
                "0" => None,
                _ => Some(output),
            },
            fees: match fees.as_str() {
                "0" => None,
                _ => Some(fees),
            },
        };

        Ok(out)
    }
}

// HACK: This is the mapping to return the tx hashes for a block. For some
// reason, the openspi type BlockContentAddressesInnerTransactionsInner is being
// serialized as an object instead of a the expected strings. As a workaround,
// we return a Vec<String> instead.
impl<'a> IntoModel<Vec<String>> for BlockModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<String>, StatusCode> {
        let block = &self.block;

        let txs = block
            .txs()
            .iter()
            .map(|tx| tx.hash().to_string())
            //.sorted()
            //.map(|tx| BlockContentAddressesInnerTransactionsInner { tx_hash: tx })
            .collect();

        Ok(txs)
    }
}

impl<'a> IntoModel<Vec<BlockContentTxsCborInner>> for BlockModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<BlockContentTxsCborInner>, StatusCode> {
        let block = &self.block;

        let txs = block
            .txs()
            .iter()
            .map(|tx| BlockContentTxsCborInner {
                tx_hash: tx.hash().to_string(),
                cbor: hex::encode(tx.encode()),
            })
            .collect();

        Ok(txs)
    }
}

impl<'a> IntoModel<Vec<BlockContentAddressesInner>> for BlockModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<BlockContentAddressesInner>, StatusCode> {
        let touched_addresses = self.touched_addresses.ok_or_else(|| {
            tracing::error!("touched addresses not collected for block");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        // BTreeMap keeps entries sorted alphabetically by address, matching
        // Blockfrost. Tx hashes are appended in block order and deduped per
        // address because each tx's touched addresses arrive as a set.
        let mut by_address: BTreeMap<String, Vec<String>> = BTreeMap::new();

        for (tx_hash, touched_by_tx) in touched_addresses {
            for address in touched_by_tx {
                by_address.entry(address).or_default().push(tx_hash.clone());
            }
        }

        let addresses = by_address
            .into_iter()
            .map(|(address, tx_hashes)| BlockContentAddressesInner {
                address,
                transactions: tx_hashes
                    .into_iter()
                    .map(|tx_hash| BlockContentAddressesInnerTransactionsInner { tx_hash })
                    .collect(),
            })
            .collect();

        Ok(addresses)
    }
}
