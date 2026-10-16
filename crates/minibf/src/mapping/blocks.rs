use std::collections::{BTreeMap, BTreeSet, HashSet};

use axum::http::StatusCode;
use blockfrost_openapi::models::{
    block_content::BlockContent, block_content_addresses_inner::BlockContentAddressesInner,
    block_content_addresses_inner_transactions_inner::BlockContentAddressesInnerTransactionsInner,
    block_content_txs_cbor_inner::BlockContentTxsCborInner,
};
use dolos_cardano::ChainSummary;
use dolos_core::Genesis;
use itertools::Itertools as _;
use pallas::{
    codec::minicbor,
    crypto::hash::{Hash, Hasher},
    ledger::traverse::{MultiEraBlock, MultiEraHeader, MultiEraTx},
};

use super::{bech32_pool, collated, IntoModel};
use crate::log_and_500;

pub struct BlockModelBuilder<'a> {
    block: MultiEraBlock<'a>,
    chain: Option<&'a ChainSummary>,
    previous: Option<MultiEraBlock<'a>>,
    next: Option<MultiEraBlock<'a>>,
    tip: Option<MultiEraBlock<'a>>,
    touched_addresses: Option<Vec<(String, BTreeSet<String>)>>,
    genesis_delegates: Option<HashSet<Hash<28>>>,
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
            genesis_delegates: None,
        })
    }

    pub fn txs(&self) -> Vec<MultiEraTx<'_>> {
        self.block.txs()
    }

    /// Calls `collect` for every tx in block order.
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

    /// The keys Shelley genesis delegates block production to, which db-sync
    /// names `ShelleyGenesis-…` as slot leaders rather than as pools.
    pub fn with_genesis_delegates(self, genesis: &Genesis) -> Self {
        let delegates = genesis
            .shelley
            .gen_delegs
            .iter()
            .flatten()
            .filter_map(|(_, x)| x.delegate.as_deref()?.parse().ok())
            .collect();

        Self {
            genesis_delegates: Some(delegates),
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

        let Some(genesis_delegates) = self.genesis_delegates.as_ref() else {
            return Ok(None);
        };

        match header.issuer_vkey() {
            Some(key) => {
                let hash: Hash<28> = Hasher::<224>::hash(key);

                // a genesis delegate can mint well past the first Shelley
                // epoch, wherever the decentralisation parameter leaves it slots
                if !genesis_delegates.contains(&hash) {
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

        // db-sync gives a Byron epoch-boundary block no slot and no number
        let is_boundary = matches!(block, MultiEraBlock::EpochBoundary(_));

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
            epoch_slot: epoch_slot.filter(|_| !is_boundary).map(|x| x as i32),
            time: block_time.unwrap_or_default(),
            slot: (!is_boundary).then_some(block.slot() as i32),
            height: (!is_boundary).then_some(block.number() as i32),
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

// `/blocks/{hash}/txs` is a plain list of tx hashes.
impl<'a> IntoModel<Vec<String>> for BlockModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<String>, StatusCode> {
        let block = &self.block;

        let txs = block.txs().iter().map(|tx| tx.hash().to_string()).collect();

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

        Ok(touched_addresses_model(touched_addresses))
    }
}

/// Each address the transactions touch, with the transactions that touch it,
/// from `(tx hash, addresses)` pairs in block order. Sorted by address like
/// Blockfrost (see `collated`); the hashes stay in block order.
pub fn touched_addresses_model(
    touched_addresses: impl IntoIterator<Item = (String, BTreeSet<String>)>,
) -> Vec<BlockContentAddressesInner> {
    let mut by_address: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for (tx_hash, touched_by_tx) in touched_addresses {
        for address in touched_by_tx {
            by_address.entry(address).or_default().push(tx_hash.clone());
        }
    }

    let mut by_address: Vec<_> = by_address.into_iter().collect();
    by_address.sort_by(|(a, _), (b, _)| collated(a, b));

    by_address
        .into_iter()
        .map(|(address, tx_hashes)| BlockContentAddressesInner {
            address,
            transactions: tx_hashes
                .into_iter()
                .map(|tx_hash| BlockContentAddressesInnerTransactionsInner { tx_hash })
                .collect(),
        })
        .collect()
}

/// Parses the header of a stored block without decoding the transactions.
/// Returns `None` for Byron blocks (no issuer).
pub fn decode_block_header(body: &[u8]) -> Result<Option<MultiEraHeader<'_>>, StatusCode> {
    use std::borrow::Cow;

    use pallas::codec::utils::KeepRaw;
    use pallas::ledger::primitives::{alonzo, babbage};
    use pallas::ledger::traverse::{probe, Era};

    let era = match probe::block_era(body) {
        probe::Outcome::Matched(era) => era,
        probe::Outcome::EpochBoundary => return Ok(None),
        probe::Outcome::Inconclusive => {
            return Err(log_and_500("failed to probe block era")("inconclusive"))
        }
    };

    if era == Era::Byron {
        return Ok(None);
    }

    // `[era_tag, [header, tx_bodies, ...]]`: the header is the first item of
    // the inner array.
    let mut d = minicbor::Decoder::new(body);
    let header = (|| -> Result<_, minicbor::decode::Error> {
        d.array()?;
        d.u8()?;
        d.array()?;

        match era {
            Era::Shelley | Era::Allegra | Era::Mary | Era::Alonzo => {
                let header: KeepRaw<alonzo::Header> = d.decode()?;
                Ok(MultiEraHeader::ShelleyCompatible(Cow::Owned(header)))
            }
            _ => {
                let header: KeepRaw<babbage::Header> = d.decode()?;
                Ok(MultiEraHeader::BabbageCompatible(Cow::Owned(header)))
            }
        }
    })()
    .map_err(log_and_500("failed to decode block header"))?;

    Ok(Some(header))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_block_header_matches_full_decode() {
        let (_, raw) = dolos_testing::blocks::make_conway_block(1234);

        let header = decode_block_header(&raw).unwrap().expect("conway header");
        let block = MultiEraBlock::decode(&raw).unwrap();

        assert_eq!(header.hash(), block.hash());
        assert_eq!(header.issuer_vkey(), block.header().issuer_vkey());
    }

    #[test]
    fn decode_block_header_reads_the_shelley_header_shape() {
        use pallas::ledger::primitives::alonzo;

        let issuer_vkey = vec![0xAA; 32];
        let header = alonzo::Header {
            header_body: alonzo::HeaderBody {
                block_number: 7,
                slot: 42,
                prev_hash: None,
                issuer_vkey: issuer_vkey.clone().into(),
                vrf_vkey: vec![].into(),
                nonce_vrf: alonzo::VrfCert(vec![].into(), vec![].into()),
                leader_vrf: alonzo::VrfCert(vec![].into(), vec![].into()),
                block_body_size: 0,
                block_body_hash: pallas::crypto::hash::Hash::from([0u8; 32]),
                operational_cert_hot_vkey: vec![].into(),
                operational_cert_sequence_number: 0,
                operational_cert_kes_period: 0,
                operational_cert_sigma: vec![].into(),
                protocol_major: 6,
                protocol_minor: 0,
            },
            body_signature: vec![].into(),
        };
        let header_cbor = minicbor::to_vec(&header).unwrap();

        // Wrap as a stored alonzo block: `[5, [header]]`. The helper stops at
        // the header, so the block needs no transaction sections.
        let mut body = vec![0x82, 0x05, 0x81];
        body.extend(&header_cbor);

        let decoded = decode_block_header(&body).unwrap().expect("alonzo header");

        assert!(matches!(decoded, MultiEraHeader::ShelleyCompatible(_)));
        assert_eq!(decoded.issuer_vkey().unwrap(), issuer_vkey.as_slice());
        assert_eq!(decoded.hash(), Hasher::<256>::hash(&header_cbor));
    }

    #[test]
    fn decode_block_header_skips_byron_blocks() {
        // `[0, []]` = epoch boundary block, `[1, []]` = byron main block.
        assert!(decode_block_header(&[0x82, 0x00, 0x80]).unwrap().is_none());
        assert!(decode_block_header(&[0x82, 0x01, 0x80]).unwrap().is_none());
    }

    #[test]
    fn decode_block_header_rejects_malformed_bytes() {
        // Not a block wrapper at all.
        assert!(decode_block_header(&[0xff, 0x00]).is_err());

        // A real block truncated inside the header.
        let (_, raw) = dolos_testing::blocks::make_conway_block(1234);
        assert!(decode_block_header(&raw[..raw.len() / 4]).is_err());
    }
}
