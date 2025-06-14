use axum::http::StatusCode;
use pallas::{
    crypto::hash::Hasher,
    ledger::traverse::{MultiEraBlock, MultiEraHeader, MultiEraUpdate},
};
use serde::{Deserialize, Serialize};

use dolos_cardano::pparams::{self, EraSummary};
use dolos_core::{ArchiveStore, BlockBody, Domain, StateStore as _};

pub mod hash_or_number;
pub mod latest;
pub mod slot;

pub struct BlockHeaderFields {
    pub previous_block: Option<String>,
    pub block_vrf: Option<String>,
    pub op_cert: Option<String>,
    pub op_cert_counter: Option<String>,
    pub slot_leader: String,
}

pub fn hash_or_number_to_body(
    hash_or_number: &str,
    chain: &impl ArchiveStore,
) -> Result<BlockBody, StatusCode> {
    match hex::decode(hash_or_number) {
        Ok(hash) => match chain
            .get_block_by_hash(&hash)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        {
            Some(body) => Ok(body),
            None => Err(StatusCode::NOT_FOUND),
        },
        Err(_) => match hash_or_number.parse() {
            Ok(number) => match chain
                .get_block_by_number(&number)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            {
                Some(body) => Ok(body),
                None => Err(StatusCode::NOT_FOUND),
            },
            Err(_) => Err(StatusCode::BAD_REQUEST),
        },
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Block {
    pub slot: Option<u64>,
    pub hash: String,
    pub tx_count: u64,
    pub size: u64,
    pub time: u64,
    pub height: Option<u64>,
    pub epoch: Option<u64>,
    pub epoch_slot: Option<u64>,
    pub slot_leader: String,
    pub output: Option<String>,
    pub fees: Option<String>,
    pub block_vrf: Option<String>,
    pub op_cert: Option<String>,
    pub op_cert_counter: Option<String>,
    pub previous_block: Option<String>,
    pub next_block: Option<String>,
    pub confirmations: u64,
}

impl Block {
    pub fn from_body<D: Domain>(body: &[u8], domain: &D) -> Result<Block, StatusCode> {
        let curr = MultiEraBlock::decode(body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let next: Option<String> = match domain
            .archive()
            .get_block_by_number(&(curr.number() + 1))
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        {
            Some(body) => {
                let block =
                    MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                Some(block.hash().to_string())
            }
            None => None,
        };

        let confirmations = match domain
            .archive()
            .get_tip()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        {
            Some((_, body)) => {
                let block =
                    MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                block.number() - curr.number()
            }
            None => return Err(StatusCode::SERVICE_UNAVAILABLE),
        };

        let slot = curr.slot();

        let tip = domain
            .state()
            .cursor()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let updates = domain
            .state()
            .get_pparams(tip.map(|t| t.slot()).unwrap_or_default())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .into_iter()
            .map(|eracbor| {
                MultiEraUpdate::try_from(eracbor).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
            })
            .collect::<Result<Vec<MultiEraUpdate>, StatusCode>>()?;

        let summary = pparams::fold_with_hacks(domain.genesis(), &updates, slot);

        let BlockHeaderFields {
            previous_block,
            block_vrf,
            op_cert,
            op_cert_counter,
            slot_leader,
        } = Self::extract_from_header(&curr.header())?;
        let (epoch, epoch_slot, time) = crate::mapping::slot_time(slot, &summary);
        Ok(Self {
            slot: Some(curr.slot()),
            hash: curr.hash().to_string(),
            tx_count: curr.tx_count() as u64,
            size: curr.body_size().unwrap_or(0) as u64,
            epoch: Some(epoch),
            epoch_slot: Some(epoch_slot),
            height: Some(curr.number()),
            next_block: next.clone(),
            time,
            confirmations,
            previous_block,
            block_vrf,
            op_cert,
            op_cert_counter,
            output: match curr.tx_count() {
                0 => None,
                _ => Some(
                    curr.txs()
                        .iter()
                        .map(|tx| tx.outputs().iter().map(|o| o.value().coin()).sum::<u64>())
                        .sum::<u64>()
                        .to_string(),
                ),
            },
            fees: match curr.tx_count() {
                0 => None,
                _ => Some(
                    curr.txs()
                        .iter()
                        .map(|tx| tx.fee().unwrap_or(0))
                        .sum::<u64>()
                        .to_string(),
                ),
            },
            slot_leader,
        })
    }

    pub fn find_in_chain<D: Domain>(domain: &D, hash_or_number: &str) -> Result<Block, StatusCode> {
        Self::from_body(
            &hash_or_number_to_body(hash_or_number, domain.archive())?,
            domain,
        )
    }

    pub fn extract_from_header(header: &MultiEraHeader) -> Result<BlockHeaderFields, StatusCode> {
        let previous_block = header.previous_hash().map(|h| h.to_string());
        let block_vrf = match header.vrf_vkey() {
            Some(v) => Some(
                bech32::encode::<bech32::Bech32>(bech32::Hrp::parse("vrf_vk").unwrap(), v)
                    .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?,
            ),
            None => None,
        };

        let slot_leader = match header.issuer_vkey() {
            Some(hash) => bech32::encode::<bech32::Bech32>(
                bech32::Hrp::parse("pool").unwrap(),
                Hasher::<224>::hash(hash).as_ref(),
            )
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
            None => return Err(StatusCode::INTERNAL_SERVER_ERROR),
        };

        let (op_cert, op_cert_counter) = match header {
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
        };
        Ok(BlockHeaderFields {
            previous_block,
            block_vrf,
            slot_leader,
            op_cert_counter,
            op_cert,
        })
    }
}
