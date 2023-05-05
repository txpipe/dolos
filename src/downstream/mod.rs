use std::pin::Pin;

use bytes::Bytes;
use futures_core::Stream;
use pallas::crypto::hash::Hash;
use tonic::{Request, Response, Status};
use utxorpc::proto::sync::v1::*;

use pallas::ledger::traverse as trv;
use utxorpc::proto::cardano::v1 as u5c;

use crate::rolldb::RollDB;

fn bytes_to_hash(raw: &[u8]) -> Hash<32> {
    let array: [u8; 32] = raw.try_into().unwrap();
    Hash::<32>::new(array)
}

fn raw_to_anychain2(raw: &[u8]) -> AnyChainBlock {
    let block = any_chain_block::Chain::Raw(Bytes::copy_from_slice(raw));
    AnyChainBlock { chain: Some(block) }
}

fn raw_to_anychain(raw: &[u8]) -> AnyChainBlock {
    let block = trv::MultiEraBlock::decode(raw).unwrap();

    let block = u5c::Block {
        header: u5c::BlockHeader {
            slot: block.slot(),
            hash: block.hash().to_vec().into(),
        }
        .into(),
        body: u5c::BlockBody {
            tx: block.txs().iter().map(|tx| from_traverse_tx(tx)).collect(),
        }
        .into(),
    };

    let block = any_chain_block::Chain::Cardano(block);
    AnyChainBlock { chain: Some(block) }
}

pub struct ChainSyncServiceImpl(RollDB);

impl ChainSyncServiceImpl {
    pub fn new(db: RollDB) -> Self {
        Self(db)
    }
}

fn from_traverse_tx(tx: &trv::MultiEraTx) -> u5c::Tx {
    u5c::Tx {
        inputs: tx
            .inputs()
            .iter()
            .map(|i| u5c::TxInput {
                tx_hash: i.hash().to_vec().into(),
                output_index: i.index() as u32,
                as_output: None,
            })
            .collect(),
        outputs: tx
            .outputs()
            .iter()
            .map(|o| u5c::TxOutput {
                address: o.address().map(|a| a.to_vec()).unwrap_or_default().into(),
                coin: o.lovelace_amount(),
                // TODO: this is wrong, we're crating a new item for each asset even if they share
                // the same policy id. We need to adjust Pallas' interface to make this mapping more
                // ergonomic.
                assets: o
                    .non_ada_assets()
                    .iter()
                    .map(|a| u5c::Multiasset {
                        policy_id: a.policy().map(|x| x.to_vec()).unwrap_or_default().into(),
                        assets: vec![u5c::Asset {
                            name: a.name().map(|x| x.to_vec()).unwrap_or_default().into(),
                            quantity: a.coin() as u64,
                        }],
                    })
                    .collect(),
                datum: None,
                datum_hash: Default::default(),
                script: None,
                redeemer: None,
            })
            .collect(),
        certificates: vec![],
        withdrawals: vec![],
        mint: vec![],
        reference_inputs: vec![],
        witnesses: u5c::WitnessSet {
            vkeywitness: vec![],
            script: vec![],
            plutus_datums: vec![],
        }
        .into(),
        collateral: u5c::Collateral {
            collateral: vec![],
            collateral_return: None,
            total_collateral: Default::default(),
        }
        .into(),
        fee: tx.fee().unwrap_or_default(),
        validity: u5c::TxValidity {
            start: tx.validity_start().unwrap_or_default(),
            ttl: tx.ttl().unwrap_or_default(),
        }
        .into(),
        successful: tx.is_valid(),
        auxiliary: u5c::AuxData {
            metadata: vec![],
            scripts: vec![],
        }
        .into(),
    }
}

#[async_trait::async_trait]
impl chain_sync_service_server::ChainSyncService for ChainSyncServiceImpl {
    type FollowTipStream =
        Pin<Box<dyn Stream<Item = Result<FollowTipResponse, Status>> + Send + 'static>>;

    async fn fetch_block(
        &self,
        request: Request<FetchBlockRequest>,
    ) -> Result<Response<FetchBlockResponse>, Status> {
        let message = request.into_inner();

        let blocks: Result<Vec<_>, _> = message
            .r#ref
            .iter()
            .map(|r| bytes_to_hash(&r.hash))
            .map(|hash| self.0.get_block(hash))
            .collect();

        let out: Vec<_> = blocks
            .map_err(|err| Status::internal("can't query block"))?
            .iter()
            .flatten()
            .map(|b| raw_to_anychain(b))
            .collect();

        let response = FetchBlockResponse { block: out };

        Ok(Response::new(response))
    }

    async fn dump_history(
        &self,
        request: Request<DumpHistoryRequest>,
    ) -> Result<Response<DumpHistoryResponse>, Status> {
        todo!()
    }

    async fn follow_tip(
        &self,
        request: Request<FollowTipRequest>,
    ) -> Result<Response<Self::FollowTipStream>, tonic::Status> {
        todo!()
    }
}
