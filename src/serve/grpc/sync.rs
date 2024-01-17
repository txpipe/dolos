use futures_core::Stream;
use pallas::{
    crypto::hash::Hash,
    ledger::traverse::{Era, MultiEraOutput},
    storage::rolldb::{chain, wal},
};
use std::pin::Pin;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};
use utxorpc::proto::sync::v1::*;

use crate::storage::applydb::ApplyDB;

fn bytes_to_hash(raw: &[u8]) -> Hash<32> {
    let array: [u8; 32] = raw.try_into().unwrap();
    Hash::<32>::new(array)
}

// fn raw_to_anychain2(raw: &[u8]) -> AnyChainBlock {
//     let block = any_chain_block::Chain::Raw(Bytes::copy_from_slice(raw));
//     AnyChainBlock { chain: Some(block) }
// }

fn fetch_stxi(hash: Hash<32>, idx: u64, ledger: &ApplyDB) -> utxorpc::proto::cardano::v1::TxOutput {
    let (era, cbor) = ledger.get_stxi(hash, idx).unwrap().unwrap();
    let era = Era::try_from(era).unwrap();
    let txo = MultiEraOutput::decode(era, &cbor).unwrap();
    pallas::interop::utxorpc::map_tx_output(&txo)
}

fn raw_to_anychain(raw: &[u8], ledger: &ApplyDB) -> AnyChainBlock {
    let mut block = pallas::interop::utxorpc::map_block_cbor(raw);

    let input_refs: Vec<_> = block
        .body
        .iter()
        .flat_map(|b| b.tx.iter())
        .flat_map(|t| t.inputs.iter())
        .map(|i| (bytes_to_hash(&i.tx_hash), i.output_index))
        .collect();

    // TODO: turn this into a multi-get
    let mut stxis: Vec<_> = input_refs
        .into_iter()
        .map(|(hash, idx)| (hash.clone(), idx, fetch_stxi(hash, idx as u64, &ledger)))
        .collect();

    for tx in block.body.as_mut().unwrap().tx.iter_mut() {
        for input in tx.inputs.iter_mut() {
            let key = (bytes_to_hash(&input.tx_hash), input.output_index);
            let index = stxis
                .binary_search_by_key(&key, |&(a, b, _)| (a, b))
                .unwrap();

            let (_, _, stxi) = stxis.remove(index);
            input.as_output = Some(stxi);
        }
    }

    //pallas::interop::utxorpc::map_tx_output(x)

    AnyChainBlock {
        chain: utxorpc::proto::sync::v1::any_chain_block::Chain::Cardano(block).into(),
    }
}

fn roll_to_tip_response(log: wal::Log, ledger: &ApplyDB) -> FollowTipResponse {
    utxorpc::proto::sync::v1::FollowTipResponse {
        action: match log {
            wal::Log::Apply(_, _, block) => {
                follow_tip_response::Action::Apply(raw_to_anychain(&block, ledger)).into()
            }
            wal::Log::Undo(_, _, block) => {
                follow_tip_response::Action::Undo(raw_to_anychain(&block, ledger)).into()
            }
            // TODO: shouldn't we have a u5c event for origin?
            wal::Log::Origin => None,
            wal::Log::Mark(..) => None,
        },
    }
}

pub struct ChainSyncServiceImpl {
    wal: wal::Store,
    chain: chain::Store,
    ledger: ApplyDB,
}

impl ChainSyncServiceImpl {
    pub fn new(wal: wal::Store, chain: chain::Store, ledger: ApplyDB) -> Self {
        Self { wal, chain, ledger }
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
            .map(|hash| self.chain.get_block(hash))
            .collect();

        let out: Vec<_> = blocks
            .map_err(|_err| Status::internal("can't query block"))?
            .iter()
            .flatten()
            .map(|b| raw_to_anychain(b, &self.ledger))
            .collect();

        let response = FetchBlockResponse { block: out };

        Ok(Response::new(response))
    }

    async fn dump_history(
        &self,
        request: Request<DumpHistoryRequest>,
    ) -> Result<Response<DumpHistoryResponse>, Status> {
        let msg = request.into_inner();
        let from = msg.start_token.map(|r| r.index).unwrap_or_default();
        let len = msg.max_items as usize + 1;

        let mut page: Vec<_> = self
            .chain
            .read_chain_page(from, len)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_err| Status::internal("can't query history"))?;

        let next_token = if page.len() == len {
            let (next_slot, next_hash) = page.remove(len - 1);
            Some(BlockRef {
                index: next_slot,
                hash: next_hash.to_vec().into(),
            })
        } else {
            None
        };

        let blocks = page
            .into_iter()
            .map(|(_, hash)| self.chain.get_block(hash))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_err| Status::internal("can't query history"))?
            .into_iter()
            .map(|x| x.ok_or(Status::internal("can't query history")))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|raw| raw_to_anychain(&raw, &self.ledger))
            .collect();

        let response = DumpHistoryResponse {
            block: blocks,
            next_token,
        };

        Ok(Response::new(response))
    }

    async fn follow_tip(
        &self,
        _request: Request<FollowTipRequest>,
    ) -> Result<Response<Self::FollowTipStream>, tonic::Status> {
        let ledger = self.ledger.clone();

        let s = wal::RollStream::start_after(self.wal.clone(), None)
            .map(move |log| Ok(roll_to_tip_response(log, &ledger)));

        Ok(Response::new(Box::pin(s)))
    }
}
