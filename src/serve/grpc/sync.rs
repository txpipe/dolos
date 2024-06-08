use futures_core::Stream;
use futures_util::StreamExt;
use itertools::Itertools;
use pallas::interop::utxorpc::{spec as u5c, Mapper};
use pallas::{
    crypto::hash::Hash,
    ledger::traverse::{Era, MultiEraOutput},
    storage::rolldb::{chain, wal},
};
use std::pin::Pin;
use tonic::{Request, Response, Status};

use crate::wal::{self, RawBlock};

fn u5c_to_chain_point(block_ref: u5c::sync::BlockRef) -> wal::ChainPoint {
    wal::ChainPoint::Specific(block_ref.index, block_ref.hash.as_ref().into())
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
fn raw_to_anychain(mapper: &Mapper<super::Context>, raw: &wal::RawBlock) -> AnyChainBlock {
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

    let block = mapper.map_block_cbor(body);

    u5c::sync::AnyChainBlock {
        chain: u5c::sync::any_chain_block::Chain::Cardano(block).into(),
    }
}

fn roll_to_tip_response(
    mapper: &Mapper<super::Context>,
    log: &wal::LogValue,
) -> u5c::sync::FollowTipResponse {
    u5c::sync::FollowTipResponse {
        action: match log {
            wal::LogValue::Apply(x) => {
                u5c::sync::follow_tip_response::Action::Apply(raw_to_anychain(mapper, x)).into()
            }
            wal::LogValue::Undo(x) => {
                u5c::sync::follow_tip_response::Action::Undo(raw_to_anychain(mapper, x)).into()
            }
            // TODO: shouldn't we have a u5c event for origin?
            wal::LogValue::Mark(..) => None,
        },
    }
}

pub struct ChainSyncServiceImpl<W: wal::WalReader> {
    wal: W,
    mapper: pallas::interop::utxorpc::Mapper<super::Context>,
}

impl<W: wal::WalReader> ChainSyncServiceImpl<W> {
    pub fn new(wal: W) -> Self {
        Self {
            wal,
            mapper: pallas::interop::utxorpc::Mapper::default(),
        }
    }
}

#[async_trait::async_trait]
impl<W> u5c::sync::chain_sync_service_server::ChainSyncService for ChainSyncServiceImpl<W>
where
    W: wal::WalReader + Send + Sync + 'static,
{
    type FollowTipStream =
        Pin<Box<dyn Stream<Item = Result<u5c::sync::FollowTipResponse, Status>> + Send + 'static>>;

    async fn fetch_block(
        &self,
        request: Request<u5c::sync::FetchBlockRequest>,
    ) -> Result<Response<u5c::sync::FetchBlockResponse>, Status> {
        let message = request.into_inner();

        let points: Vec<_> = message.r#ref.into_iter().map(u5c_to_chain_point).collect();

        let out = self
            .wal
            .read_sparse_blocks(&points)
            .map_err(|_err| Status::internal("can't query block"))?
            .into_iter()
            .map(|x| raw_to_anychain(&self.mapper, &x))
            .collect();

        let response = u5c::sync::FetchBlockResponse { block: out };

        Ok(Response::new(response))
    }

    async fn dump_history(
        &self,
        request: Request<u5c::sync::DumpHistoryRequest>,
    ) -> Result<Response<u5c::sync::DumpHistoryResponse>, Status> {
        let msg = request.into_inner();

        let from = msg.start_token.map(u5c_to_chain_point);

        let len = msg.max_items as usize + 1;

        let mut page = self
            .wal
            .read_block_page(from.as_ref(), len)
            .map_err(|_err| Status::internal("can't query block"))?
            .collect_vec();

        let next_token = if page.len() == len {
            let RawBlock { slot, hash, .. } = page.remove(len - 1);

            Some(u5c::sync::BlockRef {
                index: slot,
                hash: hash.to_vec().into(),
            })
        } else {
            None
        };

        let blocks = page
            .into_iter()
            .map(|x| raw_to_anychain(&self.mapper, &x))
            .collect();

        let response = u5c::sync::DumpHistoryResponse {
            block: blocks,
            next_token,
        };

        Ok(Response::new(response))
    }

    async fn follow_tip(
        &self,
        request: Request<u5c::sync::FollowTipRequest>,
    ) -> Result<Response<Self::FollowTipStream>, tonic::Status> {
        let request = request.into_inner();

        let intersect: Vec<_> = request
            .intersect
            .into_iter()
            .map(u5c_to_chain_point)
            .collect();

        let (from_seq, _) = self
            .wal
            .find_intersect(&intersect)
            .map_err(|_err| Status::internal("can't read WAL"))?
            .ok_or(Status::internal("can't find WAL sequence"))?;

        let mapper = self.mapper.clone();

        let stream = wal::WalStream::start(self.wal.clone(), from_seq)
            .map(move |(_, log)| Ok(roll_to_tip_response(&mapper, &log)));

        Ok(Response::new(Box::pin(stream)))
    }
}
