use futures_core::Stream;
use pallas::{
    crypto::hash::Hash,
    storage::rolldb::{chain, wal},
};
use std::pin::Pin;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};
use utxorpc::proto::sync::v1::*;

fn bytes_to_hash(raw: &[u8]) -> Hash<32> {
    let array: [u8; 32] = raw.try_into().unwrap();
    Hash::<32>::new(array)
}

// fn raw_to_anychain2(raw: &[u8]) -> AnyChainBlock {
//     let block = any_chain_block::Chain::Raw(Bytes::copy_from_slice(raw));
//     AnyChainBlock { chain: Some(block) }
// }

fn raw_to_anychain(raw: &[u8]) -> AnyChainBlock {
    let block = pallas::interop::utxorpc::map_block_cbor(raw);

    AnyChainBlock {
        chain: utxorpc::proto::sync::v1::any_chain_block::Chain::Cardano(block).into(),
    }
}

fn roll_to_tip_response(log: wal::Log) -> FollowTipResponse {
    utxorpc::proto::sync::v1::FollowTipResponse {
        action: match log {
            wal::Log::Apply(_, _, block) => {
                follow_tip_response::Action::Apply(raw_to_anychain(&block)).into()
            }
            wal::Log::Undo(_, _, block) => {
                follow_tip_response::Action::Undo(raw_to_anychain(&block)).into()
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
}

impl ChainSyncServiceImpl {
    pub fn new(wal: wal::Store, chain: chain::Store) -> Self {
        Self { wal, chain }
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
            .map(|b| raw_to_anychain(b))
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
            .map(|raw| raw_to_anychain(&raw))
            .collect();

        let response = DumpHistoryResponse {
            block: blocks,
            next_token,
        };

        Ok(Response::new(response))
    }

    async fn follow_tip(
        &self,
        request: Request<FollowTipRequest>,
    ) -> Result<Response<Self::FollowTipStream>, tonic::Status> {
        let request = request.into_inner();

        let has_intersect = !request.intersect.is_empty();

        for intersect_attempt in request.intersect {
            let slot = intersect_attempt.index;
            let hash: [u8; 32] = intersect_attempt.hash.to_vec().try_into().unwrap();

            let wal_seq = match self.wal.find_wal_seq(Some((slot, hash.into()))) {
                Ok(x) => x,
                Err(_) => continue, // TODO, not found is an error, error type (kvtable) not public. pallas find_wal_seq should not accept option.
            };

            if let Some(x) = wal_seq {
                // TODO: race cond? WAL may have pruned our entry (and the
                // following which we want) since we found the seq above
                // TODO: combine find_wal_seq/rollstream functionality
                let s = wal::RollStream::start_after(self.wal.clone(), Some(x))
                    .map(|log| Ok(roll_to_tip_response(log)));

                return Ok(Response::new(Box::pin(s)));
            }
        }

        // intersects were provided but we couldn't intersect WAL using them
        if has_intersect {
            return Err(Status::not_found("could not find intersect"));
        }

        let s = wal::RollStream::start_after(self.wal.clone(), None)
            .map(|log| Ok(roll_to_tip_response(log)));

        Ok(Response::new(Box::pin(s)))
    }
}
