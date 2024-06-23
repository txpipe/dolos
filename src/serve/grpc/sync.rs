use futures_core::Stream;
use futures_util::StreamExt;
use itertools::{Either, Itertools};
use pallas::interop::utxorpc as interop;
use pallas::interop::utxorpc::{spec as u5c, Mapper};
use std::pin::Pin;
use tonic::{Request, Response, Status};

use crate::ledger;
use crate::wal::{self, RawBlock, WalReader as _};

fn u5c_to_chain_point(block_ref: u5c::sync::BlockRef) -> Result<wal::ChainPoint, Status> {
    Ok(wal::ChainPoint::Specific(
        block_ref.index,
        super::convert::bytes_to_hash32(&block_ref.hash)?,
    ))
}

// fn raw_to_anychain2(raw: &[u8]) -> AnyChainBlock {
//     let block = any_chain_block::Chain::Raw(Bytes::copy_from_slice(raw));
//     AnyChainBlock { chain: Some(block) }
// }

fn raw_to_anychain(
    mapper: &Mapper<ledger::store::LedgerStore>,
    raw: &wal::RawBlock,
) -> u5c::sync::AnyChainBlock {
    let wal::RawBlock { body, .. } = raw;
    let block = mapper.map_block_cbor(body);

    u5c::sync::AnyChainBlock {
        chain: u5c::sync::any_chain_block::Chain::Cardano(block).into(),
    }
}

fn raw_to_blockref(raw: &wal::RawBlock) -> u5c::sync::BlockRef {
    let RawBlock { slot, hash, .. } = raw;

    u5c::sync::BlockRef {
        index: *slot,
        hash: hash.to_vec().into(),
    }
}

fn roll_to_tip_response(
    mapper: &Mapper<ledger::store::LedgerStore>,
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

pub struct ChainSyncServiceImpl {
    wal: wal::redb::WalStore,
    mapper: interop::Mapper<ledger::store::LedgerStore>,
}

impl ChainSyncServiceImpl {
    pub fn new(wal: wal::redb::WalStore, ledger: ledger::store::LedgerStore) -> Self {
        Self {
            wal,
            mapper: Mapper::new(ledger),
        }
    }
}

#[async_trait::async_trait]
impl u5c::sync::chain_sync_service_server::ChainSyncService for ChainSyncServiceImpl {
    type FollowTipStream =
        Pin<Box<dyn Stream<Item = Result<u5c::sync::FollowTipResponse, Status>> + Send + 'static>>;

    async fn fetch_block(
        &self,
        request: Request<u5c::sync::FetchBlockRequest>,
    ) -> Result<Response<u5c::sync::FetchBlockResponse>, Status> {
        let message = request.into_inner();

        let points: Vec<_> = message
            .r#ref
            .into_iter()
            .map(u5c_to_chain_point)
            .try_collect()?;

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

        let from = msg.start_token.map(u5c_to_chain_point).transpose()?;

        let len = msg.max_items as usize + 1;

        let page = self
            .wal
            .read_block_page(from.as_ref(), len)
            .map_err(|_err| Status::internal("can't query block"))?;

        let (items, next_token): (_, Vec<_>) =
            page.into_iter().enumerate().partition_map(|(idx, x)| {
                if idx < len - 1 {
                    Either::Left(raw_to_anychain(&self.mapper, &x))
                } else {
                    Either::Right(raw_to_blockref(&x))
                }
            });

        let response = u5c::sync::DumpHistoryResponse {
            block: items,
            next_token: next_token.into_iter().next(),
        };

        Ok(Response::new(response))
    }

    async fn follow_tip(
        &self,
        request: Request<u5c::sync::FollowTipRequest>,
    ) -> Result<Response<Self::FollowTipStream>, tonic::Status> {
        let request = request.into_inner();

        let from_seq = if request.intersect.is_empty() {
            self.wal
                .find_tip()
                .map_err(|_err| Status::internal("can't read WAL"))?
                .map(|(x, _)| x)
                .unwrap_or_default()
        } else {
            let intersect: Vec<_> = request
                .intersect
                .into_iter()
                .map(u5c_to_chain_point)
                .try_collect()?;

            self.wal
                .find_intersect(&intersect)
                .map_err(|_err| Status::internal("can't read WAL"))?
                .map(|(x, _)| x)
                .ok_or(Status::internal("can't find WAL sequence"))?
        };

        let mapper = self.mapper.clone();

        let stream = wal::WalStream::start(self.wal.clone(), from_seq)
            .map(move |(_, log)| Ok(roll_to_tip_response(&mapper, &log)));

        Ok(Response::new(Box::pin(stream)))
    }
}
