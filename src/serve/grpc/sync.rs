use futures_core::Stream;
use futures_util::stream::once;
use futures_util::StreamExt;
use itertools::{Either, Itertools};
use pallas::interop::utxorpc as interop;
use pallas::interop::utxorpc::spec::sync::BlockRef;
use pallas::interop::utxorpc::{spec as u5c, Mapper};
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::ledger::pparams::Genesis;
use crate::state::LedgerStore;
use crate::wal::{self, ChainPoint, RawBlock, WalBlockReader, WalReader as _};

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

fn raw_to_anychain(mapper: &Mapper<LedgerStore>, raw: &wal::RawBlock) -> u5c::sync::AnyChainBlock {
    let wal::RawBlock { body, .. } = raw;
    let block = mapper.map_block_cbor(body);

    u5c::sync::AnyChainBlock {
        native_bytes: body.to_vec().into(),
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

fn wal_log_to_tip_response(
    mapper: &Mapper<LedgerStore>,
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

fn point_to_reset_tip_response(point: ChainPoint) -> u5c::sync::FollowTipResponse {
    match point {
        ChainPoint::Origin => u5c::sync::FollowTipResponse {
            action: u5c::sync::follow_tip_response::Action::Reset(BlockRef {
                hash: vec![].into(),
                index: 0,
            })
            .into(),
        },
        ChainPoint::Specific(slot, hash) => u5c::sync::FollowTipResponse {
            action: u5c::sync::follow_tip_response::Action::Reset(BlockRef {
                hash: hash.to_vec().into(),
                index: slot,
            })
            .into(),
        },
    }
}

pub struct SyncServiceImpl {
    wal: wal::redb::WalStore,
    genesis: Arc<Genesis>,
    mapper: interop::Mapper<LedgerStore>,
}

impl SyncServiceImpl {
    pub fn new(wal: wal::redb::WalStore, genesis: Arc<Genesis>, ledger: LedgerStore) -> Self {
        Self {
            wal,
            genesis,
            mapper: Mapper::new(ledger.clone()),
        }
    }
}

#[async_trait::async_trait]
impl u5c::sync::sync_service_server::SyncService for SyncServiceImpl {
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

        let len = msg.max_items as usize + 1;
        let from = match &msg.start_token.map(u5c_to_chain_point).transpose()? {
            Some(point) => self.wal.locate_point(point).map_err(|err| {
                Status::invalid_argument(format!("failed to locate chain point: {}", err))
            })?,
            None => None,
        };

        // Amount of slots until unmutability is guaranteed.
        let lookahead =
            WalBlockReader::<wal::redb::WalStore>::lookahead_from_genesis(&self.genesis);
        let reader = WalBlockReader::try_new(&self.wal, from, lookahead)
            .map_err(|_| Status::internal("can't open wal"))?;

        let (items, next_token): (_, Vec<_>) =
            reader.take(len).enumerate().partition_map(|(idx, x)| {
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

        let (from_seq, point) = if request.intersect.is_empty() {
            self.wal
                .find_tip()
                .map_err(|_err| Status::internal("can't read WAL"))?
                .ok_or(Status::internal("WAL has no data"))?
        } else {
            let intersect: Vec<_> = request
                .intersect
                .into_iter()
                .map(u5c_to_chain_point)
                .try_collect()?;

            self.wal
                .find_intersect(&intersect)
                .map_err(|_err| Status::internal("can't read WAL"))?
                .ok_or(Status::internal("can't find WAL sequence"))?
        };

        let mapper = self.mapper.clone();

        // Find the intersect, skip 1 block, then convert each to a tip response
        // We skip 1 block to mimic the ouroboros chainsync miniprotocol convention
        // We both agree that the intersection point is in our past, so it doesn't
        // make sense to broadcast this. We send a `Reset` message, so that
        // the consumer knows what intersection was found and can reset their state
        // This would also mimic ouroboros giving a `Rollback` as the first message.

        let reset = once(async { Ok(point_to_reset_tip_response(point)) });

        let forward = wal::WalStream::start(self.wal.clone(), from_seq)
            .skip(1)
            .map(move |(_, log)| Ok(wal_log_to_tip_response(&mapper, &log)));

        let stream = reset.chain(forward);

        Ok(Response::new(Box::pin(stream)))
    }

    async fn read_tip(
        &self,
        _request: tonic::Request<u5c::sync::ReadTipRequest>,
    ) -> std::result::Result<tonic::Response<u5c::sync::ReadTipResponse>, tonic::Status> {
        let (_, point) = self
            .wal
            .find_tip()
            .map_err(|_err| Status::internal("can't read WAL"))?
            .ok_or(Status::internal("WAL has no data"))?;

        let response = u5c::sync::ReadTipResponse {
            tip: match point {
                ChainPoint::Origin => None,
                ChainPoint::Specific(slot, hash) => Some(BlockRef {
                    index: slot,
                    hash: hash.to_vec().into(),
                }),
            },
        };

        Ok(Response::new(response))
    }
}
