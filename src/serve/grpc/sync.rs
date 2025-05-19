use futures_core::Stream;
use futures_util::stream::once;
use futures_util::StreamExt;
use itertools::{Either, Itertools};
use pallas::interop::utxorpc as interop;
use pallas::interop::utxorpc::spec::sync::BlockRef;
use pallas::interop::utxorpc::{spec as u5c, Mapper};
use pallas::ledger::traverse::MultiEraBlock;
use std::pin::Pin;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use crate::chain::ChainStore;
use crate::prelude::BlockBody;
use crate::state::LedgerStore;
use crate::wal::{self, ChainPoint, RawBlock, WalReader as _};

fn u5c_to_chain_point(block_ref: u5c::sync::BlockRef) -> Result<wal::ChainPoint, Status> {
    Ok(wal::ChainPoint::Specific(
        block_ref.slot,
        super::convert::bytes_to_hash32(&block_ref.hash)?,
    ))
}

// fn raw_to_anychain2(raw: &[u8]) -> AnyChainBlock {
//     let block = any_chain_block::Chain::Raw(Bytes::copy_from_slice(raw));
//     AnyChainBlock { chain: Some(block) }
// }

fn get_block_height(chain: &ChainStore, slot: u64) -> Result<u64, Status> {
    let block_body = chain
        .get_block_by_slot(&slot)
        .map_err(|_| Status::internal("Failed to query chain service.".to_string()))?
        .ok_or(Status::not_found("Failed to find block.".to_string()))?;

    let height = MultiEraBlock::decode(&block_body)
        .map_err(|_| Status::internal("Failed to decode block."))?
        .number();

    Ok(height)
}

fn raw_to_anychain(mapper: &Mapper<LedgerStore>, body: &BlockBody) -> u5c::sync::AnyChainBlock {
    let block = mapper.map_block_cbor(body);

    u5c::sync::AnyChainBlock {
        native_bytes: body.to_vec().into(),
        chain: u5c::sync::any_chain_block::Chain::Cardano(block).into(),
    }
}

fn raw_to_blockref(raw: &wal::RawBlock, chain: &ChainStore) -> u5c::sync::BlockRef {
    let RawBlock { slot, hash, .. } = raw;

    let height = get_block_height(chain, *slot)
        .map_err(|_| Status::internal("Failed to query chain service.".to_string()))
        .unwrap_or(0);

    u5c::sync::BlockRef {
        slot: *slot,
        hash: hash.to_vec().into(),
        height,
    }
}

fn wal_log_to_tip_response(
    mapper: &Mapper<LedgerStore>,
    log: &wal::LogValue,
) -> u5c::sync::FollowTipResponse {
    u5c::sync::FollowTipResponse {
        action: match log {
            wal::LogValue::Apply(x) => {
                u5c::sync::follow_tip_response::Action::Apply(raw_to_anychain(mapper, &x.body))
                    .into()
            }
            wal::LogValue::Undo(x) => {
                u5c::sync::follow_tip_response::Action::Undo(raw_to_anychain(mapper, &x.body))
                    .into()
            }
            // TODO: shouldn't we have a u5c event for origin?
            wal::LogValue::Mark(..) => None,
        },
    }
}

fn point_to_reset_tip_response(
    point: ChainPoint,
    chain: ChainStore,
) -> u5c::sync::FollowTipResponse {
    match point {
        ChainPoint::Origin => u5c::sync::FollowTipResponse {
            action: u5c::sync::follow_tip_response::Action::Reset(BlockRef {
                slot: 0,
                hash: vec![].into(),
                height: 0,
            })
            .into(),
        },
        ChainPoint::Specific(slot, hash) => u5c::sync::FollowTipResponse {
            action: u5c::sync::follow_tip_response::Action::Reset(BlockRef {
                slot,
                hash: hash.to_vec().into(),
                height: get_block_height(&chain, slot)
                    .map_err(|_| Status::internal("Failed to query chain service.".to_string()))
                    .unwrap_or(0),
            })
            .into(),
        },
    }
}

pub struct SyncServiceImpl {
    wal: wal::redb::WalStore,
    chain: ChainStore,
    mapper: interop::Mapper<LedgerStore>,
    cancellation_token: CancellationToken,
}

impl SyncServiceImpl {
    pub fn new(
        wal: wal::redb::WalStore,
        ledger: LedgerStore,
        chain: ChainStore,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            wal,
            mapper: Mapper::new(ledger),
            chain,
            cancellation_token,
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

        let out: Vec<_> = message
            .r#ref
            .iter()
            .map(|br| {
                self.chain
                    .get_block_by_slot(&br.slot)
                    .map_err(|_| Status::internal("Failed to query chain service."))?
                    .map(|body| raw_to_anychain(&self.mapper, &body))
                    .ok_or(Status::not_found(format!("Failed to find block: {:?}", br)))
            })
            .collect::<Result<Vec<u5c::sync::AnyChainBlock>, Status>>()?;

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
                    Either::Left(raw_to_anychain(&self.mapper, &x.body))
                } else {
                    Either::Right(raw_to_blockref(&x, &self.chain))
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

        let chain = self.chain.clone();
        let reset = once(async { Ok(point_to_reset_tip_response(point, chain)) });

        let forward =
            wal::WalStream::start(self.wal.clone(), from_seq, self.cancellation_token.clone())
                .skip(1)
                .map(move |(_, log)| Ok(wal_log_to_tip_response(&mapper, &log)));

        let stream = reset.chain(forward);

        Ok(Response::new(Box::pin(stream)))
    }

    async fn read_tip(
        &self,
        _request: tonic::Request<u5c::sync::ReadTipRequest>,
    ) -> std::result::Result<tonic::Response<u5c::sync::ReadTipResponse>, tonic::Status> {
        let (slot, body) = self
            .chain
            .get_tip()
            .map_err(|e| Status::internal(format!("Unable to read WAL: {:?}", e)))?
            .ok_or(Status::internal("chain has no data."))?;

        let hash = MultiEraBlock::decode(&body)
            .map_err(|_| Status::internal("Failed to decode tip block."))?
            .hash();

        let height = get_block_height(&self.chain, slot)?;

        let response = u5c::sync::ReadTipResponse {
            tip: Some(BlockRef {
                slot,
                hash: hash.to_vec().into(),
                height,
            }),
        };

        Ok(Response::new(response))
    }
}
