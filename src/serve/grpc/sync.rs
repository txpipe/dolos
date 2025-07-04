use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::stream::once;
use itertools::{Either, Itertools};
use pallas::interop::utxorpc::spec::sync::BlockRef;
use pallas::interop::utxorpc::{self as interop, LedgerContext};
use pallas::interop::utxorpc::{Mapper, spec as u5c};
use std::pin::Pin;
use tonic::{Request, Response, Status};

use super::stream::WalStream;
use crate::prelude::*;

fn u5c_to_chain_point(block_ref: u5c::sync::BlockRef) -> Result<ChainPoint, Status> {
    Ok(ChainPoint::Specific(
        block_ref.index,
        super::convert::bytes_to_hash32(&block_ref.hash)?,
    ))
}

// fn raw_to_anychain2(raw: &[u8]) -> AnyChainBlock {
//     let block = any_chain_block::Chain::Raw(Bytes::copy_from_slice(raw));
//     AnyChainBlock { chain: Some(block) }
// }

fn raw_to_anychain<C: LedgerContext>(
    mapper: &Mapper<C>,
    body: &BlockBody,
) -> u5c::sync::AnyChainBlock {
    let block = mapper.map_block_cbor(body);

    u5c::sync::AnyChainBlock {
        native_bytes: body.to_vec().into(),
        chain: u5c::sync::any_chain_block::Chain::Cardano(block).into(),
    }
}

fn raw_to_blockref(raw: &RawBlock) -> u5c::sync::BlockRef {
    let RawBlock { slot, hash, .. } = raw;

    u5c::sync::BlockRef {
        index: *slot,
        hash: hash.to_vec().into(),
    }
}

fn point_to_blockref(point: &ChainPoint) -> u5c::sync::BlockRef {
    match point {
        ChainPoint::Origin => u5c::sync::BlockRef {
            index: 0,
            hash: vec![].into(),
        },
        ChainPoint::Specific(slot, hash) => u5c::sync::BlockRef {
            index: *slot,
            hash: hash.to_vec().into(),
        },
    }
}

fn wal_log_to_tip_response<C: LedgerContext>(
    mapper: &Mapper<C>,
    log: &LogValue,
) -> u5c::sync::FollowTipResponse {
    u5c::sync::FollowTipResponse {
        action: match log {
            LogValue::Apply(x) => {
                u5c::sync::follow_tip_response::Action::Apply(raw_to_anychain(mapper, &x.body))
                    .into()
            }
            LogValue::Undo(x) => {
                u5c::sync::follow_tip_response::Action::Undo(raw_to_anychain(mapper, &x.body))
                    .into()
            }
            // TODO: shouldn't we have a u5c event for origin?
            LogValue::Mark(..) => None,
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

pub struct SyncServiceImpl<D: Domain, C: CancelToken>
where
    D::State: LedgerContext,
{
    domain: D,
    mapper: interop::Mapper<D::State>,
    cancel: C,
}

impl<D: Domain, C: CancelToken> SyncServiceImpl<D, C>
where
    D::State: LedgerContext,
{
    pub fn new(domain: D, cancel: C) -> Self {
        let mapper = Mapper::new(domain.state().clone());

        Self {
            domain,
            mapper,
            cancel,
        }
    }
}

#[async_trait::async_trait]
impl<D: Domain, C: CancelToken> u5c::sync::sync_service_server::SyncService
    for SyncServiceImpl<D, C>
where
    D::State: LedgerContext,
{
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
                self.domain
                    .archive()
                    .get_block_by_slot(&br.index)
                    .map_err(|_| Status::internal("Failed to query chain service."))?
                    .map(|body| raw_to_anychain(&self.mapper, &body))
                    .ok_or(Status::not_found(format!("Failed to find block: {br:?}")))
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
            .domain
            .wal()
            .read_block_page(from.as_ref(), len)
            .map_err(|_err| Status::internal("can't query block"))?;

        let (items, next_token): (_, Vec<_>) =
            page.into_iter().enumerate().partition_map(|(idx, x)| {
                if idx < len - 1 {
                    Either::Left(raw_to_anychain(&self.mapper, &x.body))
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
            self.domain
                .wal()
                .find_tip()
                .map_err(|_err| Status::internal("can't read WAL"))?
                .ok_or(Status::internal("WAL has no data"))?
        } else {
            let intersect: Vec<_> = request
                .intersect
                .into_iter()
                .map(u5c_to_chain_point)
                .try_collect()?;

            self.domain
                .wal()
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

        let forward = WalStream::start(self.domain.wal().clone(), from_seq, self.cancel.clone())
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
            .domain
            .wal()
            .find_tip()
            .map_err(|e| Status::internal(format!("Unable to read WAL: {e:?}")))?
            .ok_or(Status::internal("chain has no data."))?;

        let response = u5c::sync::ReadTipResponse {
            tip: Some(point_to_blockref(&point)),
        };

        Ok(Response::new(response))
    }
}
