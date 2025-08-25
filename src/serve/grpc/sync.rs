use futures_core::Stream;
use futures_util::StreamExt;
use itertools::Itertools;
use pallas::interop::utxorpc as interop;
use pallas::interop::utxorpc::spec::sync::BlockRef;
use pallas::interop::utxorpc::LedgerContext;
use pallas::interop::utxorpc::{spec as u5c, Mapper};
use std::pin::Pin;
use tonic::{Request, Response, Status};

use crate::prelude::*;

const MAX_DUMP_HISTORY_ITEMS: u32 = 100;

fn u5c_to_chain_point(block_ref: u5c::sync::BlockRef) -> Result<ChainPoint, Status> {
    Ok(ChainPoint::Specific(
        block_ref.slot,
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

fn raw_to_blockref<C: LedgerContext>(
    mapper: &Mapper<C>,
    body: &BlockBody,
) -> Option<u5c::sync::BlockRef> {
    let u5c::cardano::Block { header, .. } = mapper.map_block_cbor(body);

    header.map(|h| u5c::sync::BlockRef {
        slot: h.slot,
        hash: h.hash,
        height: h.height,
    })
}

fn point_to_blockref(point: &ChainPoint) -> u5c::sync::BlockRef {
    match point {
        ChainPoint::Origin => u5c::sync::BlockRef {
            slot: 0,
            hash: vec![].into(),
            height: 0,
        },
        ChainPoint::Specific(slot, hash) => u5c::sync::BlockRef {
            slot: *slot,
            hash: hash.to_vec().into(),
            // TODO(p): implement height
            height: 0,
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
            LogValue::Mark(ChainPoint::Specific(slot, hash)) => {
                u5c::sync::follow_tip_response::Action::Reset(BlockRef {
                    hash: hash.to_vec().into(),
                    slot: *slot,
                    // TODO(p): implement height
                    height: 0,
                })
                .into()
            }
            LogValue::Mark(ChainPoint::Origin) => {
                u5c::sync::follow_tip_response::Action::Reset(BlockRef {
                    hash: vec![].into(),
                    slot: 0,
                    height: 0,
                })
                .into()
            }
        },
    }
}

pub struct SyncServiceImpl<D: Domain, C: CancelToken> {
    domain: D,
    mapper: interop::Mapper<super::ContextAdapter<D::State>>,
    cancel: C,
}

impl<D: Domain, C: CancelToken> SyncServiceImpl<D, C> {
    pub fn new(domain: D, cancel: C) -> Self {
        let mapper = Mapper::new(super::ContextAdapter(domain.state().clone()));

        Self {
            domain,
            mapper,
            cancel,
        }
    }
}

#[async_trait::async_trait]
impl<D, C> u5c::sync::sync_service_server::SyncService for SyncServiceImpl<D, C>
where
    D: Domain,
    C: CancelToken,
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
                    .get_block_by_slot(&br.slot)
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

        let from = msg.start_token.map(|x| x.slot);

        if msg.max_items > MAX_DUMP_HISTORY_ITEMS {
            return Err(Status::invalid_argument(format!(
                "max_items must be less than or equal to {MAX_DUMP_HISTORY_ITEMS}"
            )));
        }

        let len = msg.max_items as usize;

        let mut range = self
            .domain
            .archive()
            .get_range(from, None)
            .map_err(|_| Status::internal("cant query archive"))?;

        let items = range
            .by_ref()
            .take(len)
            .map(|(_, body)| raw_to_anychain(&self.mapper, &body))
            .collect();

        let next_token = range
            .next()
            .and_then(|(_, body)| raw_to_blockref(&self.mapper, &body));

        let response = u5c::sync::DumpHistoryResponse {
            block: items,
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
            .try_collect()?;

        // let (stream, point) = super::stream::ChainStream::<D>::start(
        //     self.domain.wal().clone(),
        //     self.domain.archive().clone(),
        //     &intersect,
        // );

        let stream = super::stream::ChainStream::start::<D, _>(
            self.domain.wal().clone(),
            self.domain.archive().clone(),
            intersect.clone(),
            self.cancel.clone(),
        );

        let mapper = self.mapper.clone();

        let stream = stream.map(move |log| Ok(wal_log_to_tip_response(&mapper, &log)));

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
            // TODO: impl
            timestamp: 0,
        };

        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_testing::toy_domain::ToyDomain;
    use pallas::interop::utxorpc::spec::sync::sync_service_server::SyncService as _;

    #[tokio::test]
    async fn test_dump_history_pagination() {
        let domain = ToyDomain::new(None, None);
        let cancel = CancelTokenImpl::default();

        for i in 0..34 {
            let block = dolos_testing::blocks::make_conway_block(i);
            domain.apply_blocks(&[block]).unwrap();
        }

        let service = SyncServiceImpl::new(domain, cancel);

        let mut start_token = None;

        for _ in 0..3 {
            let request = u5c::sync::DumpHistoryRequest {
                start_token,
                max_items: 10,
                field_mask: None,
            };

            let response = service
                .dump_history(Request::new(request))
                .await
                .unwrap()
                .into_inner();

            assert_eq!(response.block.len(), 10);

            start_token = response.next_token;
        }

        let request = u5c::sync::DumpHistoryRequest {
            start_token,
            max_items: 10,
            field_mask: None,
        };

        let response = service
            .dump_history(Request::new(request))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.block.len(), 4);
        assert_eq!(response.next_token, None);
    }

    #[tokio::test]
    async fn test_dump_history_max_items() {
        let domain = ToyDomain::new(None, None);
        let cancel = CancelTokenImpl::default();

        let service = SyncServiceImpl::new(domain, cancel);

        let request = u5c::sync::DumpHistoryRequest {
            start_token: None,
            max_items: MAX_DUMP_HISTORY_ITEMS + 1,
            field_mask: None,
        };

        let response = service
            .dump_history(Request::new(request))
            .await
            .unwrap_err();

        assert_eq!(response.code(), tonic::Code::InvalidArgument);
    }
}
