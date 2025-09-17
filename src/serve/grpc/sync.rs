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
        ..Default::default()
    })
}

fn point_to_blockref(point: &ChainPoint) -> u5c::sync::BlockRef {
    BlockRef {
        hash: point.hash().map(|h| h.to_vec()).unwrap_or_default().into(),
        slot: point.slot(),
        ..Default::default()
    }
}

fn tip_event_to_response<C: LedgerContext>(
    mapper: &Mapper<C>,
    event: &TipEvent,
) -> u5c::sync::FollowTipResponse {
    u5c::sync::FollowTipResponse {
        action: match event {
            TipEvent::Apply(_, block) => {
                u5c::sync::follow_tip_response::Action::Apply(raw_to_anychain(mapper, &block))
                    .into()
            }
            TipEvent::Undo(_, block) => {
                u5c::sync::follow_tip_response::Action::Undo(raw_to_anychain(mapper, &block)).into()
            }
            TipEvent::Mark(x) => {
                u5c::sync::follow_tip_response::Action::Reset(point_to_blockref(x)).into()
            }
        },
    }
}

pub struct SyncServiceImpl<D, C>
where
    D: Domain + LedgerContext,
    C: CancelToken,
{
    domain: D,
    mapper: interop::Mapper<D>,
    cancel: C,
}

impl<D, C> SyncServiceImpl<D, C>
where
    D: Domain + LedgerContext,
    C: CancelToken,
{
    pub fn new(domain: D, cancel: C) -> Self {
        let mapper = Mapper::new(domain.clone());

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
    D: Domain + LedgerContext,
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
            self.domain.clone(),
            intersect.clone(),
            self.cancel.clone(),
        );

        let mapper = self.mapper.clone();

        let stream = stream.map(move |log| Ok(tip_event_to_response(&mapper, &log)));

        Ok(Response::new(Box::pin(stream)))
    }

    async fn read_tip(
        &self,
        _request: tonic::Request<u5c::sync::ReadTipRequest>,
    ) -> std::result::Result<tonic::Response<u5c::sync::ReadTipResponse>, tonic::Status> {
        let (point, _) = self
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
    use dolos_testing::toy_domain::ToyDomain;
    use pallas::interop::utxorpc::spec::sync::sync_service_server::SyncService as _;

    use crate::facade::DomainExt as _;

    use super::*;

    #[tokio::test]
    async fn test_dump_history_pagination() {
        let domain = ToyDomain::new(None, None);
        let cancel = CancelTokenImpl::default();

        for i in 0..34 {
            let (_, block) = dolos_testing::blocks::make_conway_block(i);
            domain.roll_forward(&block).unwrap();
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
