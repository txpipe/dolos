use futures_core::Stream;
use futures_util::StreamExt;
use itertools::Itertools;
use pallas::interop::utxorpc::v1alpha as interop;
use pallas::interop::utxorpc::v1alpha::spec::sync::BlockRef;
use pallas::interop::utxorpc::v1alpha::{spec as u5c, Mapper};
use pallas::interop::utxorpc::LedgerContext;
use std::pin::Pin;
use tonic::{Request, Response, Status};

use crate::prelude::*;

const MAX_DUMP_HISTORY_ITEMS: u32 = 100;

fn u5c_to_chain_point(block_ref: u5c::sync::BlockRef) -> Result<ChainPoint, Status> {
    Ok(ChainPoint::Specific(
        block_ref.slot,
        crate::serve::grpc::convert::bytes_to_hash32(&block_ref.hash)?,
    ))
}

/// Selects which representations of an `AnyChainBlock` are populated.
///
/// `AnyChainBlock` can carry both the raw `native_bytes` and a fully parsed
/// chain block. Parsing (and serializing) the parsed block is the dominant cost
/// of a sync response, so we honor the request's `field_mask` to avoid doing
/// work the consumer didn't ask for.
#[derive(Clone, Copy)]
struct BlockMask {
    native_bytes: bool,
    chain: bool,
}

impl BlockMask {
    const fn all() -> Self {
        Self {
            native_bytes: true,
            chain: true,
        }
    }

    /// Interprets a `google.protobuf.FieldMask`'s paths against `AnyChainBlock`.
    ///
    /// An absent or empty mask selects everything (backwards compatible). Paths
    /// are matched leniently: a leading `block.` segment (referring to the
    /// repeated `block` field in the response message) is tolerated, as is a
    /// bare leaf name. Recognized leaves are `native_bytes` and the parsed
    /// chain (`cardano`/`chain`). Selecting the whole `block` field keeps both.
    fn from_paths(paths: &[String]) -> Self {
        if paths.is_empty() {
            return Self::all();
        }

        let mut mask = Self {
            native_bytes: false,
            chain: false,
        };

        for path in paths {
            // Tolerate the response-relative `block.` prefix.
            let field = path.strip_prefix("block.").unwrap_or(path);
            // Only the first segment matters for deciding what to populate.
            match field.split('.').next().unwrap_or("") {
                "native_bytes" => mask.native_bytes = true,
                "cardano" | "chain" => mask.chain = true,
                // The whole block (or an unrecognized empty path) keeps both.
                "" | "block" => return Self::all(),
                _ => {}
            }
        }

        mask
    }
}

fn raw_to_anychain<C: LedgerContext>(
    mapper: &Mapper<C>,
    body: &BlockBody,
    mask: BlockMask,
) -> u5c::sync::AnyChainBlock {
    u5c::sync::AnyChainBlock {
        native_bytes: if mask.native_bytes {
            body.to_vec().into()
        } else {
            Default::default()
        },
        chain: if mask.chain {
            u5c::sync::any_chain_block::Chain::Cardano(mapper.map_block_cbor(body)).into()
        } else {
            None
        },
    }
}

fn raw_to_blockref<C: LedgerContext>(
    mapper: &Mapper<C>,
    body: &BlockBody,
) -> Option<u5c::sync::BlockRef> {
    let block = mapper.map_block_cbor(body);
    let header = block.header?;

    Some(u5c::sync::BlockRef {
        slot: header.slot,
        hash: header.hash,
        height: header.height,
        timestamp: block.timestamp,
    })
}

fn point_to_blockref(point: &ChainPoint, timestamp: u64) -> u5c::sync::BlockRef {
    BlockRef {
        hash: point.hash().map(|h| h.to_vec()).unwrap_or_default().into(),
        slot: point.slot(),
        timestamp,
        ..Default::default()
    }
}

fn tip_event_to_response<C: LedgerContext>(
    mapper: &Mapper<C>,
    event: &TipEvent,
    mask: BlockMask,
) -> u5c::sync::FollowTipResponse {
    match event {
        TipEvent::Apply(_, block) => {
            let block_ref = raw_to_blockref(mapper, block);
            u5c::sync::FollowTipResponse {
                action: Some(u5c::sync::follow_tip_response::Action::Apply(
                    raw_to_anychain(mapper, block, mask),
                )),
                tip: block_ref,
            }
        }
        TipEvent::Undo(_, block) => u5c::sync::FollowTipResponse {
            action: Some(u5c::sync::follow_tip_response::Action::Undo(
                raw_to_anychain(mapper, block, mask),
            )),
            tip: None, // TODO: we don't have easy access to the new tip here
        },
        TipEvent::Mark(x) => u5c::sync::FollowTipResponse {
            action: Some(u5c::sync::follow_tip_response::Action::Reset(
                point_to_blockref(x, 0), // TODO: we don't have the timestamp here
            )),
            tip: Some(point_to_blockref(x, 0)),
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

        let mask = BlockMask::from_paths(
            message
                .field_mask
                .as_ref()
                .map(|m| m.paths.as_slice())
                .unwrap_or_default(),
        );

        let query = dolos_core::AsyncQueryFacade::new(self.domain.clone());

        let mut out = Vec::new();
        for br in message.r#ref.iter() {
            let mut body: Option<BlockBody> = None;

            if !br.hash.is_empty() {
                body = query
                    .block_by_hash(br.hash.to_vec())
                    .await
                    .map_err(|_| Status::internal("Failed to query chain service."))?;
            }

            if body.is_none() && br.height != 0 {
                body = query
                    .block_by_number(br.height)
                    .await
                    .map_err(|_| Status::internal("Failed to query chain service."))?;
            }

            if body.is_none() && br.slot != 0 {
                body = query
                    .block_by_slot(br.slot)
                    .await
                    .map_err(|_| Status::internal("Failed to query chain service."))?;
            }

            let Some(body) = body else {
                return Err(Status::not_found(format!("Failed to find block: {br:?}")));
            };

            out.push(raw_to_anychain(&self.mapper, &body, mask));
        }

        let response = u5c::sync::FetchBlockResponse { block: out };

        Ok(Response::new(response))
    }

    async fn dump_history(
        &self,
        request: Request<u5c::sync::DumpHistoryRequest>,
    ) -> Result<Response<u5c::sync::DumpHistoryResponse>, Status> {
        let msg = request.into_inner();

        let mask = BlockMask::from_paths(
            msg.field_mask
                .as_ref()
                .map(|m| m.paths.as_slice())
                .unwrap_or_default(),
        );

        let mut from = None;

        if let Some(ref br) = msg.start_token {
            let mut slot: Option<u64> = None;

            if !br.hash.is_empty() {
                slot = self
                    .domain
                    .indexes()
                    .slot_by_block_hash(&br.hash)
                    .map_err(|_| Status::internal("Failed to query chain service."))?;
            }

            if slot.is_none() && br.height != 0 {
                slot = self
                    .domain
                    .indexes()
                    .slot_by_block_number(br.height)
                    .map_err(|_| Status::internal("Failed to query chain service."))?;
            }

            if slot.is_none() && br.slot != 0 {
                slot = Some(br.slot);
            }

            from = match slot {
                Some(s) => Some(s),
                None if !br.hash.is_empty() || br.height != 0 || br.slot != 0 => {
                    return Err(Status::not_found(format!(
                        "Failed to find block for start_token: {br:?}"
                    )));
                }
                None => None,
            };
        }

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
            .map(|(_, body)| raw_to_anychain(&self.mapper, &body, mask))
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

        let mask = BlockMask::from_paths(
            request
                .field_mask
                .as_ref()
                .map(|m| m.paths.as_slice())
                .unwrap_or_default(),
        );

        let intersect: Vec<_> = request
            .intersect
            .into_iter()
            .map(u5c_to_chain_point)
            .try_collect()?;

        let stream = crate::serve::grpc::stream::ChainStream::start::<D, _>(
            self.domain.clone(),
            intersect.clone(),
            self.cancel.clone(),
        );

        let mapper = self.mapper.clone();

        let stream = stream.map(move |log| Ok(tip_event_to_response(&mapper, &log, mask)));

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

        let timestamp = self
            .domain
            .get_slot_timestamp(point.slot())
            .map(|s| s * 1000)
            .unwrap_or(0);
        let response = u5c::sync::ReadTipResponse {
            tip: Some(point_to_blockref(&point, timestamp)),
        };

        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use dolos_testing::toy_domain::ToyDomain;
    use pallas::interop::utxorpc::v1alpha::spec::sync::sync_service_server::SyncService as _;

    use super::*;

    #[tokio::test]
    async fn test_dump_history_pagination() {
        let domain = ToyDomain::new(None, None);
        let cancel = CancelTokenImpl::default();

        let batch = (0..34)
            .map(|i| dolos_testing::blocks::make_conway_block(i).1)
            .collect_vec();

        use dolos_core::ImportExt;
        domain.import_blocks(batch).unwrap();

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

    #[test]
    fn block_mask_from_paths_semantics() {
        // Absent/empty mask keeps everything.
        let all = BlockMask::from_paths(&[]);
        assert!(all.native_bytes && all.chain);

        // Bare leaf names.
        let bytes_only = BlockMask::from_paths(&["native_bytes".to_string()]);
        assert!(bytes_only.native_bytes && !bytes_only.chain);

        let chain_only = BlockMask::from_paths(&["cardano".to_string()]);
        assert!(!chain_only.native_bytes && chain_only.chain);

        // Response-relative `block.` prefix is tolerated (as the user reported).
        let prefixed = BlockMask::from_paths(&["block.native_bytes".to_string()]);
        assert!(prefixed.native_bytes && !prefixed.chain);

        // Deeper paths into the parsed block select the chain representation.
        let deep = BlockMask::from_paths(&["block.cardano.header".to_string()]);
        assert!(!deep.native_bytes && deep.chain);

        // Selecting the whole block keeps both.
        let whole = BlockMask::from_paths(&["block".to_string()]);
        assert!(whole.native_bytes && whole.chain);

        // Multiple paths accumulate.
        let both =
            BlockMask::from_paths(&["native_bytes".to_string(), "cardano".to_string()]);
        assert!(both.native_bytes && both.chain);
    }

    #[tokio::test]
    async fn dump_history_applies_field_mask() {
        let domain = ToyDomain::new(None, None);
        let cancel = CancelTokenImpl::default();

        let batch = (0..3)
            .map(|i| dolos_testing::blocks::make_conway_block(i).1)
            .collect_vec();

        use dolos_core::ImportExt;
        domain.import_blocks(batch).unwrap();

        let service = SyncServiceImpl::new(domain, cancel);

        let mut request = u5c::sync::DumpHistoryRequest {
            start_token: None,
            max_items: 10,
            field_mask: Some(Default::default()),
        };
        request.field_mask.as_mut().unwrap().paths = vec!["block.native_bytes".to_string()];

        let response = service
            .dump_history(Request::new(request))
            .await
            .unwrap()
            .into_inner();

        assert!(!response.block.is_empty());
        for block in response.block {
            assert!(!block.native_bytes.is_empty());
            assert!(block.chain.is_none());
        }
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
