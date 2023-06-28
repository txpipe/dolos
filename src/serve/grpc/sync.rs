use futures_core::Stream;
use gasket::messaging::Message;
use pallas::crypto::hash::Hash;
use std::pin::Pin;
use tokio::sync::broadcast::Receiver;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};
use utxorpc::proto::sync::v1::*;

use crate::{
    prelude::RollEvent,
    storage::rolldb::{wal::WalAction, RollDB},
};

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

fn roll_to_tip_response(
    evt: crate::storage::rolldb::wal::Value,
    block: &[u8],
) -> FollowTipResponse {
    utxorpc::proto::sync::v1::FollowTipResponse {
        action: match evt.action() {
            WalAction::Apply => follow_tip_response::Action::Apply(raw_to_anychain(block)).into(),
            WalAction::Undo => follow_tip_response::Action::Undo(raw_to_anychain(block)).into(),
            WalAction::Mark => None,
        },
    }
}

pub struct ChainSyncServiceImpl(RollDB, Receiver<Message<RollEvent>>);

impl ChainSyncServiceImpl {
    pub fn new(db: RollDB, sync_events: Receiver<Message<RollEvent>>) -> Self {
        Self(db, sync_events)
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
            .map(|hash| self.0.get_block(hash))
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
            .0
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
            .map(|(_, hash)| self.0.get_block(hash))
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

    // async fn follow_tip(
    //     &self,
    //     _request: Request<FollowTipRequest>,
    // ) -> Result<Response<Self::FollowTipStream>, tonic::Status> {
    //     let (tx, rx) = tokio::sync::mpsc::channel(1);

    //     let db2 = self.0.clone();

    //     tokio::spawn(async move {
    //         let iter = db2.crawl_from_origin();
    //         let mut last_seq = None;

    //         for x in iter {
    //             if let Ok((val, seq)) = x {
    //                 let val = roll_to_tip_response(val, &db2);
    //                 tx.send(val).await.unwrap();
    //                 last_seq = seq;
    //             }
    //         }

    //         loop {
    //             db2.tip_change.notified().await;
    //             let iter = db2.crawl_wal(last_seq).skip(1);

    //             for x in iter {
    //                 if let Ok((seq, val)) = x {
    //                     let val = roll_to_tip_response(val, &db2);
    //                     tx.send(val).await.unwrap();
    //                     last_seq = Some(seq);
    //                 }
    //             }
    //         }
    //     });

    //     let rx = tokio_stream::wrappers::ReceiverStream::new(rx);

    //     Ok(Response::new(Box::pin(rx)))
    // }

    async fn follow_tip(
        &self,
        _request: Request<FollowTipRequest>,
    ) -> Result<Response<Self::FollowTipStream>, tonic::Status> {
        let s = crate::storage::rolldb::stream::RollStream::start_with_block(self.0.clone(), None)
            .map(|(evt, block)| Ok(roll_to_tip_response(evt, &block)));

        Ok(Response::new(Box::pin(s)))
    }
}
