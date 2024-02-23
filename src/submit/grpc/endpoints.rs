use futures_core::Stream;
use gasket::framework::*;
use gasket::messaging::{tokio::ChannelSendAdapter, SendAdapter};
use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::MultiEraTx;
use std::collections::HashMap;
use std::ops::Deref;
use std::path::PathBuf;
use std::{pin::Pin, sync::Arc};
use tokio::sync::{Notify, RwLock};
use tonic::transport::{Certificate, Server, ServerTlsConfig};
use tonic::{Request, Response, Status};
use tracing::info;
use utxorpc_spec::utxorpc;
use utxorpc_spec::utxorpc::v1alpha::submit::submit_service_server::SubmitServiceServer;
use utxorpc_spec::utxorpc::v1alpha::submit::{Stage as SubmitStage, WaitForTxResponse, *};

use crate::prelude::Error;

use super::mempool::Monitor;
use super::Transaction;

pub struct SubmitServiceImpl {
    channel: ChannelSendAdapter<Vec<Transaction>>,
    mempool_view: Arc<RwLock<Monitor>>,
    change_notify: Arc<Notify>,
}

impl SubmitServiceImpl {
    pub fn new(
        channel: ChannelSendAdapter<Vec<Transaction>>,
        mempool_view: Arc<RwLock<Monitor>>,
        change_notify: Arc<Notify>,
    ) -> Self {
        Self {
            channel,
            mempool_view: mempool_view,
            change_notify: change_notify,
        }
    }
}

#[async_trait::async_trait]
impl submit_service_server::SubmitService for SubmitServiceImpl {
    type WaitForTxStream =
        Pin<Box<dyn Stream<Item = Result<WaitForTxResponse, tonic::Status>> + Send + 'static>>;

    type WatchMempoolStream =
        Pin<Box<dyn Stream<Item = Result<WatchMempoolResponse, tonic::Status>> + Send + 'static>>;

    async fn submit_tx(
        &self,
        request: Request<SubmitTxRequest>,
    ) -> Result<Response<SubmitTxResponse>, Status> {
        let message = request.into_inner();

        info!("received new grpc submit tx request: {:?}", message);

        let mut received = vec![];

        for (idx, tx_bytes) in message.tx.into_iter().flat_map(|x| x.r#type).enumerate() {
            match tx_bytes {
                any_chain_tx::Type::Raw(bytes) => {
                    let decoded = MultiEraTx::decode(&bytes).map_err(|e| {
                        Status::invalid_argument(
                            format! {"could not decode tx at index {idx}: {e}"},
                        )
                    })?;

                    let hash = decoded.hash();

                    // TODO: we don't phase-2 validate txs before propagating so we could
                    // propagate p2 invalid transactions resulting in collateral loss
                    if !decoded.redeemers().is_empty() {
                        return Err(Status::invalid_argument(
                            "txs interacting with plutus scripts not yet supported",
                        ));
                    }

                    received.push(Transaction {
                        hash,
                        era: 5, // TODO: tx decoding as conway, then era is invalid for mainnet
                        bytes: bytes.into(),
                    })
                }
            }
        }

        let hashes = received.iter().map(|x| x.hash.to_vec().into()).collect();

        self.channel
            .clone()
            .send(received.into())
            .await
            .map_err(|_| Status::internal("couldn't add txs to mempool"))?;

        Ok(Response::new(SubmitTxResponse { r#ref: hashes }))
    }

    async fn wait_for_tx(
        &self,
        request: Request<WaitForTxRequest>,
    ) -> Result<Response<Self::WaitForTxStream>, Status> {
        let mempool_view_rwlock = self.mempool_view.clone();
        let change_notifier = self.change_notify.clone();

        Ok(Response::new(Box::pin(async_stream::stream! {
            let tx_refs = request.into_inner().r#ref;

            let mut last_update: HashMap<&[u8; 32], Option<SubmitStage>> = HashMap::new();

            let mut tx_hashes = vec![];

            for tx_ref in tx_refs {
                let tx_hash: [u8; 32] = tx_ref
                    .deref()
                    .try_into()
                    .map_err(|_| Status::invalid_argument("tx hash malformed"))?;

                tx_hashes.push(tx_hash)
            }

            last_update.extend(tx_hashes.iter().map(|x| (x, None)));

            info!("starting wait_for_tx async stream for tx hashes: {:?}", tx_hashes.iter().map(|x| Hash::new(*x)).collect::<Vec<_>>());

            loop {
                change_notifier.notified().await;

                for hash in tx_hashes.iter() {
                    let mempool_view = mempool_view_rwlock.read().await;

                    let stage = if let Some(maybe_inclusion) = mempool_view.txs.get(&(*hash).into()) {
                        if let Some(inclusion) = maybe_inclusion {
                            // TODO: spec does not have way to detail number of confirmations
                            let _confirmations = mempool_view.tip_height - inclusion;

                            // tx is included on chain
                            SubmitStage::Confirmed
                        } else {
                            // tx has been propagated but not included on chain
                            SubmitStage::Mempool
                        }
                    } else {
                        // tx hash provided has not been passed to propagators
                        SubmitStage::Unspecified // TODO: what stage should be used here?
                    };

                    // if stage changed since we last informed user, send user update
                    match last_update.get(&hash).unwrap() {
                        Some(last_stage) if (*last_stage == stage) => (),
                        _ => {
                            let response = WaitForTxResponse {
                                r#ref: hash.to_vec().into(),
                                stage: stage.into()
                            };

                            yield Ok(response);

                            last_update.insert(hash, Some(stage));
                        }
                    }
                }
            }
        })))
    }

    async fn read_mempool(
        &self,
        _request: tonic::Request<ReadMempoolRequest>,
    ) -> Result<tonic::Response<ReadMempoolResponse>, tonic::Status> {
        todo!()
    }

    async fn watch_mempool(
        &self,
        _request: tonic::Request<WatchMempoolRequest>,
    ) -> Result<tonic::Response<Self::WatchMempoolStream>, tonic::Status> {
        todo!()
    }
}

#[derive(Stage)]
#[stage(name = "endpoints", unit = "()", worker = "Worker")]
pub struct Stage {
    listen_address: String,
    tls_client_ca_root: Option<PathBuf>,
    send_channel: ChannelSendAdapter<Vec<Transaction>>,
    mempool_view: Arc<RwLock<Monitor>>,
    change_notify: Arc<Notify>,
    // #[metric]
    // received_txs: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(
        listen_address: String,
        tls_client_ca_root: Option<PathBuf>,
        send_channel: ChannelSendAdapter<Vec<Transaction>>,
        mempool_view: Arc<RwLock<Monitor>>,
        change_notify: Arc<Notify>,
    ) -> Self {
        Self {
            listen_address,
            tls_client_ca_root,
            send_channel,
            mempool_view,
            change_notify,
        }
    }
}

pub struct Worker {}

impl Worker {}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(_stage: &Stage) -> Result<Self, WorkerError> {
        Ok(Self {})
    }

    async fn schedule(&mut self, _stage: &mut Stage) -> Result<WorkSchedule<()>, WorkerError> {
        Ok(WorkSchedule::Unit(()))
    }

    async fn execute(&mut self, _unit: &(), stage: &mut Stage) -> Result<(), WorkerError> {
        let addr = stage.listen_address.parse().or_panic()?;

        let service = SubmitServiceImpl::new(
            stage.send_channel.clone(),
            stage.mempool_view.clone(),
            stage.change_notify.clone(),
        );
        let service = SubmitServiceServer::new(service);

        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(utxorpc::v1alpha::cardano::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(utxorpc::v1alpha::submit::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(protoc_wkt::google::protobuf::FILE_DESCRIPTOR_SET)
            .build()
            .unwrap();

        let mut server = Server::builder().accept_http1(true);

        if let Some(pem) = stage.tls_client_ca_root.clone() {
            let pem = std::env::current_dir().unwrap().join(pem);
            let pem = std::fs::read_to_string(pem)
                .map_err(Error::config)
                .or_panic()?;
            let pem = Certificate::from_pem(pem);

            let tls = ServerTlsConfig::new().client_ca_root(pem);

            server = server.tls_config(tls).map_err(Error::config).or_panic()?;
        }

        info!("serving via gRPC on address: {}", stage.listen_address);

        let _ = server
            // GrpcWeb is over http1 so we must enable it.
            .add_service(tonic_web::enable(service))
            .add_service(reflection)
            .serve(addr)
            .await;

        Ok(())
    }
}
