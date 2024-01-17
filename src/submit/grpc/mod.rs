use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use futures_util::future::join;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tonic::transport::{Certificate, Server, ServerTlsConfig};

use pallas::{
    crypto::hash::Hash,
    network::{
        facades::PeerClient,
        miniprotocols::{
            chainsync::Tip,
            txsubmission::{EraTxBody, EraTxId, TxIdAndSize},
        },
    },
};
use tracing::info;
use utxorpc::proto::submit::v1::submit_service_server::SubmitServiceServer;

use crate::{prelude::*, submit::grpc::submit::SubmitPeerHandler};

mod submit;

#[derive(Clone, Debug)]
pub struct Transaction {
    hash: Hash<32>,
    era: u16,
    bytes: Vec<u8>,
}

impl Into<TxIdAndSize<EraTxId>> for Transaction {
    fn into(self) -> TxIdAndSize<EraTxId> {
        TxIdAndSize(
            EraTxId(self.era, self.hash.to_vec()),
            self.bytes.len() as u32,
        )
    }
}

impl Into<EraTxBody> for Transaction {
    fn into(self) -> EraTxBody {
        EraTxBody(self.era, self.bytes)
    }
}

pub struct InclusionPoint {
    slot: u64,
    height: u64,
}

pub struct Mempool {
    pub tip: Option<Tip>,
    pub to_send: VecDeque<Transaction>,
    pub unacked: VecDeque<Transaction>,
    pub acked: Vec<(Transaction, Option<InclusionPoint>)>,
}

impl Mempool {
    fn new() -> Self {
        Self {
            tip: None,
            to_send: VecDeque::new(),
            unacked: VecDeque::new(),
            acked: Vec::new(),
        }
    }

    fn add_txs(&mut self, txs: Vec<Transaction>) {
        self.to_send.extend(txs)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    listen_address: String,
    peer_address: String,
    peer_magic: u64,
    tls_client_ca_root: Option<PathBuf>,
}

// we need to watch new blocks from the peer to detect acknowledged txs in new blocks OR
// we need to take txs from a mempool and propagate them to the peer
// we need to receive txs from gRPC and add them to a mempool

pub async fn serve(config: Config) -> Result<(), Error> {
    let addr = config.listen_address.parse().unwrap();

    // gRPC sends new transactions through `tx`
    // propagator receives transactions through `rx`
    let (send_channel, receive_channel) = mpsc::channel(64);

    // propagator progresses txs through mempool -> unacked -> acked -> (pruned)
    let mempool = Arc::new(Mutex::new(Mempool::new()));

    // gRPC service

    let service = submit::SubmitServiceImpl::new(send_channel, mempool.clone());
    let service = SubmitServiceServer::new(service);

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(utxorpc::proto::cardano::v1::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(utxorpc::proto::submit::v1::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(protoc_wkt::google::protobuf::FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    let mut server = Server::builder().accept_http1(true);

    if let Some(pem) = config.tls_client_ca_root {
        let pem = std::env::current_dir().unwrap().join(pem);
        let pem = std::fs::read_to_string(pem).map_err(Error::config)?;
        let pem = Certificate::from_pem(pem);

        let tls = ServerTlsConfig::new().client_ca_root(pem);

        server = server.tls_config(tls).map_err(Error::config)?;
    }

    info!("serving via gRPC on address: {}", config.listen_address);

    let grpc_server = server
        // GrpcWeb is over http1 so we must enable it.
        .add_service(tonic_web::enable(service))
        .add_service(reflection)
        .serve(addr);

    // handle propagating txs to peer and watching new blocks

    let peer = PeerClient::connect(config.peer_address, config.peer_magic)
        .await
        .map_err(|_| Error::config("could not connect to peer"))?;

    let mut peer_handler = SubmitPeerHandler::new(peer, receive_channel, mempool);

    // ---

    let _ = join(peer_handler.begin(), grpc_server);

    Ok(())
}
