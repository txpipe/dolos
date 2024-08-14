use pallas::ledger::traverse::MultiEraBlock;
use serde_json::json;
use std::{collections::HashSet, path::Path};
use thiserror::Error;

wasmtime::component::bindgen!();

mod adapter;
mod loader;
mod router;
mod store;

use loader::Loader;
use router::Router;
use store::Store;

pub type WorkerId = String;

#[derive(Error, Debug)]
pub enum Error {
    #[error("wasm error {0}")]
    Wasm(wasmtime::Error),

    #[error("store error {0}")]
    Store(redb::Error),

    #[error("worker not found '{0}'")]
    WorkerNotFound(WorkerId),

    #[error("worker failed to handle event (code: '{0}')")]
    Handle(u32),

    #[error("no target available to solve request")]
    NoTarget,

    #[error("more than one target available to solve request")]
    AmbiguousTarget,
}

impl From<wasmtime::Error> for Error {
    fn from(value: wasmtime::Error) -> Self {
        Self::Wasm(value)
    }
}

impl From<redb::Error> for Error {
    fn from(value: redb::Error) -> Self {
        Self::Store(value)
    }
}

impl From<redb::DatabaseError> for Error {
    fn from(value: redb::DatabaseError) -> Self {
        Self::Store(value.into())
    }
}

impl From<redb::TransactionError> for Error {
    fn from(value: redb::TransactionError) -> Self {
        Self::Store(value.into())
    }
}

impl From<redb::TableError> for Error {
    fn from(value: redb::TableError) -> Self {
        Self::Store(value.into())
    }
}

impl From<redb::StorageError> for Error {
    fn from(value: redb::StorageError) -> Self {
        Self::Store(value.into())
    }
}

pub type BlockSlot = u64;
pub type BlockHash = pallas::crypto::hash::Hash<32>;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ChainPoint(pub BlockSlot, pub BlockHash);

pub type LogSeq = u64;

#[derive(Clone)]
pub struct Runtime {
    loader: Loader,
    router: Router,
    store: Store,
}

impl Runtime {
    pub fn new(store: Store) -> Result<Self, Error> {
        let router = Router::new();

        Ok(Self {
            loader: Loader::new(router.clone())?,
            router,
            store,
        })
    }

    pub fn cursor(&self) -> Result<Option<LogSeq>, Error> {
        let cursor = self.store.lowest_cursor()?;

        Ok(cursor)
    }

    pub fn register_worker(&mut self, id: &str, wasm_path: impl AsRef<Path>) -> Result<(), Error> {
        self.loader.register_worker(id, wasm_path)?;

        Ok(())
    }

    fn fire_and_forget(
        &mut self,
        event: &Event,
        targets: HashSet<router::Target>,
    ) -> Result<(), Error> {
        for target in targets {
            let result = self
                .loader
                .dispatch_event(&target.worker, target.channel, event);

            match result {
                Ok(Response::Acknowledge) => {
                    tracing::debug!(worker = target.worker, "worker acknowledge");
                }
                Ok(_) => {
                    tracing::warn!(worker = target.worker, "worker returned unexpected data");
                }
                Err(Error::Handle(code)) => {
                    tracing::warn!(code, "worker error");
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    pub fn apply_block(&self, block: &MultiEraBlock, wal_seq: LogSeq) -> Result<(), Error> {
        Ok(())
    }

    pub fn undo_block(&self, block: &MultiEraBlock) -> Result<(), Error> {
        Ok(())
    }

    pub fn handle_request(
        &self,
        worker: &str,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        let target = self.router.find_request_target(worker, method)?;

        let evt = Event::Request(serde_json::to_vec(&params).unwrap());

        let reply = self
            .loader
            .dispatch_event(&target.worker, target.channel, &evt)?;

        let json = match reply {
            Response::Acknowledge => json!({}),
            Response::Json(x) => serde_json::from_slice(&x).unwrap(),
            Response::Cbor(x) => json!({ "cbor": x }),
            Response::PartialTx(x) => json!({ "tx": x }),
        };

        Ok(json)
    }
}
