use rocket::{http::Status, post, State};

use crate::mempool::{Mempool, MempoolError};

#[post("/tx/submit", format = "application/cbor", data = "<cbor>")]
pub fn route(cbor: Vec<u8>, mempool: &State<Mempool>) -> Result<String, Status> {
    let hash = mempool.receive_raw(&cbor).map_err(|e| {
        tracing::warn!(error = e.to_string(), "failed to submit transaction.");
        match e {
            MempoolError::Phase1Error(_) => Status::BadRequest,
            MempoolError::Phase2Error(_) => Status::BadRequest,
            MempoolError::InvalidTx(_) => Status::BadRequest,
            MempoolError::TraverseError(_) => Status::BadRequest,
            MempoolError::StateError(_) => Status::InternalServerError,
            MempoolError::DecodeError(_) => Status::BadRequest,
            MempoolError::PlutusNotSupported => Status::BadRequest,
        }
    })?;
    Ok(hex::encode(hash))
}
