use axum::http::StatusCode;

mod by_hash;
mod cbor;
mod delegations;
mod metadata;
mod metadata_cbor;
mod mirs;
mod pool_retires;
mod pool_updates;
mod redeemers;
mod required_signers;
mod stakes;
mod utxos;
mod withdrawals;

pub use by_hash::by_hash;
pub use cbor::by_hash_cbor;
pub use delegations::by_hash_delegations;
pub use metadata::by_hash_metadata;
pub use metadata_cbor::by_hash_metadata_cbor;
pub use mirs::by_hash_mirs;
pub use pool_retires::by_hash_pool_retires;
pub use pool_updates::by_hash_pool_updates;
pub use redeemers::by_hash_redeemers;
pub use required_signers::by_hash_required_signers;
pub use stakes::by_hash_stakes;
pub use utxos::by_hash_utxos;
pub use withdrawals::by_hash_withdrawals;

/// The tx hash of a `/txs/{hash}` path, or Blockfrost's `400` when it is not
/// hex.
fn parse_tx_hash(tx_hash: &str) -> Result<Vec<u8>, StatusCode> {
    hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)
}

#[cfg(test)]
mod testing {
    use axum::http::StatusCode;

    use crate::test_support::TestApp;

    pub const MISSING_HASH: &str =
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    pub const INVALID_HASH: &str = "not-a-hash";

    /// The body of a `200` response to `path`.
    pub async fn get_ok<T: serde::de::DeserializeOwned>(app: &TestApp, path: &str) -> T {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        serde_json::from_slice(&bytes).expect("failed to parse response")
    }

    pub async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
}
