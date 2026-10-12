mod eras;
mod naked;

pub use eras::eras;
pub use naked::naked;

#[cfg(test)]
mod testing {
    use axum::http::StatusCode;

    use crate::test_support::TestApp;

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
