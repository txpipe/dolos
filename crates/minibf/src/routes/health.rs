use axum::{extract::State, http::StatusCode, Json};
use dolos_core::Domain;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::Facade;

/// Blockfrost's `/health` body, and deliberately nothing more.
///
/// The conformance suite compares this response with `toStrictEqual`, so an
/// extra field here is a conformance failure. The staleness detail a caller
/// needs lives on [`TipResponse`] instead; what `/health` carries is the
/// verdict, which is the part a load balancer reads.
#[derive(Debug, Serialize, Deserialize)]
pub struct RootResponse {
    pub is_healthy: bool,
}

/// `/health/tip` — how current the node's own chain is.
///
/// A sibling of `/health/clock`, which already establishes that this server
/// answers time-correctness questions about itself.
#[derive(Debug, Serialize, Deserialize)]
pub struct TipResponse {
    /// Slot of the node's own tip.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tip_slot: Option<u64>,

    /// Wall-clock seconds between the tip's slot and now.
    ///
    /// The whole point of the endpoint: it lets a caller tell a current node
    /// from one that has been serving the same block for hours, without
    /// holding a second opinion about where the chain actually is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tip_age_seconds: Option<u64>,

    /// The configured threshold, echoed so a caller can see what verdict it is
    /// being given against. `None` when the operator set none.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tip_age_seconds: Option<u64>,

    /// Whether `tip_age_seconds` is past `max_tip_age_seconds`. Absent when no
    /// threshold is configured, or when the tip could not be measured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_stale: Option<bool>,
}

/// Age of the node's tip in wall-clock seconds, or `None` when the tip or the
/// era summary cannot be read.
///
/// Staleness reporting is strictly additive to liveness, so a failure to
/// measure it must not turn a reachable node into an unhealthy one: every
/// error here degrades to "unknown age" rather than propagating.
fn tip_age_seconds<D: Domain>(domain: &Facade<D>) -> Option<(u64, u64)> {
    let tip_slot = domain.get_tip_slot().ok()?;
    let summary = domain.get_chain_summary().ok()?;

    let tip_time = summary.slot_time(tip_slot);

    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();

    Some((tip_slot, now.saturating_sub(tip_time)))
}

/// Whether the node's tip is past the configured staleness threshold.
///
/// `None` means "no verdict": either the operator configured no threshold, or
/// the tip could not be measured. A node that has fallen behind is only
/// *unhealthy* if someone said how far behind is too far — a node catching up
/// from a bootstrap is hours behind by design, so defaulting to a threshold
/// would pull every such node out of its load balancer mid-sync.
fn is_stale<D: Domain>(domain: &Facade<D>, tip_age: Option<u64>) -> Option<bool> {
    match (domain.config.max_tip_age_sec(), tip_age) {
        (Some(max), Some(age)) => Some(age > max),
        _ => None,
    }
}

pub async fn naked<D: Domain>(State(domain): State<Facade<D>>) -> (StatusCode, Json<RootResponse>) {
    let tip_age = tip_age_seconds(&domain).map(|(_, age)| age);

    let stale = is_stale(&domain, tip_age).unwrap_or(false);

    let status = if stale {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };

    (status, Json(RootResponse { is_healthy: !stale }))
}

pub async fn tip<D: Domain>(State(domain): State<Facade<D>>) -> Json<TipResponse> {
    let (tip_slot, tip_age) = match tip_age_seconds(&domain) {
        Some((slot, age)) => (Some(slot), Some(age)),
        None => (None, None),
    };

    Json(TipResponse {
        tip_slot,
        tip_age_seconds: tip_age,
        max_tip_age_seconds: domain.config.max_tip_age_sec(),
        is_stale: is_stale(&domain, tip_age),
    })
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClockResponse {
    server_time: u128,
}

impl Default for ClockResponse {
    fn default() -> Self {
        let now = SystemTime::now();
        let duration_since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");

        let server_time = duration_since_epoch.as_millis();
        Self { server_time }
    }
}

pub async fn clock() -> Result<Json<ClockResponse>, StatusCode> {
    Ok(Json(ClockResponse::default()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};

    async fn get_json<T: serde::de::DeserializeOwned>(
        app: &TestApp,
        path: &str,
    ) -> (StatusCode, T) {
        let (status, bytes) = app.get_bytes(path).await;
        let parsed = serde_json::from_slice(&bytes).unwrap_or_else(|err| {
            panic!(
                "failed to parse {path} response ({err}): {}",
                String::from_utf8_lossy(&bytes)
            )
        });

        (status, parsed)
    }

    /// The synthetic chain sits at a slot whose wall-clock time is years in the
    /// past, so it is stale by any threshold an operator would set — which is
    /// what makes it a usable fixture for the staleness cases below.
    #[tokio::test]
    async fn health_stays_blockfrost_exact() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/health").await;

        assert_eq!(status, StatusCode::OK);

        // Parsed as raw JSON, not into `RootResponse`, which would accept an
        // extra field silently and let the conformance regression through.
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("valid json");
        assert_eq!(body, serde_json::json!({ "is_healthy": true }));
    }

    #[tokio::test]
    async fn health_fails_once_the_tip_is_older_than_the_threshold() {
        let app = TestApp::new_with_minibf_config(|cfg| cfg.with_max_tip_age_sec(60));
        let (status, body) = get_json::<RootResponse>(&app, "/health").await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(!body.is_healthy);
    }

    #[tokio::test]
    async fn health_passes_when_the_tip_is_within_the_threshold() {
        let app = TestApp::new_with_minibf_config(|cfg| cfg.with_max_tip_age_sec(u64::MAX));
        let (status, body) = get_json::<RootResponse>(&app, "/health").await;

        assert_eq!(status, StatusCode::OK);
        assert!(body.is_healthy);
    }

    #[tokio::test]
    async fn tip_reports_age_without_a_threshold() {
        let app = TestApp::new();
        let (status, body) = get_json::<TipResponse>(&app, "/health/tip").await;

        assert_eq!(status, StatusCode::OK);

        assert_eq!(body.is_stale, None);
        assert_eq!(body.max_tip_age_seconds, None);

        assert!(body.tip_slot.is_some(), "tip slot should be reported");
        assert!(
            body.tip_age_seconds.is_some_and(|age| age > 0),
            "a tip in the past should report a non-zero age, got {:?}",
            body.tip_age_seconds
        );
    }

    #[tokio::test]
    async fn tip_judges_against_a_configured_threshold() {
        let app = TestApp::new_with_minibf_config(|cfg| cfg.with_max_tip_age_sec(60));
        let (status, body) = get_json::<TipResponse>(&app, "/health/tip").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.is_stale, Some(true));
        assert_eq!(body.max_tip_age_seconds, Some(60));
        assert!(body.tip_age_seconds.is_some_and(|age| age > 60));
    }

    /// Staleness reporting is additive to liveness: a node whose tip cannot be
    /// read is unmeasurable, not unhealthy, so `/health` keeps its original
    /// meaning rather than inventing a failure.
    #[tokio::test]
    async fn an_unreadable_tip_is_unmeasurable_not_unhealthy() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));

        let (status, body) = get_json::<RootResponse>(&app, "/health").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.is_healthy);

        let (status, body) = get_json::<TipResponse>(&app, "/health/tip").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.tip_slot, None);
        assert_eq!(body.tip_age_seconds, None);
        assert_eq!(body.is_stale, None);
    }
}
