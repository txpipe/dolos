use std::{ops::Range, sync::Arc};

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
    Router,
};
use dolos_core::{config::MinibfConfig, import::ImportExt as _};
use dolos_testing::{
    fixtures::hardano::{load_immutable_blocks, IMMUTABLE_BLOCK_RANGE},
    toy_domain::ToyDomain,
};
use http_body_util::BodyExt;
use tower::util::ServiceExt;

use crate::build_router;

pub use dolos_testing::fixtures::hardano::KNOWN_TX_HASH;

pub struct TestDomainBuilder {
    domain: ToyDomain,
}

impl TestDomainBuilder {
    pub fn new() -> Self {
        Self {
            domain: ToyDomain::new_with_genesis(
                Arc::new(dolos_cardano::include::preview::load()),
                None,
                None,
            ),
        }
    }

    pub fn with_immutable(self, range: Range<u64>) -> Self {
        let blocks = load_immutable_blocks(range);
        let domain = self.domain;
        domain
            .import_blocks(blocks.clone())
            .expect("failed to import fixture blocks");

        Self { domain }
    }

    pub fn finish(self) -> ToyDomain {
        self.domain
    }
}

pub struct TestApp {
    router: Router,
    _domain: ToyDomain,
}

impl TestApp {
    pub fn new() -> Self {
        let domain = TestDomainBuilder::new()
            .with_immutable(IMMUTABLE_BLOCK_RANGE.clone())
            .finish();

        let cfg = MinibfConfig {
            listen_address: "[::]:0".parse().expect("invalid listen address"),
            permissive_cors: None,
            token_registry_url: None,
            url: None,
        };

        let router = build_router(cfg, domain.clone());

        Self {
            router,
            _domain: domain,
        }
    }

    pub async fn get_bytes(&self, path: &str) -> (StatusCode, Vec<u8>) {
        let req = Request::builder()
            .method(Method::GET)
            .uri(path)
            .body(Body::empty())
            .expect("failed to build request");

        let res = self
            .router
            .clone()
            .oneshot(req)
            .await
            .expect("request failed");

        let status = res.status();
        let bytes = res
            .into_body()
            .collect()
            .await
            .expect("failed to read response body")
            .to_bytes();
        (status, bytes.to_vec())
    }
}
