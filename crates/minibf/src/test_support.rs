use std::sync::Arc;

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
    Router,
};
use dolos_core::{config::MinibfConfig, import::ImportExt as _};
use dolos_testing::{
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig, SyntheticVectors},
    toy_domain::ToyDomain,
};
use http_body_util::BodyExt;
use tower::util::ServiceExt;

use crate::{build_router_with_facade, Facade};

pub use dolos_testing::faults::TestFault;
pub struct TestDomainBuilder {
    domain: ToyDomain,
    vectors: SyntheticVectors,
}

impl TestDomainBuilder {
    pub fn new_with_synthetic(cfg: SyntheticBlockConfig) -> Self {
        let genesis = Arc::new(dolos_cardano::include::preview::load());
        let (blocks, vectors, chain_config) = build_synthetic_blocks(cfg);

        let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);
        domain
            .import_blocks(blocks.clone())
            .expect("failed to import synthetic blocks");

        Self { domain, vectors }
    }

    pub fn finish(self) -> (ToyDomain, SyntheticVectors) {
        (self.domain, self.vectors)
    }
}

pub struct TestApp {
    router: Router,
    _domain: dolos_testing::faults::FaultyToyDomain,
    vectors: SyntheticVectors,
}

impl TestApp {
    pub fn new() -> Self {
        Self::new_with_fault(None)
    }

    pub fn new_with_fault(fault: Option<TestFault>) -> Self {
        let (domain, vectors) =
            TestDomainBuilder::new_with_synthetic(SyntheticBlockConfig::default()).finish();

        let domain = match fault {
            Some(fault) => dolos_testing::faults::FaultyToyDomain::new(domain, fault),
            None => dolos_testing::faults::FaultyToyDomain::new(domain, TestFault::None),
        };

        let cfg = MinibfConfig {
            listen_address: "[::]:0".parse().expect("invalid listen address"),
            permissive_cors: None,
            token_registry_url: None,
            url: None,
        };

        let facade = Facade {
            inner: domain.clone(),
            config: cfg,
            cache: crate::cache::CacheService::default(),
        };

        let router = build_router_with_facade(facade);

        Self {
            router,
            _domain: domain,
            vectors,
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

    pub async fn post_bytes(
        &self,
        path: &str,
        content_type: &str,
        body: Vec<u8>,
    ) -> (StatusCode, Vec<u8>) {
        let req = Request::builder()
            .method(Method::POST)
            .uri(path)
            .header("content-type", content_type)
            .body(Body::from(body))
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

    pub fn vectors(&self) -> &SyntheticVectors {
        &self.vectors
    }
}
