use std::sync::Arc;

use axum::{
    body::Body,
    http::{header, HeaderMap, Method, Request, StatusCode},
    Router,
};
use dolos_core::{
    config::{CardanoConfig, MinikupoConfig},
    import::ImportExt as _,
    Domain,
};
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
    pub fn new_with_synthetic(mut cfg: SyntheticBlockConfig) -> Self {
        let genesis = Arc::new(dolos_cardano::include::preview::load());
        let min_slot = {
            let temp = ToyDomain::new_with_genesis_and_config(
                genesis.clone(),
                CardanoConfig::default(),
                None,
                None,
            );
            let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(temp.state())
                .expect("era summary");
            summary.epoch_start(2)
        };

        if cfg.slot < min_slot {
            cfg.slot = min_slot;
        }

        let (blocks, vectors, chain_config) = build_synthetic_blocks(cfg);
        let domain = ToyDomain::new_with_genesis_and_config(genesis, chain_config, None, None);
        domain
            .import_blocks(blocks)
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
        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };

        Self::new_with_cfg_and_fault(cfg, fault)
    }

    #[allow(dead_code)]
    pub fn new_with_cfg(cfg: SyntheticBlockConfig) -> Self {
        Self::new_with_cfg_and_fault(cfg, None)
    }

    pub fn new_empty() -> Self {
        let genesis = Arc::new(dolos_cardano::include::preview::load());
        let domain =
            ToyDomain::new_with_genesis_and_config(genesis, CardanoConfig::default(), None, None);

        let domain = dolos_testing::faults::FaultyToyDomain::new(domain, TestFault::None);
        let router = build_router_with_facade(Facade {
            inner: domain.clone(),
            config: test_config(),
        });

        Self {
            router,
            _domain: domain,
            vectors: SyntheticVectors {
                address: String::new(),
                stake_address: String::new(),
                asset_unit: String::new(),
                policy_id: String::new(),
                asset_name_hex: String::new(),
                metadata_label: String::new(),
                block_hash: String::new(),
                tx_hash: String::new(),
                datum_hash: String::new(),
                datum_cbor_hex: String::new(),
                script_hash: String::new(),
                script_cbor_hex: String::new(),
                blocks: vec![],
                account_addresses: vec![],
                account_address_blocks: vec![],
                account_address_bounds: vec![],
                pool_id: String::new(),
                drep_id: String::new(),
                tx_cbor: vec![],
                account_withdrawals: vec![],
            },
        }
    }

    pub fn new_with_cfg_and_fault(cfg: SyntheticBlockConfig, fault: Option<TestFault>) -> Self {
        let (domain, vectors) = TestDomainBuilder::new_with_synthetic(cfg).finish();

        let domain = match fault {
            Some(fault) => dolos_testing::faults::FaultyToyDomain::new(domain, fault),
            None => dolos_testing::faults::FaultyToyDomain::new(domain, TestFault::None),
        };

        let router = build_router_with_facade(Facade {
            inner: domain.clone(),
            config: test_config(),
        });

        Self {
            router,
            _domain: domain,
            vectors,
        }
    }

    pub async fn get_response(&self, path: &str) -> (StatusCode, HeaderMap, Vec<u8>) {
        self.get_response_with_accept(path, None).await
    }

    pub async fn get_response_with_accept(
        &self,
        path: &str,
        accept: Option<&str>,
    ) -> (StatusCode, HeaderMap, Vec<u8>) {
        let mut req = Request::builder().method(Method::GET).uri(path);

        if let Some(accept) = accept {
            req = req.header(header::ACCEPT, accept);
        }

        let req = req.body(Body::empty()).expect("failed to build request");
        let res = self
            .router
            .clone()
            .oneshot(req)
            .await
            .expect("request failed");

        let status = res.status();
        let headers = res.headers().clone();
        let bytes = res
            .into_body()
            .collect()
            .await
            .expect("failed to read response body")
            .to_bytes();

        (status, headers, bytes.to_vec())
    }

    pub async fn get_bytes(&self, path: &str) -> (StatusCode, Vec<u8>) {
        let (status, _, bytes) = self.get_response(path).await;
        (status, bytes)
    }

    pub fn vectors(&self) -> &SyntheticVectors {
        &self.vectors
    }
}

fn test_config() -> MinikupoConfig {
    MinikupoConfig {
        listen_address: "[::]:0".parse().expect("invalid listen address"),
        permissive_cors: None,
    }
}
