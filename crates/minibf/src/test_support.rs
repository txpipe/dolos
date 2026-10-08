use std::sync::Arc;

use axum::{
    body::Body,
    http::{HeaderMap, Method, Request, StatusCode},
    Router,
};
use dolos_core::{
    config::{CardanoConfig, MinibfConfig},
    import::ImportExt as _,
    ArchiveIndexDelta, ArchiveStore as _, ArchiveWriter as _, ChainPoint, Domain, RawBlock,
    StateStore,
};
use dolos_testing::{
    synthetic::{
        build_synthetic_blocks, seed_account_stake_logs, seed_epoch_logs, seed_reward_logs,
        SyntheticBlockConfig, SyntheticVectors,
    },
    toy_domain::ToyDomain,
};
use http_body_util::BodyExt;
use pallas::ledger::traverse::MultiEraBlock;
use tower::util::ServiceExt;

use crate::{build_router_with_facade, Facade};

pub use dolos_testing::faults::TestFault;

pub const REG_POOL_HEX: &str = "5fe086be9d20749a9e59d6cae6ff1df9dae2e0ee95798bd83a143efe";
pub const REG_POOL_ID: &str = "pool1tlsgd05ayp6f48je6m9wdlcal8dw9c8wj4uchkp6zsl0uuvslt4";
pub const MISSING_POOL_ID: &str = "pool1qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qursw2w89e2";

const CHARSET: &[u8; 32] = b"qpzry9x8gf2tvdw0s3jn54khce6mua7l";
const KELVIN: char = '\u{212A}';

fn polymod(values: &[u8]) -> u32 {
    const GENERATOR: [u32; 5] = [
        0x3b6a_57b2,
        0x2650_8e6d,
        0x1ea1_19fa,
        0x3d42_33dd,
        0x2a14_62b3,
    ];
    let mut checksum = 1u32;
    for value in values {
        let top = checksum >> 25;
        checksum = ((checksum & 0x01ff_ffff) << 5) ^ u32::from(*value);
        for (bit, generator) in GENERATOR.iter().enumerate() {
            if (top >> bit) & 1 == 1 {
                checksum ^= generator;
            }
        }
    }
    checksum
}

pub fn bech32_values_from_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut values = Vec::new();
    let (mut acc, mut bits) = (0u32, 0u32);
    for byte in bytes {
        acc = (acc << 8) | u32::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            values.push(((acc >> bits) & 31) as u8);
        }
        acc &= (1 << bits) - 1;
    }
    if bits > 0 {
        values.push(((acc << (5 - bits)) & 31) as u8);
    }
    values
}

pub fn bech32_encode_values(hrp: &str, values: &[u8], constant: u32) -> String {
    let mut input: Vec<u8> = hrp.bytes().map(|c| c >> 5).collect();
    input.push(0);
    input.extend(hrp.bytes().map(|c| c & 31));
    input.extend_from_slice(values);
    input.extend([0; 6]);
    let residue = polymod(&input) ^ constant;
    let checksum = (0..6u32).map(|i| ((residue >> (5 * (5 - i))) & 31) as u8);
    let data: String = values
        .iter()
        .copied()
        .chain(checksum)
        .map(|v| CHARSET[usize::from(v)] as char)
        .collect();
    format!("{hrp}1{data}")
}

pub struct PoolIdCase {
    pub label: &'static str,
    pub id: String,
    pub unbounded_status: u16,
    pub bounded_status: u16,
}

impl PoolIdCase {
    pub fn path_id(&self) -> String {
        self.id.replace(KELVIN, "%E2%84%AA")
    }
}

/// Statuses for an app that registers `REG_POOL_ID`.
pub fn pool_id_cases() -> Vec<PoolIdCase> {
    let reg = hex::decode(REG_POOL_HEX).expect("Cannot decode the pool hex.");
    let hex = REG_POOL_HEX;
    let with_zeros = |n: usize| {
        let mut bytes = reg.clone();
        bytes.resize(reg.len() + n, 0);
        bytes
    };
    let encode = |bytes: &[u8]| bech32_encode_values("pool", &bech32_values_from_bytes(bytes), 1);
    let values = bech32_values_from_bytes(&reg);
    let mut pad_bit = values.clone();
    if let Some(last) = pad_bit.last_mut() {
        *last |= 1;
    }
    let mut pad_word = values.clone();
    pad_word.push(0);
    let mut values_1001 = values.clone();
    values_1001.resize(1001 - 11, 0);
    let mixed: String = hex
        .chars()
        .enumerate()
        .map(|(i, c)| {
            if i % 2 == 1 {
                c.to_ascii_uppercase()
            } else {
                c
            }
        })
        .collect();
    let upper = REG_POOL_ID.to_uppercase();
    let case = |label, id: String, unbounded_status, bounded_status| PoolIdCase {
        label,
        id,
        unbounded_status,
        bounded_status,
    };

    vec![
        case("b28", REG_POOL_ID.to_string(), 200, 200),
        case("b29", encode(&with_zeros(1)), 404, 404),
        case("b27", encode(&reg[..27]), 404, 404),
        case("bmissing", MISSING_POOL_ID.to_string(), 404, 404),
        case("h56", hex.to_string(), 200, 200),
        case("h56upper", hex.to_uppercase(), 200, 200),
        case("h56mixed", mixed, 200, 200),
        case("h57", format!("{hex}0"), 400, 200),
        case("h55", hex[..55].to_string(), 400, 404),
        case("h58", format!("{hex}00"), 404, 404),
        case("h54", hex[..54].to_string(), 404, 404),
        case("h1", "a".to_string(), 400, 404),
        case("h98", format!("{hex}{}", "00".repeat(21)), 404, 404),
        case("h99", format!("{hex}{}0", "00".repeat(21)), 400, 404),
        case("h100", format!("{hex}{}", "00".repeat(22)), 404, 400),
        case("hex2000", format!("{hex}{}", "00".repeat(972)), 404, 400),
        case("hex2001", format!("{hex}{}0", "00".repeat(972)), 400, 400),
        case("hex9000", format!("{hex}{}", "00".repeat(4472)), 404, 400),
        case("invalid", "not-a-pool".to_string(), 400, 400),
        case("bupper", upper.clone(), 404, 404),
        case(
            "bech32m",
            bech32_encode_values("pool", &values, 0x2bc8_30a3),
            404,
            400,
        ),
        case(
            "hrp_pool1x",
            bech32_encode_values("pool1x", &values, 1),
            400,
            400,
        ),
        case(
            "pad_bit",
            bech32_encode_values("pool", &pad_bit, 1),
            404,
            404,
        ),
        case(
            "pad_word",
            bech32_encode_values("pool", &pad_word, 1),
            404,
            404,
        ),
        case("b1000", encode(&with_zeros(590)), 404, 404),
        case(
            "b1001",
            bech32_encode_values("pool", &values_1001, 1),
            404,
            400,
        ),
        case("b1002", encode(&with_zeros(591)), 404, 400),
        case("b1023", encode(&with_zeros(604)), 404, 400),
        case("b1024", encode(&with_zeros(605)), 404, 400),
        case("b5000", encode(&with_zeros(3094)), 404, 400),
        case("kelvin_upper", upper.replace('K', "\u{212A}"), 400, 404),
        case(
            "kelvin_lower",
            REG_POOL_ID.replace('k', "\u{212A}"),
            400,
            400,
        ),
    ]
}

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
            .import_blocks(blocks.clone())
            .expect("failed to import synthetic blocks");
        let summary = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state())
            .expect("era summary");
        let tip_slot = domain
            .state()
            .read_cursor()
            .expect("cursor read failed")
            .expect("missing tip")
            .slot();
        let (epoch, _) = summary.slot_epoch(tip_slot);
        if epoch >= 1 {
            let epochs = [epoch - 1, epoch];
            seed_epoch_logs(&domain, &epochs).expect("failed to seed epoch logs");
        }
        if epoch >= 2 {
            let reward_epochs = [epoch - 2, epoch - 1];
            seed_reward_logs(
                &domain,
                &vectors.stake_address,
                &vectors.pool_id,
                &reward_epochs,
            )
            .expect("failed to seed reward logs");
        }
        if epoch >= 1 {
            seed_account_stake_logs(
                &domain,
                &vectors.stake_address,
                &vectors.pool_id,
                &[epoch - 1],
            )
            .expect("failed to seed account stake logs");
        }

        Self { domain, vectors }
    }

    pub fn finish(self) -> (ToyDomain, SyntheticVectors) {
        (self.domain, self.vectors)
    }
}

pub struct TestApp {
    router: Router,
    _domain: dolos_testing::faults::FaultyToyDomain,
    vectors: Option<SyntheticVectors>,
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

    pub fn new_with_cfg(cfg: SyntheticBlockConfig) -> Self {
        Self::new_with_cfg_and_fault(cfg, None)
    }

    pub fn new_with_cfg_and_fault(cfg: SyntheticBlockConfig, fault: Option<TestFault>) -> Self {
        let (domain, vectors) = TestDomainBuilder::new_with_synthetic(cfg).finish();
        Self::from_domain(domain, Some(vectors), fault, None)
    }

    pub fn new_with_cfg_and_setup(
        cfg: SyntheticBlockConfig,
        setup: impl FnOnce(&ToyDomain, &SyntheticVectors),
    ) -> Self {
        let (domain, vectors) = TestDomainBuilder::new_with_synthetic(cfg).finish();
        setup(&domain, &vectors);
        Self::from_domain(domain, Some(vectors), None, None)
    }

    /// App whose minibf config caps scans at `max_scan_items`, so scan budgets
    /// can be exercised without building a chain of thousands of blocks.
    pub fn new_with_scan_limit(cfg: SyntheticBlockConfig, max_scan_items: u64) -> Self {
        let (domain, vectors) = TestDomainBuilder::new_with_synthetic(cfg).finish();
        Self::from_domain(domain, Some(vectors), None, Some(max_scan_items))
    }

    /// App over a fresh domain for `genesis` whose archive holds `blocks`,
    /// written and indexed the way the roll pipeline files them, a Byron
    /// epoch-boundary block under its chain difficulty included. No ledger
    /// logic runs over them, so this is for routes that read the archive only.
    pub fn new_with_archived_blocks(genesis: dolos_core::Genesis, blocks: &[RawBlock]) -> Self {
        let domain = ToyDomain::new_with_genesis_and_config(
            Arc::new(genesis),
            CardanoConfig::default(),
            None,
            None,
        );

        let writer = domain.archive().start_writer().expect("archive writer");
        let mut deltas = Vec::new();

        for raw in blocks {
            let block = MultiEraBlock::decode(raw).expect("archived block decodes");
            let point = ChainPoint::Specific(block.slot(), block.hash());

            writer.apply(&point, raw).expect("archive block");

            deltas.push(ArchiveIndexDelta {
                slot: block.slot(),
                block_hash: block.hash().to_vec(),
                block_number: Some(block.number()),
                tx_hashes: block.txs().iter().map(|tx| tx.hash().to_vec()).collect(),
                tags: Vec::new(),
            });
        }

        writer.apply_index(&deltas).expect("index archived blocks");
        writer.commit().expect("commit archived blocks");

        Self::from_domain(domain, None, None, None)
    }

    fn from_domain(
        domain: ToyDomain,
        vectors: Option<SyntheticVectors>,
        fault: Option<TestFault>,
        max_scan_items: Option<u64>,
    ) -> Self {
        let domain = match fault {
            Some(fault) => dolos_testing::faults::FaultyToyDomain::new(domain, fault),
            None => dolos_testing::faults::FaultyToyDomain::new(domain, TestFault::None),
        };

        let cfg = MinibfConfig::new("[::]:0".parse().expect("invalid listen address"));
        let cfg = match max_scan_items {
            Some(max) => cfg.with_max_scan_items(max),
            None => cfg,
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
        let (status, _, bytes) = self.get_with_headers(path).await;
        (status, bytes)
    }

    pub async fn get_with_headers(&self, path: &str) -> (StatusCode, HeaderMap, Vec<u8>) {
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
        let headers = res.headers().clone();
        let bytes = res
            .into_body()
            .collect()
            .await
            .expect("failed to read response body")
            .to_bytes();
        (status, headers, bytes.to_vec())
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
        self.vectors
            .as_ref()
            .expect("app built from synthetic blocks")
    }

    /// Epoch at the domain's tip. Only usable on fault-free apps — fault
    /// wrappers make the underlying state reads fail.
    pub fn tip_epoch(&self) -> u64 {
        let summary =
            dolos_cardano::eras::load_era_summary::<dolos_testing::faults::FaultyToyDomain>(
                self._domain.state(),
            )
            .expect("era summary");

        let tip = self
            ._domain
            .state()
            .read_cursor()
            .expect("cursor read failed")
            .expect("missing tip")
            .slot();

        summary.slot_epoch(tip).0
    }

    /// First slot of the given epoch. Only usable on fault-free apps — fault
    /// wrappers make the underlying state reads fail.
    pub fn epoch_start(&self, epoch: u64) -> u64 {
        let summary =
            dolos_cardano::eras::load_era_summary::<dolos_testing::faults::FaultyToyDomain>(
                self._domain.state(),
            )
            .expect("era summary");

        summary.epoch_start(epoch)
    }
}
