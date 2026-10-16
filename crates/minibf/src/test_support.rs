use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    body::Body,
    http::{HeaderMap, Method, Request, StatusCode},
    Router,
};
use dolos_core::{
    config::{CardanoConfig, MinibfConfig},
    import::ImportExt as _,
    Domain, StateStore,
};
use dolos_testing::{
    synthetic::{
        build_synthetic_blocks, seed_account_stake_logs, seed_epoch_logs, seed_reward_logs,
        SyntheticBlockConfig, SyntheticVectors,
    },
    toy_domain::ToyDomain,
};
use http_body_util::BodyExt;
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

/// Policies of [`first_mint_chain`]. `SIBLING` shares `ALPHA`'s first fourteen
/// bytes, the prefix staged first mints are grouped by.
pub mod first_mint_policies {
    pub const ALPHA: [u8; 28] = [0x11; 28];
    pub const BETA: [u8; 28] = [0x22; 28];
    pub const GAMMA: [u8; 28] = [0x33; 28];
    pub const SIBLING: [u8; 28] = [
        0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x99,
        0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99, 0x99,
    ];
}

/// Twelve blocks over preview epochs 2 to 7, two per epoch, whose mints cover
/// what first-mint order has to get right: several mints in one tx, several
/// policies in one block and in one tx, re-mints within and across epochs, a
/// full burn minted again, a repeat across the txs of one block, a policy
/// sharing another's group-key prefix, and an epoch with more first mints than
/// a sealed chunk holds. The tip's epoch seals epochs 2 to 5 and leaves 6 and 7
/// staged.
pub fn first_mint_chain() -> SyntheticBlockConfig {
    use first_mint_policies::*;

    let mint = |policy: [u8; 28], name: &str, amount: i64| (policy, name.to_string(), amount);

    let many: Vec<_> = (0..300)
        .map(|i| mint(BETA, &format!("N{i:03}"), 1))
        .collect();

    let mints_by_block = vec![
        vec![
            vec![mint(ALPHA, "A", 1), mint(ALPHA, "B", 1)],
            vec![mint(BETA, "X", 5)],
        ],
        vec![vec![mint(ALPHA, "A", 1)], vec![mint(SIBLING, "A", 1)]],
        vec![
            vec![mint(ALPHA, "A", -2)],
            vec![mint(GAMMA, "Z", 1), mint(BETA, "Y", 1)],
        ],
        vec![vec![mint(ALPHA, "C", 1)], many],
        vec![
            vec![mint(ALPHA, "A", 1)],
            vec![mint(ALPHA, "E", 1), mint(ALPHA, "D", 1), mint(BETA, "W", 1)],
        ],
        vec![vec![], vec![mint(GAMMA, "Z", -1)]],
        vec![vec![mint(ALPHA, "F", 1)], vec![mint(ALPHA, "B", 1)]],
        vec![vec![mint(BETA, "V", 1)], vec![]],
        vec![
            vec![mint(ALPHA, "G", 1), mint(ALPHA, "H", 1)],
            vec![mint(ALPHA, "G", 1)],
        ],
        vec![vec![mint(GAMMA, "Y", 1)], vec![mint(SIBLING, "B", 1)]],
        vec![vec![mint(BETA, "U", 1)], vec![mint(ALPHA, "I", 1)]],
        vec![vec![mint(ALPHA, "A", 1)], vec![mint(ALPHA, "J", 1)]],
    ];

    SyntheticBlockConfig {
        block_count: mints_by_block.len(),
        txs_per_block: 2,
        slot_step: 43_200,
        mints_by_block,
        plain_txs: true,
        // each tx pays a fee of 7 out of its seed, so the pots balance across
        // the boundaries
        seed_amount: dolos_testing::MIN_UTXO_AMOUNT + 7,
        ..Default::default()
    }
}

/// Accounts of [`stake_address_chain`].
pub mod stake_address_accounts {
    use pallas::ledger::{
        addresses::{
            Address, Network, Pointer, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart,
        },
        primitives::StakeCredential,
    };

    use dolos_testing::synthetic::SyntheticBlockConfig;

    /// The synthetic chain's own account: every tx's first output pays it.
    pub fn primary() -> StakeCredential {
        let address = Address::from_bech32(&SyntheticBlockConfig::default().address).unwrap();
        dolos_cardano::pallas_extras::address_as_stake_cred(&address)
            .unwrap()
            .0
    }

    /// Another key account, paid only by extra outputs.
    pub fn other() -> StakeCredential {
        StakeCredential::AddrKeyhash([0x5a; 28].into())
    }

    /// A script account sharing `other`'s hash.
    pub fn script() -> StakeCredential {
        StakeCredential::ScriptHash([0x5a; 28].into())
    }

    /// The base address paying `payment` under `account`.
    pub fn base(account: &StakeCredential, payment: u8) -> Vec<u8> {
        let delegation = match account {
            StakeCredential::AddrKeyhash(hash) => ShelleyDelegationPart::Key(*hash),
            StakeCredential::ScriptHash(hash) => ShelleyDelegationPart::Script(*hash),
        };

        ShelleyAddress::new(
            Network::Testnet,
            ShelleyPaymentPart::Key([payment; 28].into()),
            delegation,
        )
        .to_vec()
    }

    /// Addresses no account lists: an enterprise address and a pointer one.
    pub fn unlisted(payment: u8) -> [Vec<u8>; 2] {
        let payment = ShelleyPaymentPart::Key([payment; 28].into());

        [
            ShelleyAddress::new(
                Network::Testnet,
                payment.clone(),
                ShelleyDelegationPart::Null,
            )
            .to_vec(),
            ShelleyAddress::new(
                Network::Testnet,
                payment,
                // one the pointer table resolves, which the ledger needs
                ShelleyDelegationPart::Pointer(Pointer::new(53_004_562, 9, 0)),
            )
            .to_vec(),
        ]
    }
}

/// Twelve blocks over preview epochs 2 to 7, two per epoch, of three txs each,
/// whose outputs cover what first-appearance order has to get right. Each tx
/// pays the primary account first, and the builder's own outputs reuse
/// addresses across consecutive blocks and every epoch. Extra outputs add
/// several addresses of one account in one tx, an address repeated within a
/// tx and a block, addresses whose bytes sort against their chain order, one
/// returning epochs after it first appeared, a hundred and fifty fresh ones in
/// one tx for deep pages, and other accounts and unlisted addresses between
/// them. The tip's epoch seals epochs 2 to 5 and leaves 6 and 7 staged.
pub fn stake_address_chain() -> SyntheticBlockConfig {
    use stake_address_accounts::*;

    let (a, b, c) = (primary(), other(), script());

    let none = || vec![vec![], vec![], vec![]];

    let mut extra = vec![none(); 12];

    // several outputs to one account in one tx, against byte order, with a
    // repeat inside the tx and another account in between
    extra[0][1] = vec![
        base(&a, 0x90),
        base(&b, 0x91),
        base(&a, 0x80),
        base(&a, 0x90),
    ];
    // the repeat again in the next tx of the same block, and unlisted ones
    let [enterprise, pointer] = unlisted(0x92);
    extra[0][2] = vec![enterprise, base(&a, 0x90), pointer, base(&c, 0x93)];

    // a hundred and fifty fresh addresses in one tx
    extra[3][0] = (0..150u8).rev().map(|i| base(&a, i)).collect();

    // an epoch-2 address returning in epoch 6, beside a fresh one
    extra[8][1] = vec![base(&a, 0x80), base(&a, 0xa0), base(&b, 0x91)];

    // a staged epoch with the script account and more fresh ones
    extra[10][2] = vec![
        base(&c, 0x93),
        base(&c, 0xa1),
        base(&a, 0xa2),
        base(&a, 0x05),
    ];

    SyntheticBlockConfig {
        block_count: 12,
        txs_per_block: 3,
        slot_step: 43_200,
        plain_txs: true,
        ada_only_by_block: vec![true; 12],
        extra_outputs_by_block: extra,
        // each tx pays a fee of 7 out of its seed, so the pots balance across
        // the boundaries
        seed_amount: dolos_testing::MIN_UTXO_AMOUNT + 7,
        ..Default::default()
    }
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
    facade: Facade<dolos_testing::faults::FaultyToyDomain>,
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

    pub fn new_with_cfg(cfg: SyntheticBlockConfig) -> Self {
        Self::new_with_cfg_and_fault(cfg, None)
    }

    pub fn new_with_cfg_and_fault(cfg: SyntheticBlockConfig, fault: Option<TestFault>) -> Self {
        let (domain, vectors) = TestDomainBuilder::new_with_synthetic(cfg).finish();
        Self::from_domain(domain, vectors, fault, None)
    }

    pub fn new_with_cfg_and_setup(
        cfg: SyntheticBlockConfig,
        setup: impl FnOnce(&ToyDomain, &SyntheticVectors),
    ) -> Self {
        let (domain, vectors) = TestDomainBuilder::new_with_synthetic(cfg).finish();
        setup(&domain, &vectors);
        Self::from_domain(domain, vectors, None, None)
    }

    /// App whose minibf config caps scans at `max_scan_items`, so scan budgets
    /// can be exercised without building a chain of thousands of blocks.
    pub fn new_with_scan_limit(cfg: SyntheticBlockConfig, max_scan_items: u64) -> Self {
        let (domain, vectors) = TestDomainBuilder::new_with_synthetic(cfg).finish();
        Self::from_domain(domain, vectors, None, Some(max_scan_items))
    }

    fn from_domain(
        domain: ToyDomain,
        vectors: SyntheticVectors,
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
            inner: domain,
            config: cfg,
            cache: crate::cache::CacheService::default(),
        };

        let router = build_router_with_facade(facade.clone());

        Self {
            router,
            facade,
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

    pub fn facade(&self) -> Facade<dolos_testing::faults::FaultyToyDomain> {
        self.facade.clone()
    }

    pub fn vectors(&self) -> &SyntheticVectors {
        &self.vectors
    }

    /// Epoch at the domain's tip. Only usable on fault-free apps — fault
    /// wrappers make the underlying state reads fail.
    pub fn tip_epoch(&self) -> u64 {
        let summary =
            dolos_cardano::eras::load_era_summary::<dolos_testing::faults::FaultyToyDomain>(
                self.facade.state(),
            )
            .expect("era summary");

        let tip = self
            .facade
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
                self.facade.state(),
            )
            .expect("era summary");

        summary.epoch_start(epoch)
    }
}

/// The delay of a slow-read fault in a stall test.
pub const SLOW_READ: Duration = Duration::from_secs(1);

/// How long `/health` may take beside a slow request.
pub const HEALTH_BUDGET: Duration = Duration::from_millis(300);

/// Sends `path` and `/health` together and checks that `/health` answers
/// within [`HEALTH_BUDGET`]. Returns the status of `path`.
pub async fn health_beside(app: &TestApp, path: &str) -> StatusCode {
    let started = Instant::now();
    let heavy = app.get_bytes(path);
    let health = async {
        let (status, _) = app.get_bytes("/health").await;
        (status, started.elapsed())
    };

    let ((status, _), (health_status, health_took)) = tokio::join!(heavy, health);

    assert_eq!(health_status, StatusCode::OK);
    assert!(
        health_took < HEALTH_BUDGET,
        "/health took {health_took:?} behind {path}"
    );

    status
}

/// Writes `copies` copies of every account and every pool under new keys, so
/// that a scan of either reads several chunks. The account copies keep the
/// delegations of the originals.
pub fn replicate_accounts_and_pools(domain: &ToyDomain, copies: u8) {
    use dolos_cardano::{
        model::{AccountState, PoolState},
        AccountKey, PoolKey,
    };
    use dolos_core::StateWriter as _;
    use pallas::{crypto::hash::Hash, ledger::primitives::StakeCredential};

    let accounts: Vec<_> = domain
        .state()
        .iter_entities_typed::<AccountState>(None)
        .expect("account scan")
        .collect::<Result<_, _>>()
        .expect("account read");
    let pools: Vec<_> = domain
        .state()
        .iter_entities_typed::<PoolState>(None)
        .expect("pool scan")
        .collect::<Result<_, _>>()
        .expect("pool read");

    let writer = domain.state().start_writer().expect("state writer");

    for copy in 1..=copies {
        for (_, account) in &accounts {
            let mut account = account.clone();
            account.credential = StakeCredential::AddrKeyhash(Hash::from([copy; 28]));
            writer
                .write_entity_typed(&AccountKey::from(&account.credential), &account)
                .expect("account write");
        }

        for (_, pool) in &pools {
            let mut pool = pool.clone();
            pool.operator = Hash::from([copy; 28]);
            writer
                .write_entity_typed(&PoolKey(pool.operator), &pool)
                .expect("pool write");
        }
    }

    writer.commit().expect("state commit");
}

/// An app whose accounts and pools span several chunks of two.
pub fn replicated_app() -> TestApp {
    replicated_app_with_fault(None)
}

pub fn replicated_app_with_fault(fault: Option<TestFault>) -> TestApp {
    let cfg = SyntheticBlockConfig {
        block_count: 5,
        txs_per_block: 3,
        ..Default::default()
    };

    let (domain, vectors) = TestDomainBuilder::new_with_synthetic(cfg).finish();
    replicate_accounts_and_pools(&domain, 4);

    TestApp::from_domain(domain, vectors, fault, None)
}
