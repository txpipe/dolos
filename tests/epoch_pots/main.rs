mod compare;

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use csv_diff::diff_row::DiffByteRecord;
use pallas::ledger::addresses::Network;
use serde::Deserialize;

use compare::{compare_csvs_with_ignore, extract_row_from_csv, has_data, write_fixture};

use dolos_cardano::model::{
    EpochState, EraSummary, FixedNamespace, PParamKind, PParamValue, PParamsSet,
};
use dolos_cardano::pallas_extras;
use dolos_cardano::rupd::StakeSnapshot;
use dolos_cardano::ewrap::AppliedReward;
use dolos_cardano::CardanoWorkUnit;
use dolos_cardano::PoolHash;
use dolos_core::{config::CardanoConfig, Domain, StateStore};
use dolos_testing::harness::cardano::{copy_dir_recursive, Config, LedgerHarness};

// ---------------------------------------------------------------------------
// Minimal xtask config structs (avoid pulling in the full xtask crate)
// ---------------------------------------------------------------------------

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct XtaskConfig {
    instances_root: PathBuf,
    seeds: SeedConfig,
    snapshots: SnapshotConfig,
}

#[derive(Debug, Deserialize)]
struct SeedConfig {
    mainnet: Option<PathBuf>,
    preview: Option<PathBuf>,
    preprod: Option<PathBuf>,
}

impl SeedConfig {
    fn get(&self, network: &str) -> Option<&PathBuf> {
        match network {
            "mainnet" => self.mainnet.as_ref(),
            "preview" => self.preview.as_ref(),
            "preprod" => self.preprod.as_ref(),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct SnapshotConfig {
    mainnet: PathBuf,
    preview: PathBuf,
    preprod: PathBuf,
}

impl SnapshotConfig {
    fn get(&self, network: &str) -> Option<&PathBuf> {
        match network {
            "mainnet" => Some(&self.mainnet),
            "preview" => Some(&self.preview),
            "preprod" => Some(&self.preprod),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Config loading
// ---------------------------------------------------------------------------

fn load_xtask_config() -> Result<XtaskConfig> {
    let repo_root = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .or_else(|_| std::env::current_dir().context("detecting repo root"))?;
    let config_path = repo_root.join("xtask.toml");
    let raw = std::fs::read_to_string(&config_path)
        .with_context(|| format!("reading {}", config_path.display()))?;
    toml::from_str(&raw).context("parsing xtask.toml")
}


// ---------------------------------------------------------------------------
// Tracing
// ---------------------------------------------------------------------------

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("dolos_cardano=info,dolos_cardano::rupd::loading=debug")
        .with_writer(std::io::stderr)
        .try_init();
}

// ---------------------------------------------------------------------------
// RUPD dump helpers
// ---------------------------------------------------------------------------

fn bech32_pool(pool: &PoolHash) -> std::io::Result<String> {
    let hrp = bech32::Hrp::parse_unchecked("pool");
    bech32::encode::<bech32::Bech32>(hrp, pool.as_slice()).map_err(std::io::Error::other)
}

fn dump_delegation_csv(
    snapshot: &StakeSnapshot,
    out_dir: &Path,
    epoch: u64,
) -> std::io::Result<()> {
    let path = out_dir.join(format!("delegation-{}.csv", epoch));
    let file = std::fs::File::create(&path)?;
    let mut writer = std::io::BufWriter::new(file);

    writeln!(writer, "pool_bech32,pool_hash,total_lovelace")?;

    let mut rows = Vec::with_capacity(snapshot.pool_stake.len());
    for (pool, stake) in snapshot.pool_stake.iter() {
        let pool_bech32 = bech32_pool(pool)?;
        let pool_hash = hex::encode(pool.as_slice());
        rows.push((pool_bech32, pool_hash, *stake));
    }

    rows.sort_by(|a, b| a.0.cmp(&b.0));

    for (pool_bech32, pool_hash, stake) in rows {
        writeln!(writer, "{},{},{}", pool_bech32, pool_hash, stake)?;
    }

    Ok(())
}

fn dump_stake_csv(
    snapshot: &StakeSnapshot,
    network: Network,
    out_dir: &Path,
    epoch: u64,
) -> std::io::Result<()> {
    let path = out_dir.join(format!("stake-{}.csv", epoch));
    let file = std::fs::File::create(&path)?;
    let mut writer = std::io::BufWriter::new(file);

    writeln!(writer, "stake,pool,lovelace")?;

    let mut rows = Vec::new();
    for (pool, credential, stake) in snapshot.iter_accounts() {
        let address = pallas_extras::stake_credential_to_address(network, credential);
        let stake_address = address.to_bech32().map_err(std::io::Error::other)?;
        let pool_bech32 = bech32_pool(pool)?;
        rows.push((stake_address, pool_bech32, *stake));
    }

    rows.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));

    for (stake_address, pool_bech32, stake) in rows {
        writeln!(writer, "{},{},{}", stake_address, pool_bech32, stake)?;
    }

    Ok(())
}

/// Dump rewards that were actually applied (spendable) at EWRAP time.
/// This filters out rewards for accounts that deregistered between RUPD and EWRAP.
fn dump_applied_rewards_csv(
    applied_rewards: &[AppliedReward],
    network: Network,
    out_dir: &Path,
    epoch: u64,
) -> std::io::Result<()> {
    let path = out_dir.join(format!("rewards-{}.csv", epoch));
    let file = std::fs::File::create(&path)?;
    let mut writer = std::io::BufWriter::new(file);

    writeln!(writer, "stake,pool,amount,type,earned_epoch")?;

    let mut rows = Vec::new();
    for reward in applied_rewards {
        if reward.amount == 0 {
            continue;
        }

        let address = pallas_extras::stake_credential_to_address(network, &reward.credential);
        let stake_address = address.to_bech32().map_err(std::io::Error::other)?;
        let pool_bech32 = bech32_pool(&reward.pool)?;
        let rtype = if reward.as_leader { "leader" } else { "member" };

        rows.push((stake_address, pool_bech32, reward.amount, rtype, epoch));
    }

    rows.sort_by(|a, b| (&a.0, &a.1, &a.3).cmp(&(&b.0, &b.1, &b.3)));

    for (stake, pool, amount, rtype, earned) in rows {
        writeln!(writer, "{},{},{},{},{}", stake, pool, amount, rtype, earned)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// CSV dump helpers
// ---------------------------------------------------------------------------

fn rational_to_f64(r: &pallas::ledger::primitives::RationalNumber) -> f64 {
    if r.denominator == 0 {
        0.0
    } else {
        r.numerator as f64 / r.denominator as f64
    }
}

fn get_rational(pparams: &PParamsSet, kind: PParamKind) -> f64 {
    match pparams.get(kind) {
        Some(
            PParamValue::ExpansionRate(r)
            | PParamValue::TreasuryGrowthRate(r)
            | PParamValue::DecentralizationConstant(r)
            | PParamValue::PoolPledgeInfluence(r),
        ) => rational_to_f64(r),
        _ => 0.0,
    }
}

/// Write a single-row epochs CSV from an EpochState.
fn write_epoch_row(epoch: &EpochState, path: &Path) -> Result<()> {
    let mut wtr = csv::Writer::from_path(path)
        .with_context(|| format!("creating {}", path.display()))?;

    wtr.write_record([
        "epoch_no",
        "protocol_major",
        "treasury",
        "reserves",
        "rewards",
        "utxo",
        "deposits_stake",
        "fees",
        "nonce",
        "block_count",
    ])?;

    let nonce = epoch
        .nonces
        .as_ref()
        .map(|x| hex::encode(x.active))
        .unwrap_or_default();

    let rolling = epoch.rolling.live();
    let pparams = epoch.pparams.live();
    let protocol_major = pparams
        .as_ref()
        .and_then(|x| x.protocol_major())
        .unwrap_or_default();

    wtr.write_record(&[
        epoch.number.to_string(),
        protocol_major.to_string(),
        epoch.initial_pots.treasury.to_string(),
        epoch.initial_pots.reserves.to_string(),
        epoch.initial_pots.rewards.to_string(),
        epoch.initial_pots.utxos.to_string(),
        epoch.initial_pots.stake_deposits().to_string(),
        epoch.initial_pots.fees.to_string(),
        nonce,
        rolling
            .map(|x| x.blocks_minted)
            .unwrap_or_default()
            .to_string(),
    ])?;

    wtr.flush()?;
    Ok(())
}

/// Write a single-row pparams CSV from an EpochState.
fn write_pparam_row(epoch: &EpochState, path: &Path) -> Result<()> {
    let mut wtr = csv::Writer::from_path(path)
        .with_context(|| format!("creating {}", path.display()))?;

    wtr.write_record([
        "epoch_no",
        "protocol_major",
        "protocol_minor",
        "min_fee_a",
        "min_fee_b",
        "key_deposit",
        "pool_deposit",
        "expansion_rate",
        "treasury_growth_rate",
        "decentralisation",
        "desired_pool_number",
        "min_pool_cost",
        "influence",
    ])?;

    let pparams = epoch.pparams.live();
    if let Some(pparams) = pparams.as_ref() {
        let (major, minor) = pparams.protocol_version().unwrap_or((0, 0));

        wtr.write_record(&[
            epoch.number.to_string(),
            major.to_string(),
            minor.to_string(),
            pparams.min_fee_a().unwrap_or(0).to_string(),
            pparams.min_fee_b().unwrap_or(0).to_string(),
            pparams.key_deposit().unwrap_or(0).to_string(),
            pparams.pool_deposit().unwrap_or(0).to_string(),
            get_rational(pparams, PParamKind::ExpansionRate).to_string(),
            get_rational(pparams, PParamKind::TreasuryGrowthRate).to_string(),
            get_rational(pparams, PParamKind::DecentralizationConstant).to_string(),
            pparams
                .desired_number_of_stake_pools()
                .unwrap_or(0)
                .to_string(),
            pparams.min_pool_cost().unwrap_or(0).to_string(),
            get_rational(pparams, PParamKind::PoolPledgeInfluence).to_string(),
        ])?;
    }

    wtr.flush()?;
    Ok(())
}

fn dump_eras(state: &impl StateStore, path: &Path) -> Result<()> {
    let mut wtr = csv::Writer::from_path(path)
        .with_context(|| format!("creating {}", path.display()))?;

    wtr.write_record(["protocol", "start_epoch", "epoch_length", "slot_length"])?;

    let iter = state
        .iter_entities_typed::<EraSummary>(EraSummary::NS, None)
        .context("iterating eras")?;

    for result in iter {
        let (_key, era) = result.context("decoding era")?;
        wtr.write_record(&[
            era.protocol.to_string(),
            era.start.epoch.to_string(),
            era.epoch_length.to_string(),
            era.slot_length.to_string(),
        ])?;
    }

    wtr.flush()?;
    Ok(())
}


// ---------------------------------------------------------------------------
// Fixtures (compile-time embedded)
// ---------------------------------------------------------------------------

mod fixtures {
    pub mod mainnet_250 {
        pub const EPOCHS: &str = include_str!("fixtures/mainnet-250/epochs.csv");
        pub const PPARAMS: &str = include_str!("fixtures/mainnet-250/pparams.csv");
        pub const ERAS: &str = include_str!("fixtures/mainnet-250/eras.csv");
        pub const DELEGATION: &str = include_str!("fixtures/mainnet-250/delegation-248.csv");
        pub const STAKE: &str = include_str!("fixtures/mainnet-250/stake-248.csv");
        pub const REWARDS: &str = include_str!("fixtures/mainnet-250/rewards.csv");
    }
    pub mod mainnet_300 {
        pub const EPOCHS: &str = include_str!("fixtures/mainnet-300/epochs.csv");
        pub const PPARAMS: &str = include_str!("fixtures/mainnet-300/pparams.csv");
        pub const ERAS: &str = include_str!("fixtures/mainnet-300/eras.csv");
        pub const DELEGATION: &str = include_str!("fixtures/mainnet-300/delegation-298.csv");
        pub const STAKE: &str = include_str!("fixtures/mainnet-300/stake-298.csv");
        pub const REWARDS: &str = include_str!("fixtures/mainnet-300/rewards.csv");
    }
    pub mod preview_550 {
        pub const EPOCHS: &str = include_str!("fixtures/preview-550/epochs.csv");
        pub const PPARAMS: &str = include_str!("fixtures/preview-550/pparams.csv");
        pub const ERAS: &str = include_str!("fixtures/preview-550/eras.csv");
        pub const DELEGATION: &str = include_str!("fixtures/preview-550/delegation-548.csv");
        pub const STAKE: &str = include_str!("fixtures/preview-550/stake-548.csv");
        pub const REWARDS: &str = include_str!("fixtures/preview-550/rewards.csv");
    }
    pub mod preview_649 {
        pub const EPOCHS: &str = include_str!("fixtures/preview-649/epochs.csv");
        pub const PPARAMS: &str = include_str!("fixtures/preview-649/pparams.csv");
        pub const ERAS: &str = include_str!("fixtures/preview-649/eras.csv");
        pub const DELEGATION: &str = include_str!("fixtures/preview-649/delegation-647.csv");
        pub const STAKE: &str = include_str!("fixtures/preview-649/stake-647.csv");
        pub const REWARDS: &str = include_str!("fixtures/preview-649/rewards.csv");
    }
    pub mod preview_700 {
        pub const EPOCHS: &str = include_str!("fixtures/preview-700/epochs.csv");
        pub const PPARAMS: &str = include_str!("fixtures/preview-700/pparams.csv");
        pub const ERAS: &str = include_str!("fixtures/preview-700/eras.csv");
        pub const DELEGATION: &str = include_str!("fixtures/preview-700/delegation-698.csv");
        pub const STAKE: &str = include_str!("fixtures/preview-700/stake-698.csv");
        pub const REWARDS: &str = include_str!("fixtures/preview-700/rewards.csv");
    }
}

// ---------------------------------------------------------------------------
// Core test runner
// ---------------------------------------------------------------------------

fn run_epoch_pots_test(
    network: &str,
    subject_epoch: u64,
    seed_dir: &Path,
    performance_epochs: &str,
    gt_pparams: &str,
    gt_eras: &str,
    gt_delegation: &str,
    gt_stake: &str,
    gt_rewards: &str,
) -> Result<()> {
    let config = load_xtask_config()?;

    let snapshot_dir = config
        .snapshots
        .get(network)
        .with_context(|| format!("no snapshot configured for network {network}"))?;

    // stop_epoch = subject + 1 so the subject epoch completes fully
    let stop_epoch = subject_epoch + 1;

    let genesis = match network {
        "mainnet" => dolos_cardano::include::mainnet::load(),
        "preprod" => dolos_cardano::include::preprod::load(),
        "preview" => dolos_cardano::include::preview::load(),
        other => anyhow::bail!("unsupported network: {other}"),
    };

    let keep_dir = std::env::var("EPOCH_POTS_KEEP_DIR")
        .map(|v| !v.is_empty() && v != "0" && v.to_lowercase() != "false")
        .unwrap_or(false);

    let tmp = tempfile::Builder::new()
        .prefix("epoch_pots_")
        .tempdir()
        .context("creating temp dir")?;
    let tmp_path = tmp.path().to_path_buf();

    if keep_dir {
        let _ = tmp.keep();
    }

    let work_state_dir = tmp_path.join("state");
    copy_dir_recursive(&seed_dir.join("state"), &work_state_dir)
        .context("copying seed state")?;

    eprintln!(
        "running epoch_pots test: network={}, subject_epoch={}, stop_epoch={}, work_dir={}{}",
        network,
        subject_epoch,
        stop_epoch,
        tmp_path.display(),
        if keep_dir { " (KEEP)" } else { "" }
    );

    let harness = LedgerHarness::new(Config {
        state_dir: work_state_dir,
        immutable_dir: snapshot_dir.join("immutable"),
        genesis,
        chain: CardanoConfig {
            stop_epoch: Some(stop_epoch),
            ..Default::default()
        },
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    let dumps_dir = tmp_path.join("dumps");
    std::fs::create_dir_all(&dumps_dir)?;

    let cardano_network = dolos_cardano::network_from_genesis(&harness.domain().genesis());

    // Capture the completed subject epoch via the estart callback.
    // When estart fires for epoch N+1, ended_state() holds the completed epoch N.
    let mut captured_epoch: Option<EpochState> = None;

    harness
        .run(100, |_domain, work| {
            match work {
                CardanoWorkUnit::Estart(estart) => {
                    if let Some(ended) = estart.ended_state() {
                        if ended.number == subject_epoch {
                            captured_epoch = Some(ended.clone());
                        }
                    }
                }
                CardanoWorkUnit::Rupd(rupd) => {
                    // Dump delegation and stake from RUPD (snapshot-based data)
                    if let Some(w) = rupd.work() {
                        if let Some((_, performance_epoch)) = w.relevant_epochs() {
                            if let Err(e) =
                                dump_delegation_csv(&w.snapshot, &dumps_dir, performance_epoch)
                            {
                                eprintln!("failed to dump delegation csv: {e}");
                            }
                            if let Err(e) =
                                dump_stake_csv(&w.snapshot, cardano_network, &dumps_dir, performance_epoch)
                            {
                                eprintln!("failed to dump stake csv: {e}");
                            }
                        }
                    }
                }
                CardanoWorkUnit::Ewrap(ewrap) => {
                    // Dump rewards from EWRAP (only actually applied/spendable rewards)
                    if let Some(boundary) = ewrap.boundary() {
                        // performance_epoch = ending_epoch - 1
                        // For epoch 214 ending, rewards are for performance_epoch 213
                        let ending_epoch = boundary.ending_state().number;
                        if ending_epoch >= 1 {
                            let performance_epoch = ending_epoch - 1;
                            if let Err(e) = dump_applied_rewards_csv(
                                &boundary.applied_rewards,
                                cardano_network,
                                &dumps_dir,
                                performance_epoch,
                            ) {
                                eprintln!("failed to dump rewards csv: {e}");
                            }
                        }
                    }
                }
                _ => {}
            }
        })
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let epoch = captured_epoch
        .with_context(|| format!("epoch {subject_epoch} was never completed during the run"))?;

    eprintln!("captured completed epoch {} from estart callback", epoch.number);

    let gt_dir = tmp_path.join("ground-truth");
    std::fs::create_dir_all(&gt_dir)?;

    let mut failures = Vec::new();
    let epoch_str = subject_epoch.to_string();

    // Epochs: single-row comparison
    {
        let dolos_path = dumps_dir.join("epochs.csv");
        let gt_path = gt_dir.join("epochs.csv");
        eprintln!("\nComparing epochs (subject epoch {})", subject_epoch);

        write_epoch_row(&epoch, &dolos_path)?;
        extract_row_from_csv(performance_epochs, &epoch_str, &gt_path)?;

        match compare_csvs_with_ignore(&dolos_path, &gt_path, &[0], 20, |_| false) {
            Ok(n) if n > 0 => failures.push(format!("epochs ({n} diffs)")),
            Err(e) => failures.push(format!("epochs compare failed: {e}")),
            _ => {}
        }
    }

    // PParams: single-row comparison
    {
        let dolos_path = dumps_dir.join("pparams.csv");
        let gt_path = gt_dir.join("pparams.csv");
        eprintln!("\nComparing pparams (subject epoch {})", subject_epoch);

        write_pparam_row(&epoch, &dolos_path)?;
        extract_row_from_csv(gt_pparams, &epoch_str, &gt_path)?;

        match compare_csvs_with_ignore(&dolos_path, &gt_path, &[0], 20, |_| false) {
            Ok(n) if n > 0 => failures.push(format!("pparams ({n} diffs)")),
            Err(e) => failures.push(format!("pparams compare failed: {e}")),
            _ => {}
        }
    }

    // Eras: full comparison
    {
        let dolos_path = dumps_dir.join("eras.csv");
        let gt_path = gt_dir.join("eras.csv");
        eprintln!("\nComparing eras");

        dump_eras(harness.state(), &dolos_path)?;
        write_fixture(gt_eras, &gt_path)?;

        match compare_csvs_with_ignore(&dolos_path, &gt_path, &[0], 20, |diff| {
            let record = match diff {
                DiffByteRecord::Add(info) | DiffByteRecord::Delete(info) => {
                    info.byte_record()
                }
                DiffByteRecord::Modify { add, .. } => add.byte_record(),
            };
            record
                .get(0)
                .and_then(|v| std::str::from_utf8(v).ok())
                .and_then(|v| v.parse::<u16>().ok())
                .map(|p| p < 2)
                .unwrap_or(false)
        }) {
            Ok(n) if n > 0 => failures.push(format!("eras ({n} diffs)")),
            Err(e) => failures.push(format!("eras compare failed: {e}")),
            _ => {}
        }
    }

    // Delegation, stake, rewards: compare RUPD dumps against ground truth
    let performance_epoch = subject_epoch.saturating_sub(2);

    // Delegation
    {
        let dolos_path = dumps_dir.join(format!("delegation-{}.csv", performance_epoch));
        let gt_path = gt_dir.join(format!("delegation-{}.csv", performance_epoch));

        if has_data(gt_delegation) && dolos_path.exists() {
            eprintln!("\nComparing delegation (epoch {})", performance_epoch);
            write_fixture(gt_delegation, &gt_path)?;

            match compare_csvs_with_ignore(&dolos_path, &gt_path, &[0], 20, |_| false) {
                Ok(n) if n > 0 => failures.push(format!("delegation ({n} diffs)")),
                Err(e) => failures.push(format!("delegation compare failed: {e}")),
                _ => {}
            }
        } else {
            eprintln!("\nSkipping delegation (epoch {}): no data", performance_epoch);
        }
    }

    // Stake
    {
        let dolos_path = dumps_dir.join(format!("stake-{}.csv", performance_epoch));
        let gt_path = gt_dir.join(format!("stake-{}.csv", performance_epoch));

        if has_data(gt_stake) && dolos_path.exists() {
            eprintln!("\nComparing stake (epoch {})", performance_epoch);
            write_fixture(gt_stake, &gt_path)?;

            match compare_csvs_with_ignore(&dolos_path, &gt_path, &[0, 1], 20, |_| false) {
                Ok(n) if n > 0 => failures.push(format!("stake ({n} diffs)")),
                Err(e) => failures.push(format!("stake compare failed: {e}")),
                _ => {}
            }
        } else {
            eprintln!("\nSkipping stake (epoch {}): no data", performance_epoch);
        }
    }

    // Rewards
    {
        let dolos_path = dumps_dir.join(format!("rewards-{}.csv", performance_epoch));
        let gt_path = gt_dir.join(format!("rewards-{}.csv", performance_epoch));

        if has_data(gt_rewards) && dolos_path.exists() {
            eprintln!("\nComparing rewards (epoch {})", performance_epoch);
            write_fixture(gt_rewards, &gt_path)?;

            match compare_csvs_with_ignore(&dolos_path, &gt_path, &[0, 1, 3, 4], 20, |_| false) {
                Ok(n) if n > 0 => failures.push(format!("rewards ({n} diffs)")),
                Err(e) => failures.push(format!("rewards compare failed: {e}")),
                _ => {}
            }
        } else {
            eprintln!("\nSkipping rewards (epoch {}): no data", performance_epoch);
        }
    }

    if !failures.is_empty() {
        panic!(
            "\nMismatches for {network} subject epoch {subject_epoch}:\n  - {}",
            failures.join("\n  - ")
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Test functions
// ---------------------------------------------------------------------------

const MAINNET_SEED_EPOCHS: &[u64] = &[200, 270];
const PREVIEW_SEED_EPOCHS: &[u64] = &[500, 700];
const PREPROD_SEED_EPOCHS: &[u64] = &[200];

fn nearest_lower_seed(available: &[u64], subject_epoch: u64, network: &str) -> u64 {
    available
        .iter()
        .copied()
        .filter(|epoch| *epoch <= subject_epoch)
        .max()
        .unwrap_or_else(|| panic!("no seed available for {network} <= {subject_epoch}"))
}

fn seed_dir_for(network: &str, subject_epoch: u64) -> std::path::PathBuf {
    let base = std::env::var("DOLOS_SEED_DIR").expect("DOLOS_SEED_DIR must be set");

    let seed_epoch = match network {
        "mainnet" => nearest_lower_seed(MAINNET_SEED_EPOCHS, subject_epoch, network),
        "preview" => nearest_lower_seed(PREVIEW_SEED_EPOCHS, subject_epoch, network),
        "preprod" => nearest_lower_seed(PREPROD_SEED_EPOCHS, subject_epoch, network),
        other => panic!("unsupported network: {other}"),
    };

    std::path::Path::new(&base).join(format!("{network}-{seed_epoch}"))
}

#[test]
fn test_mainnet_250() {
    init_tracing();
    let seed_dir = seed_dir_for("mainnet", 250);
    run_epoch_pots_test(
        "mainnet",
        250,
        &seed_dir,
        fixtures::mainnet_250::EPOCHS,
        fixtures::mainnet_250::PPARAMS,
        fixtures::mainnet_250::ERAS,
        fixtures::mainnet_250::DELEGATION,
        fixtures::mainnet_250::STAKE,
        fixtures::mainnet_250::REWARDS,
    )
    .unwrap();
}

#[test]
fn test_mainnet_300() {
    init_tracing();
    let seed_dir = seed_dir_for("mainnet", 300);
    run_epoch_pots_test(
        "mainnet",
        300,
        &seed_dir,
        fixtures::mainnet_300::EPOCHS,
        fixtures::mainnet_300::PPARAMS,
        fixtures::mainnet_300::ERAS,
        fixtures::mainnet_300::DELEGATION,
        fixtures::mainnet_300::STAKE,
        fixtures::mainnet_300::REWARDS,
    )
    .unwrap();
}

#[test]
fn test_preview_550() {
    init_tracing();
    let seed_dir = seed_dir_for("preview", 550);
    run_epoch_pots_test(
        "preview",
        550,
        &seed_dir,
        fixtures::preview_550::EPOCHS,
        fixtures::preview_550::PPARAMS,
        fixtures::preview_550::ERAS,
        fixtures::preview_550::DELEGATION,
        fixtures::preview_550::STAKE,
        fixtures::preview_550::REWARDS,
    )
    .unwrap();
}

#[test]
fn test_preview_649() {
    init_tracing();
    let seed_dir = seed_dir_for("preview", 649);
    run_epoch_pots_test(
        "preview",
        649,
        &seed_dir,
        fixtures::preview_649::EPOCHS,
        fixtures::preview_649::PPARAMS,
        fixtures::preview_649::ERAS,
        fixtures::preview_649::DELEGATION,
        fixtures::preview_649::STAKE,
        fixtures::preview_649::REWARDS,
    )
    .unwrap();
}

#[test]
fn test_preview_700() {
    init_tracing();
    let seed_dir = seed_dir_for("preview", 700);
    run_epoch_pots_test(
        "preview",
        700,
        &seed_dir,
        fixtures::preview_700::EPOCHS,
        fixtures::preview_700::PPARAMS,
        fixtures::preview_700::ERAS,
        fixtures::preview_700::DELEGATION,
        fixtures::preview_700::STAKE,
        fixtures::preview_700::REWARDS,
    )
    .unwrap();
}
