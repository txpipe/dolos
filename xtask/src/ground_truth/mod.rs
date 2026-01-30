//! cardano-ground-truth command implementation.
//!
//! Generates ground-truth fixtures from cardano-db-sync for integration tests.

use anyhow::{bail, Context, Result};
use clap::Args;
use dolos_cardano::{pots::Pots, EpochState, EraBoundary, EraSummary, Nonces};
use postgres::{Client, NoTls};

use crate::config::{load_xtask_config, Network};
use crate::util::{dir_has_entries, resolve_path};

/// Arguments for cardano-ground-truth command.
#[derive(Debug, Args)]
pub struct GroundTruthArgs {
    /// Target network
    #[arg(long, value_enum)]
    pub network: Network,

    /// Generate ground-truth from origin up to this epoch (inclusive)
    #[arg(long)]
    pub epoch: u64,

    /// Overwrite existing ground-truth files
    #[arg(long, action)]
    pub force: bool,
}

/// Run the cardano-ground-truth command.
pub fn run(args: &GroundTruthArgs) -> Result<()> {
    let repo_root = std::env::current_dir().context("detecting repo root")?;
    let xtask_config = load_xtask_config(&repo_root)?;

    let instances_root = resolve_path(&repo_root, &xtask_config.instances_root);

    let dbsync_url = xtask_config
        .dbsync
        .url_for_network(&args.network)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no dbsync URL configured for network '{}' in xtask.toml",
                args.network.as_str()
            )
        })?;

    // Ground-truth goes inside the instance folder
    let instance_name = format!("test-{}-{}", args.network.as_str(), args.epoch);
    let instance_dir = instances_root.join(&instance_name);
    let output_dir = instance_dir.join("ground-truth");

    if !instance_dir.exists() {
        bail!(
            "instance not found: {}\n       Run: cargo xtask create-test-instance --network {} --epoch {}",
            instance_dir.display(),
            args.network.as_str(),
            args.epoch
        );
    }

    if output_dir.exists() && dir_has_entries(&output_dir)? && !args.force {
        bail!(
            "ground-truth already exists (use --force to overwrite): {}",
            output_dir.display()
        );
    }

    std::fs::create_dir_all(&output_dir)
        .with_context(|| format!("creating output dir: {}", output_dir.display()))?;

    println!(
        "Generating ground-truth for {} epoch {} from DBSync...",
        args.network.as_str(),
        args.epoch
    );
    println!(" DBSync URL: {}", dbsync_url);
    println!("  Output dir: {}", output_dir.display());

    // Fetch data from DBSync
    let eras = fetch_eras_from_dbsync(dbsync_url, args.epoch, &args.network)?;
    let epoch_limit = args.epoch.saturating_sub(1);
    let epochs = fetch_epochs_from_dbsync(dbsync_url, epoch_limit)?;

    // Write eras.json
    let eras_path = output_dir.join("eras.json");
    let eras_json = serde_json::to_string_pretty(&eras).context("serializing eras")?;
    std::fs::write(&eras_path, eras_json)
        .with_context(|| format!("writing eras: {}", eras_path.display()))?;
    println!("  Wrote: {}", eras_path.display());

    // Write epochs.json
    let epochs_path = output_dir.join("epochs.json");
    let epochs_json = serde_json::to_string_pretty(&epochs).context("serializing epochs")?;
    std::fs::write(&epochs_path, epochs_json)
        .with_context(|| format!("writing epochs: {}", epochs_path.display()))?;
    println!(" Wrote: {}", epochs_path.display());

    println!("Ground-truth generation complete.");

    Ok(())
}

// -----------------------------------------------------------------------------
// DBSync query implementations
// -----------------------------------------------------------------------------

/// Connect to DBSync using the provided URL.
fn connect_to_dbsync(dbsync_url: &str) -> Result<Client> {
    let client = Client::connect(dbsync_url, NoTls)
        .with_context(|| format!("Failed to connect to DBSync: {}", dbsync_url))?;
    Ok(client)
}

/// Fetch era summaries from DBSync up to (and including) the era containing `max_epoch`.
fn fetch_eras_from_dbsync(
    dbsync_url: &str,
    max_epoch: u64,
    network: &Network,
) -> Result<Vec<EraSummary>> {
    let mut client = connect_to_dbsync(dbsync_url)?;

    // Query epoch boundaries and protocol versions
    let query = r#"
        SELECT 
            e.no::bigint AS epoch_no,
            EXTRACT(EPOCH FROM e.start_time)::bigint AS start_time,
            ep.protocol_major::int4
        FROM epoch e
        JOIN epoch_param ep ON ep.epoch_no = e.no
        WHERE e.no <= $1
        ORDER BY e.no
    "#;

    let max_epoch = i32::try_from(max_epoch)
        .map_err(|_| anyhow::anyhow!("max_epoch out of range for dbsync (expected i32)"))?;
    let rows = client
        .query(query, &[&max_epoch])
        .with_context(|| "Failed to query epoch boundaries")?;

    let mut ordered_epochs: Vec<(u64, u64, u16)> = Vec::new();

    for row in rows {
        let epoch_no: i64 = row.get(0);
        let start_time: i64 = row.get(1);
        let protocol_major: i32 = row.get(2);

        ordered_epochs.push((epoch_no as u64, start_time as u64, protocol_major as u16));
    }

    ordered_epochs.sort_by_key(|(epoch, _, _)| *epoch);

    let mut era_starts: Vec<(u16, u64, u64)> = Vec::new();
    let mut last_protocol: Option<u16> = None;

    for (epoch, timestamp, protocol) in &ordered_epochs {
        if last_protocol.map(|p| p != *protocol).unwrap_or(true) {
            era_starts.push((*protocol, *epoch, *timestamp));
            last_protocol = Some(*protocol);
        }
    }

    let mut eras = Vec::new();

    for (idx, (protocol, epoch, timestamp)) in era_starts.iter().enumerate() {
        let start = EraBoundary {
            epoch: *epoch,
            slot: 0,
            timestamp: *timestamp,
        };

        let end = era_starts
            .get(idx + 1)
            .map(|(_, next_epoch, next_timestamp)| EraBoundary {
                epoch: *next_epoch,
                slot: 0,
                timestamp: *next_timestamp,
            });

        let (epoch_length, slot_length) = if *protocol >= 4 {
            let epoch_length = match network {
                Network::Mainnet => 432000,
                Network::Preview | Network::Preprod => 86400,
            };
            (epoch_length, 1)
        } else {
            (21600, 20)
        };

        eras.push(EraSummary {
            start,
            end,
            epoch_length,
            slot_length,
            protocol: *protocol,
        });
    }

    Ok(eras)
}

/// Fetch epoch states from DBSync up to (and including) `max_epoch`.
fn fetch_epochs_from_dbsync(dbsync_url: &str, max_epoch: u64) -> Result<Vec<EpochState>> {
    let mut client = connect_to_dbsync(dbsync_url)?;

    // Query epoch data with account and pool counts
    let query = r#"
        SELECT 
            e.no::bigint AS epoch_no,
            ep.nonce AS epoch_nonce,
            ap.treasury::text AS treasury,
            ap.reserves::text AS reserves,
            ap.rewards::text AS rewards,
            ap.utxo::text AS utxo,
            ap.deposits_stake::text AS deposits_stake,
            ap.fees::text AS fees_pot,
            COALESCE(sa.account_count, 0)::bigint AS account_count,
            COALESCE(pa.pool_count, 0)::bigint AS pool_count
        FROM epoch e
        JOIN epoch_param ep ON ep.epoch_no = e.no
        LEFT JOIN ada_pots ap ON ap.epoch_no = e.no
        LEFT JOIN (
            SELECT COUNT(*) as account_count
            FROM stake_address
        ) sa ON true
        LEFT JOIN (
            SELECT COUNT(*) as pool_count
            FROM pool_hash
        ) pa ON true
        WHERE e.no <= $1
        ORDER BY e.no
    "#;

    let max_epoch = i32::try_from(max_epoch)
        .map_err(|_| anyhow::anyhow!("max_epoch out of range for dbsync (expected i32)"))?;
    let rows = client
        .query(query, &[&max_epoch])
        .with_context(|| "Failed to query epoch states")?;

    let mut epochs = Vec::new();

    for row in rows {
        let epoch_no: i64 = row.get(0);
        let epoch_nonce: Option<Vec<u8>> = row.get(1);
        let treasury: Option<String> = row.get(2);
        let reserves: Option<String> = row.get(3);
        let rewards: Option<String> = row.get(4);
        let utxo: Option<String> = row.get(5);
        let deposits_stake: Option<String> = row.get(6);
        let fees_pot: Option<String> = row.get(7);
        let account_count: i64 = row.get(8);
        let pool_count: i64 = row.get(9);

        let epoch_nonce = epoch_nonce.unwrap_or_else(|| vec![0u8; 32]);
        let nonce_hash = epoch_nonce.try_into().unwrap_or([0u8; 32]);
        let nonces = Nonces {
            active: nonce_hash.into(),
            evolving: [0u8; 32].into(),
            candidate: [0u8; 32].into(),
            tail: None,
        };

        let treasury = parse_i64("treasury", treasury.as_deref().unwrap_or("0"))?;
        let reserves = parse_i64("reserves", reserves.as_deref().unwrap_or("0"))?;
        let rewards = parse_i64("rewards", rewards.as_deref().unwrap_or("0"))?;
        let utxo = parse_i64("utxo", utxo.as_deref().unwrap_or("0"))?;
        let deposits_stake = parse_i64("deposits_stake", deposits_stake.as_deref().unwrap_or("0"))?;
        let fees_pot = parse_i64("fees", fees_pot.as_deref().unwrap_or("0"))?;

        let pots = Pots {
            treasury: treasury as u64,
            reserves: reserves as u64,
            rewards: rewards as u64,
            utxos: utxo as u64,
            fees: fees_pot as u64,
            pool_count: pool_count as u64,
            account_count: account_count as u64,
            deposit_per_pool: 0,    // Would need additional queries
            deposit_per_account: 0, // Would need additional queries
            nominal_deposits: deposits_stake as u64,
            drep_deposits: 0,     // Not available in current query
            proposal_deposits: 0, // Not available in current query
        };

        epochs.push(EpochState {
            number: epoch_no as u64,
            initial_pots: pots.clone(),
            nonces: Some(nonces),
            ..EpochState::default()
        });
    }

    Ok(epochs)
}

fn parse_i64(field: &str, value: &str) -> Result<i64> {
    value
        .parse::<i64>()
        .with_context(|| format!("failed to parse {} from dbsync: {}", field, value))
}
