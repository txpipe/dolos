use std::io::Write;
use std::path::Path;

use dolos_core::Genesis;
use pallas::ledger::addresses::Network;

use crate::pallas_extras;

use super::{PoolHash, RupdWork, StakeSnapshot};

fn network_from_genesis(genesis: &Genesis) -> Network {
    match genesis.shelley.network_id.as_deref() {
        Some("Mainnet") => Network::Mainnet,
        _ => Network::Testnet,
    }
}

fn bech32_pool(pool: &PoolHash) -> std::io::Result<String> {
    let hrp = bech32::Hrp::parse_unchecked("pool");
    bech32::encode::<bech32::Bech32>(hrp, pool.as_slice())
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))
}

pub fn dump_snapshot_csv(work: &RupdWork, genesis: &Genesis, out_dir: &Path) {
    let Some((snapshot_epoch, _)) = work.relevant_epochs() else {
        return;
    };

    let network = network_from_genesis(genesis);

    if let Err(err) = std::fs::create_dir_all(out_dir) {
        tracing::warn!(error = %err, "rupd snapshot dump: failed to create output dir");
        return;
    }

    let pools_path = out_dir.join(format!("{}-pools.csv", snapshot_epoch));
    let accounts_path = out_dir.join(format!("{}-accounts.csv", snapshot_epoch));

    if let Err(err) = dump_pools_csv(&work.snapshot, &pools_path) {
        tracing::warn!(
            error = %err,
            path = %pools_path.display(),
            "rupd snapshot dump: failed to write pools csv"
        );
    }

    if let Err(err) = dump_accounts_csv(&work.snapshot, network, &accounts_path) {
        tracing::warn!(
            error = %err,
            path = %accounts_path.display(),
            "rupd snapshot dump: failed to write accounts csv"
        );
    }
}

fn dump_pools_csv(snapshot: &StakeSnapshot, path: &Path) -> std::io::Result<()> {
    let file = std::fs::File::create(path)?;
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

fn dump_accounts_csv(
    snapshot: &StakeSnapshot,
    network: Network,
    path: &Path,
) -> std::io::Result<()> {
    let file = std::fs::File::create(path)?;
    let mut writer = std::io::BufWriter::new(file);

    writeln!(writer, "stake,pool,lovelace")?;

    let mut rows = Vec::new();

    for (pool, credential, stake) in snapshot.iter_accounts() {
        let address = pallas_extras::stake_credential_to_address(network, credential);
        let stake_address = address
            .to_bech32()
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?;
        let pool_bech32 = bech32_pool(pool)?;
        rows.push((stake_address, pool_bech32, *stake));
    }

    rows.sort_by(|a, b| {
        let key_a = (&a.0, &a.1);
        let key_b = (&b.0, &b.1);
        key_a.cmp(&key_b)
    });

    for (stake_address, pool_bech32, stake) in rows {
        writeln!(writer, "{},{},{}", stake_address, pool_bech32, stake)?;
    }

    Ok(())
}
