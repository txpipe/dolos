use crate::prelude::*;
use dolos_cardano::{load_epoch, load_era_summary, AccountState, EraProtocol, PoolState};
use dolos_cardano::FixedNamespace as _;
use dolos_core::StateStore;
use pallas::codec::minicbor;
use pallas::codec::utils::{AnyCbor, Bytes, KeyValuePairs, TagWrap};
use pallas::network::miniprotocols::localstate::queries_v16 as q16;
use pallas::network::miniprotocols::localtxsubmission::SMaybe;
use std::collections::{BTreeSet, HashMap};
use tracing::debug;

pub fn build_stake_snapshots_response<D: Domain>(
    domain: &D,
    pools_filter: &SMaybe<q16::Pools>,
) -> Result<AnyCbor, Error> {
    let state = domain.state();

    let chain_summary = load_era_summary::<D>(state)
        .map_err(|e| Error::server(format!("failed to load era summary: {e}")))?;

    let epoch_state = load_epoch::<D>(state)
        .map_err(|e| Error::server(format!("failed to load epoch: {}", e)))?;
    let current_epoch = epoch_state.number;

    let filter_set: Option<BTreeSet<Vec<u8>>> = match pools_filter {
        SMaybe::Some(pools) => {
            let set: BTreeSet<Vec<u8>> = pools.0.iter().map(|p| p.to_vec()).collect();
            Some(set)
        }
        SMaybe::None => None,
    };

    let mut stake_snapshots: Vec<(Bytes, q16::Stakes)> = Vec::new();

    let protocol_for_epoch = |epoch: u64| -> EraProtocol {
        let era = chain_summary.era_for_epoch(epoch);
        EraProtocol::from(era.protocol)
    };

    type StakeSnapshotArg = (BTreeSet<Vec<u8>>, HashMap<Vec<u8>, u64>, u64);

    let gather_for_epoch = |stake_epoch: u64,
                            protocol: EraProtocol,
                            filter_set: &Option<BTreeSet<Vec<u8>>>,
                            state: &D::State|
     -> Result<StakeSnapshotArg, Error> {
        let mut active_pools: BTreeSet<Vec<u8>> = BTreeSet::new();
        let mut pool_stakes: HashMap<Vec<u8>, u64> = HashMap::new();
        let mut total_active: u64 = 0;

        let pools_iter = state
            .iter_entities_typed::<PoolState>(PoolState::NS, None)
            .map_err(|e| Error::server(format!("failed to iterate pools: {e}")))?;

        let mut all_active_pools: BTreeSet<Vec<u8>> = BTreeSet::new();
        for record in pools_iter {
            let (_, pool) =
                record.map_err(|e| Error::server(format!("failed to read pool: {e}")))?;
            let pool_id = pool.operator.to_vec();

            if let Some(snapshot) = pool.snapshot.snapshot_at(stake_epoch) {
                if !snapshot.is_retired {
                    all_active_pools.insert(pool_id.clone());
                    if filter_set.is_none() || filter_set.as_ref().unwrap().contains(&pool_id) {
                        active_pools.insert(pool_id);
                    }
                }
            }
        }

        let accounts_iter = state
            .iter_entities_typed::<AccountState>(AccountState::NS, None)
            .map_err(|e| Error::server(format!("failed to iterate accounts: {e}")))?;

        for record in accounts_iter {
            let (_, account) =
                record.map_err(|e| Error::server(format!("failed to read account: {e}")))?;

            let Some(pool_hash) = account.delegated_pool_at(stake_epoch) else {
                continue;
            };

            let pool_id = pool_hash.to_vec();

            if !all_active_pools.contains(&pool_id) {
                continue;
            }

            let stake_amount = account
                .stake
                .snapshot_at(stake_epoch)
                .map(|x| x.total_for_era(protocol))
                .unwrap_or_default();

            if stake_amount == 0 {
                continue;
            }

            total_active = total_active.saturating_add(stake_amount);

            if active_pools.contains(&pool_id) {
                pool_stakes
                    .entry(pool_id.clone())
                    .and_modify(|x| *x = x.saturating_add(stake_amount))
                    .or_insert(stake_amount);
            }
        }

        Ok((active_pools, pool_stakes, total_active))
    };

    let mark_epoch = current_epoch.saturating_sub(1);
    let set_epoch = current_epoch.saturating_sub(2);
    let go_epoch = current_epoch.saturating_sub(3);

    let (mark_active_pools, mark_stakes, mark_total_active) = gather_for_epoch(
        mark_epoch,
        protocol_for_epoch(mark_epoch),
        &filter_set,
        state,
    )?;
    let (set_active_pools, set_stakes, set_total_active) =
        gather_for_epoch(set_epoch, protocol_for_epoch(set_epoch), &filter_set, state)?;
    let (go_active_pools, go_stakes, go_total_active) =
        gather_for_epoch(go_epoch, protocol_for_epoch(go_epoch), &filter_set, state)?;

    let mut all_pools: BTreeSet<Vec<u8>> = mark_active_pools
        .union(&set_active_pools)
        .cloned()
        .collect();
    all_pools.extend(go_active_pools);

    let mark_total = mark_total_active;
    let set_total = set_total_active;
    let go_total = go_total_active;

    for pool_id_bytes in all_pools {
        let mark_stake = *mark_stakes.get(&pool_id_bytes).unwrap_or(&0);
        let set_stake = *set_stakes.get(&pool_id_bytes).unwrap_or(&0);
        let go_stake = *go_stakes.get(&pool_id_bytes).unwrap_or(&0);

        stake_snapshots.push((
            pool_id_bytes.clone().into(),
            q16::Stakes {
                snapshot_mark_pool: mark_stake,
                snapshot_set_pool: set_stake,
                snapshot_go_pool: go_stake,
            },
        ));
    }

    debug!(
        num_pools = stake_snapshots.len(),
        mark_total, set_total, go_total, "returning stake snapshots"
    );

    let response = q16::StakeSnapshots {
        stake_snapshots: KeyValuePairs::Def(stake_snapshots),
        snapshot_stake_mark_total: mark_total,
        snapshot_stake_set_total: set_total,
        snapshot_stake_go_total: go_total,
    };

    let encoded = minicbor::to_vec(response)
        .map_err(|e| Error::server(format!("failed to encode stake snapshots: {e}")))?;

    let wrapped = TagWrap::<Bytes, 24>(encoded.into());
    Ok(AnyCbor::from_encode(vec![wrapped]))
}
