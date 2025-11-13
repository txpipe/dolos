use dolos_core::{ChainError, Domain, EntityKey, Genesis, StateStore as _, StateWriter as _};
use pallas::codec::minicbor;
use pallas::ledger::addresses::{Address, Network, StakeAddress, StakePayload};
use pallas::ledger::primitives::StakeCredential;

use crate::{
    pallas_extras, AccountState, EpochValue, PoolDelegation, PoolHash, PoolParams, PoolSnapshot,
    PoolState, Stake,
};

use pallas::crypto::hash::Hash;
use pallas::ledger::configs::shelley::{
    Credential as ConfigCredential, Pool as ConfigPool, RewardAccount as ConfigRewardAccount,
};

fn parse_reward_account(reward_account: &ConfigRewardAccount) -> Vec<u8> {
    let network = match reward_account.network.as_str() {
        "MainNet" => Network::Mainnet,
        _ => Network::Testnet,
    };

    let hash = match &reward_account.credential {
        ConfigCredential::KeyHash(x) => x,
        ConfigCredential::ScriptHash(x) => x,
    };

    let hash = hash.parse::<Hash<28>>().unwrap();

    let payload = match &reward_account.credential {
        ConfigCredential::KeyHash(_) => StakePayload::Stake(hash),
        ConfigCredential::ScriptHash(_) => StakePayload::Script(hash),
    };

    let address = Address::Stake(StakeAddress::new(network, payload));

    address.to_vec()
}

fn parse_pool(dto: &ConfigPool) -> PoolState {
    let snapshot = PoolSnapshot {
        is_new: true,
        is_retired: false,
        blocks_minted: 0,
        params: PoolParams {
            pledge: dto.pledge,
            cost: dto.cost,
            margin: dto.margin.clone(),
            reward_account: parse_reward_account(&dto.reward_account),
            vrf_keyhash: dto.vrf.parse().unwrap(),

            // TODO: finish parsing the initial pool set
            pool_owners: vec![],
            relays: vec![],
            pool_metadata: None,
        },
    };

    let operator = dto.public_key.parse().unwrap();

    PoolState {
        snapshot: EpochValue::with_live(0, snapshot),
        blocks_minted_total: 0,
        register_slot: 0,
        retiring_epoch: None,
        operator,
        deposit: 0,
    }
}

fn find_initial_utxo_sum(credential: &StakeCredential, genesis: &Genesis) -> u64 {
    let Some(initial_funds) = &genesis.shelley.initial_funds else {
        return 0;
    };

    for (address, amount) in initial_funds {
        let address: Address = address.parse().unwrap();

        if let Some((candidate, _)) = pallas_extras::address_as_stake_cred(&address) {
            if credential == &candidate {
                return *amount;
            }
        }
    }

    return 0;
}

fn parse_delegation(account: &str, pool: &str, genesis: &Genesis) -> AccountState {
    let keyhash: Hash<28> = account.parse().unwrap();
    let credential = StakeCredential::AddrKeyhash(keyhash);

    let pool: PoolHash = pool.parse().unwrap();

    let stake = Stake {
        rewards_sum: 0,
        withdrawals_sum: 0,
        utxo_sum_at_pointer_addresses: 0,
        utxo_sum: find_initial_utxo_sum(&credential, genesis),
    };

    AccountState {
        credential,
        pool: EpochValue::with_live(0, PoolDelegation::Pool(pool)),
        drep: EpochValue::with_live(0, None),
        registered_at: Some(0),
        vote_delegated_at: None,
        deregistered_at: None,

        stake: EpochValue::with_live(0, stake),
    }
}

pub fn bootstrap<D: Domain>(state: &D::State, genesis: &Genesis) -> Result<(), ChainError> {
    let writer = state.start_writer()?;

    let Some(staking) = &genesis.shelley.staking else {
        return Ok(());
    };

    let Some(pools) = &staking.pools else {
        return Ok(());
    };

    for dto in pools.values() {
        let state = parse_pool(dto);
        writer.write_entity_typed(&EntityKey::from(state.operator.as_slice()), &state)?;
    }

    let Some(delegations) = &staking.stake else {
        return Ok(());
    };

    for (account, pool) in delegations {
        let state = parse_delegation(account, pool, genesis);
        let key = minicbor::to_vec(&state.credential).unwrap();
        let key = EntityKey::from(key);
        writer.write_entity_typed(&key, &state)?;
    }

    writer.commit()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use pallas::ledger::primitives::StakeCredential;

    use super::*;

    #[test]
    fn test_a() {
        let bytes =
            hex::decode("d27cb9a1a408b81239b55ec03f6e004deffcc094e7252d5c45743784").unwrap();
        let credential: StakeCredential = pallas::codec::minicbor::decode(&bytes).unwrap();
        dbg!(&credential);
    }
}

// d27cb9a1a408b81239b55ec03f6e 004deffcc094e7252d5c45743784
