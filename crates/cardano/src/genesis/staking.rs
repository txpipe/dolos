use std::collections::{BTreeSet, HashMap};
use std::net::{Ipv4Addr, Ipv6Addr};

use dolos_core::{ChainError, Domain, Genesis, StateStore as _, StateWriter as _};
use pallas::codec::utils::Bytes;
use pallas::ledger::addresses::{Address, Network, StakeAddress, StakePayload};
use pallas::ledger::primitives::{PoolMetadata, Relay, StakeCredential};
use tracing::warn;

use crate::{
    pallas_extras, AccountKey, AccountState, DRepDelegation, EpochValue, PoolDelegation, PoolHash,
    PoolKey, PoolParams, PoolSnapshot, PoolState, Stake,
};

use pallas::crypto::hash::Hash;
use pallas::interop::hardano::configs::shelley::{
    Credential as ConfigCredential, Metadata as ConfigMetadata, Pool as ConfigPool,
    Relay as ConfigRelay, RewardAccount as ConfigRewardAccount,
};

fn malformed(what: &str, value: &str) -> ChainError {
    ChainError::InvalidConfig(format!(
        "shelley genesis staking has a malformed {what}: {value}"
    ))
}

fn parse_hash<const N: usize>(what: &str, value: &str) -> Result<Hash<N>, ChainError> {
    value.parse().map_err(|_| malformed(what, value))
}

fn parse_reward_account(reward_account: &ConfigRewardAccount) -> Result<Vec<u8>, ChainError> {
    let network = match reward_account.network.as_str() {
        "MainNet" => Network::Mainnet,
        _ => Network::Testnet,
    };

    let hash = match &reward_account.credential {
        ConfigCredential::KeyHash(x) => x,
        ConfigCredential::ScriptHash(x) => x,
    };

    let hash = parse_hash::<28>("reward account", hash)?;

    let payload = match &reward_account.credential {
        ConfigCredential::KeyHash(_) => StakePayload::Stake(hash),
        ConfigCredential::ScriptHash(_) => StakePayload::Script(hash),
    };

    let address = Address::Stake(StakeAddress::new(network, payload));

    Ok(address.to_vec())
}

fn parse_ipv4(value: &str) -> Result<Bytes, ChainError> {
    let ip: Ipv4Addr = value.parse().map_err(|_| malformed("relay IPv4", value))?;

    Ok(Bytes::from(ip.octets().to_vec()))
}

fn parse_ipv6(value: &str) -> Result<Bytes, ChainError> {
    let ip: Ipv6Addr = value.parse().map_err(|_| malformed("relay IPv6", value))?;
    let mut bytes = ip.octets();

    // The ledger stores an IPv6 relay as four little-endian 32-bit words.
    for word in bytes.chunks_exact_mut(4) {
        word.reverse();
    }

    Ok(Bytes::from(bytes.to_vec()))
}

fn parse_relay(kind: &str, relay: &ConfigRelay) -> Result<Option<Relay>, ChainError> {
    let relay = match relay {
        // pallas reads a DNS relay as an address relay and drops its name.
        ConfigRelay::SingleHostAddr(_) if kind != "single host address" => {
            warn!(
                kind,
                "skipping a genesis pool relay whose DNS name was lost"
            );
            return Ok(None);
        }
        ConfigRelay::SingleHostAddr(x) => Relay::SingleHostAddr(
            x.port,
            x.ipv4.as_deref().map(parse_ipv4).transpose()?,
            x.ipv6.as_deref().map(parse_ipv6).transpose()?,
        ),
        ConfigRelay::SingleHostName(x) => Relay::SingleHostName(x.port, x.dns_name.clone()),
        ConfigRelay::MultiHostName(x) => Relay::MultiHostName(x.dns_name.clone()),
    };

    Ok(Some(relay))
}

fn parse_relays(relays: &[HashMap<String, ConfigRelay>]) -> Result<Vec<Relay>, ChainError> {
    let mut parsed = vec![];

    for (kind, relay) in relays.iter().flatten() {
        parsed.extend(parse_relay(kind, relay)?);
    }

    Ok(parsed)
}

fn parse_metadata(metadata: &ConfigMetadata) -> Result<PoolMetadata, ChainError> {
    let hash = parse_hash::<32>("pool metadata hash", &metadata.hash)?;

    Ok(PoolMetadata {
        url: metadata.url.clone(),
        hash: Bytes::from(hash.to_vec()),
    })
}

fn parse_pool(dto: &ConfigPool) -> Result<PoolState, ChainError> {
    let pool_owners: BTreeSet<Hash<28>> = dto
        .owners
        .iter()
        .map(|owner| parse_hash("pool owner", owner))
        .collect::<Result<_, _>>()?;

    let snapshot = PoolSnapshot {
        is_new: true,
        is_retired: false,
        blocks_minted: 0,
        params: PoolParams {
            pledge: dto.pledge,
            cost: dto.cost,
            margin: dto.margin.clone(),
            reward_account: parse_reward_account(&dto.reward_account)?,
            vrf_keyhash: parse_hash("pool vrf", &dto.vrf)?,
            pool_owners: pool_owners.into_iter().collect(),
            relays: parse_relays(&dto.relays)?,
            pool_metadata: dto.metadata.as_ref().map(parse_metadata).transpose()?,
        },
        stake: None,
        delegators: None,
        live_pledge: None,
    };

    let operator = parse_hash("pool id", &dto.public_key)?;

    Ok(PoolState {
        snapshot: EpochValue::with_live(0, snapshot),
        retiring_epoch: None,
        operator,
        deposit: 0,
        calidus: None,
    })
}

fn find_initial_utxo_sum(
    credential: &StakeCredential,
    genesis: &Genesis,
) -> Result<u64, ChainError> {
    let Some(initial_funds) = &genesis.shelley.initial_funds else {
        return Ok(0);
    };

    let mut sum = 0;

    for (address, amount) in initial_funds {
        let address: Address = address.parse()?;

        if let Some((candidate, _)) = pallas_extras::address_as_stake_cred(&address) {
            if credential == &candidate {
                sum += amount;
            }
        }
    }

    Ok(sum)
}

fn parse_delegation(
    account: &str,
    pool: &str,
    genesis: &Genesis,
) -> Result<AccountState, ChainError> {
    let keyhash = parse_hash::<28>("delegator", account)?;
    let credential = StakeCredential::AddrKeyhash(keyhash);

    let pool: PoolHash = parse_hash("delegated pool", pool)?;

    let stake = Stake {
        rewards_sum: 0,
        withdrawals_sum: 0,
        utxo_sum_at_pointer_addresses: 0,
        utxo_sum: find_initial_utxo_sum(&credential, genesis)?,
    };

    Ok(AccountState {
        credential,
        pool: EpochValue::with_live(0, PoolDelegation::Pool(pool)),
        drep: EpochValue::with_live(0, DRepDelegation::NotDelegated),
        registered_at: Some(0),
        vote_delegated_at: None,
        deregistered_at: None,
        retired_pool: None,

        stake: EpochValue::with_live(0, stake),
    })
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
        let state = parse_pool(dto)?;
        writer.write_entity_typed(&PoolKey(state.operator), &state)?;
    }

    for (account, pool) in staking.stake.iter().flatten() {
        let state = parse_delegation(account, pool, genesis)?;
        writer.write_entity_typed(&AccountKey::from(&state.credential), &state)?;
    }

    writer.commit()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use dolos_core::{Domain as _, StateStore as _};
    use dolos_testing::toy_domain::ToyDomain;
    use pallas::ledger::addresses::{ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart};
    use serde_json::json;

    use super::*;

    const POOL: &str = "9c70bd513f16961b3debef699da1fb8c77138edd149f00f9b3b5522d";
    const VRF: &str = "63986178c45c411fb4ab0fe4f8717a102168ceb6727bcfec68edf577c08349bd";
    const OWNER_A: &str = "1111111111111111111111111111111111111111111111111111111a";
    const OWNER_B: &str = "2222222222222222222222222222222222222222222222222222222b";
    const DELEGATOR: &str = "ae1537988b77a8815a7502eb6e02026bc71d0a7729cddc2a21582980";
    const METADATA_HASH: &str = "8c2bc8429a2fb885c1b17a8095b927cfcfe97e4fa5fe83f0dd4418ab01234567";

    fn pool_json(relays: serde_json::Value) -> serde_json::Value {
        json!({
            "publicKey": POOL,
            "vrf": VRF,
            "pledge": 1000,
            "cost": 340,
            "margin": 0.5,
            "rewardAccount": { "credential": { "keyHash": OWNER_A }, "network": "Testnet" },
            "owners": [OWNER_B, OWNER_A, OWNER_B],
            "relays": relays,
            "metadata": { "url": "https://pool.example/meta.json", "hash": METADATA_HASH },
        })
    }

    fn staked(pool: serde_json::Value) -> serde_json::Value {
        json!({ "pools": { POOL: pool }, "stake": { DELEGATOR: POOL } })
    }

    fn genesis_with(staking: serde_json::Value) -> Genesis {
        let mut genesis = crate::include::devnet::load();
        genesis.shelley.staking = Some(serde_json::from_value(staking).unwrap());
        genesis
    }

    fn bootstrapped(genesis: &Genesis) -> ToyDomain {
        let domain = ToyDomain::new(None, None);
        bootstrap::<ToyDomain>(domain.state(), genesis).unwrap();
        domain
    }

    fn read_pool(domain: &ToyDomain) -> Option<PoolState> {
        let key = PoolKey(POOL.parse().unwrap());
        domain.state().read_entity_typed::<PoolState>(&key).unwrap()
    }

    fn base_address(payment: u8, stake: &str) -> String {
        ShelleyAddress::new(
            Network::Testnet,
            ShelleyPaymentPart::key_hash(Hash::new([payment; 28])),
            ShelleyDelegationPart::key_hash(stake.parse().unwrap()),
        )
        .to_hex()
    }

    #[test]
    fn pool_keeps_owners_relays_and_metadata() {
        let relays = json!([
            { "single host address": { "IPv4": "10.0.0.1", "IPv6": null, "port": 3001 } },
            { "single host address": { "IPv4": null, "IPv6": "2001:db8::1", "port": null } },
            { "single host name": { "dnsName": "relay.pool.example", "port": 3002 } },
            { "multi host name": { "dnsName": "pool.example" } },
        ]);
        let genesis = genesis_with(staked(pool_json(relays)));

        let pool = read_pool(&bootstrapped(&genesis)).expect("genesis pool written");
        let params = &pool.snapshot.unwrap_live().params;

        let owners: Vec<Hash<28>> = vec![OWNER_A.parse().unwrap(), OWNER_B.parse().unwrap()];
        assert_eq!(params.pool_owners, owners);

        // pallas drops the DNS relays' names, so only the address relays are checked.
        let address_relays: Vec<_> = params
            .relays
            .iter()
            .filter(|relay| matches!(relay, Relay::SingleHostAddr(..)))
            .cloned()
            .collect();
        let ipv6 = [
            0xb8, 0x0d, 0x01, 0x20, 0, 0, 0, 0, 0, 0, 0, 0, 0x01, 0, 0, 0,
        ];
        assert_eq!(
            address_relays,
            vec![
                Relay::SingleHostAddr(Some(3001), Some(Bytes::from(vec![10, 0, 0, 1])), None),
                Relay::SingleHostAddr(None, None, Some(Bytes::from(ipv6.to_vec()))),
            ]
        );

        let metadata = params.pool_metadata.as_ref().expect("pool metadata kept");
        assert_eq!(metadata.url, "https://pool.example/meta.json");
        assert_eq!(hex::encode(metadata.hash.as_slice()), METADATA_HASH);
    }

    #[test]
    fn pools_are_kept_without_delegations() {
        let genesis = genesis_with(json!({ "pools": { POOL: pool_json(json!([])) } }));

        assert!(read_pool(&bootstrapped(&genesis)).is_some());
    }

    #[test]
    fn delegation_stake_sums_every_initial_fund() {
        let mut genesis = genesis_with(staked(pool_json(json!([]))));
        genesis.shelley.initial_funds = Some(HashMap::from([
            (base_address(1, DELEGATOR), 300),
            (base_address(2, DELEGATOR), 200),
            (base_address(3, OWNER_A), 1000),
        ]));

        let domain = bootstrapped(&genesis);

        let credential = StakeCredential::AddrKeyhash(DELEGATOR.parse().unwrap());
        let account = domain
            .state()
            .read_entity_typed::<AccountState>(&AccountKey(credential))
            .unwrap()
            .expect("genesis delegator written");

        assert_eq!(account.stake.unwrap_live().utxo_sum, 500);
    }

    #[test]
    fn malformed_pool_is_an_error() {
        let mut pool = pool_json(json!([]));
        pool["vrf"] = json!("not a hash");
        let genesis = genesis_with(staked(pool));

        let domain = ToyDomain::new(None, None);
        let err = bootstrap::<ToyDomain>(domain.state(), &genesis).unwrap_err();

        assert!(matches!(err, ChainError::InvalidConfig(_)), "{err}");
    }

    #[test]
    fn malformed_delegation_is_an_error() {
        let genesis = genesis_with(json!({
            "pools": { POOL: pool_json(json!([])) },
            "stake": { "not a hash": POOL },
        }));

        let domain = ToyDomain::new(None, None);
        let err = bootstrap::<ToyDomain>(domain.state(), &genesis).unwrap_err();

        assert!(matches!(err, ChainError::InvalidConfig(_)), "{err}");
    }
}
