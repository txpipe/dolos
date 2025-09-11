use std::{
    collections::{HashMap, HashSet},
    net::{Ipv4Addr, Ipv6Addr},
    path::PathBuf,
    str::FromStr,
};

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use chrono::NaiveDateTime;
use dolos_cardano::{
    build_schema, include, AccountState, AssetState, EraBoundary, EraSummary, FixedNamespace,
    PoolState, RewardLog,
};
use dolos_core::{EntityKey, Genesis, State3Store as _};
use handlebars::Handlebars;
use miette::{bail, Context, IntoDiagnostic};
use pallas::{
    codec::minicbor,
    ledger::{
        addresses::Address,
        primitives::{PoolMetadata, RationalNumber, Relay},
    },
};
use serde_json::Value;
use tokio_postgres::types::Json;
use tokio_postgres::NoTls;

use crate::{queries::init_registry, utils::account_key};

macro_rules! from_row {
    ($row:ident, $type:ty, $name:literal) => {
        $row.try_get::<_, $type>($name)
            .into_diagnostic()
            .context(format!("getting {}", $name))?
    };
}

macro_rules! from_row_bigint {
    ($row:ident, $name:literal) => {
        from_row!($row, Option<String>, $name)
            .map(|x| x.parse().unwrap())
            .unwrap_or_default()
    };
}

#[derive(Debug, Clone, PartialEq, Eq, clap::ValueEnum)]
#[non_exhaustive]
#[allow(clippy::enum_variant_names)]
pub enum KnownNetwork {
    CardanoMainnet,
    CardanoPreProd,
    CardanoPreview,
}

impl KnownNetwork {
    pub fn load_included_genesis(&self) -> Genesis {
        match self {
            KnownNetwork::CardanoMainnet => include::mainnet::load(),
            KnownNetwork::CardanoPreProd => include::preprod::load(),
            KnownNetwork::CardanoPreview => include::preview::load(),
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct Args {
    /// Build snapshot up until selected epoch, included.
    #[arg(long)]
    epoch: u64,

    /// Where to write snapshot
    #[arg(long)]
    path: Option<PathBuf>,

    /// Cache size
    #[arg(long)]
    cache_size: Option<usize>,

    /// Connection string to dbsync
    #[arg(long)]
    conn: String,

    /// Add a limit to queries for debbuging
    #[arg(long)]
    limit: Option<usize>,

    /// Network to build snapshot to, needed for genesis information.
    #[arg(long)]
    network: KnownNetwork,
}

#[tokio::main]
pub async fn run(args: &Args) -> miette::Result<()> {
    let schema = build_schema();
    let state = dolos_redb3::StateStore::open(
        schema,
        args.path.as_ref().unwrap_or(&PathBuf::from("state")),
        args.cache_size,
    )
    .into_diagnostic()
    .context("opening state store")?;

    let pg_mgr = bb8_postgres::PostgresConnectionManager::new(
        tokio_postgres::config::Config::from_str(&args.conn)
            .into_diagnostic()
            .context("failed to parse connection")?,
        tokio_postgres::NoTls,
    );

    let pool = bb8::Pool::builder()
        .max_size(5)
        .build(pg_mgr)
        .await
        .into_diagnostic()
        .context("failed to build pool")?;

    let registry = init_registry()?;

    //handle_account_state(args, &pool, &state, &registry).await?;
    //handle_asset_state(args, &pool, &state, &registry).await?;
    //handle_cursor(args, &pool, &state, &registry).await?;
    handle_era_summaries(args, &pool, &state, &registry).await?;
    //handle_pool_state(args, &pool, &state, &registry).await?;

    Ok(())
}

pub async fn handle_account_state(
    args: &Args,
    pool: &Pool<PostgresConnectionManager<NoTls>>,
    state: &dolos_redb3::StateStore,
    registry: &Handlebars<'static>,
) -> miette::Result<()> {
    let query = registry
        .render(
            "accounts",
            &serde_json::json!({ "epoch": args.epoch, "limit": match args.limit {
            Some(limit) => format!("LIMIT {limit}"),
            None => "".to_string()
        } }),
        )
        .into_diagnostic()
        .context("rendering query")?;

    let conn = pool
        .get()
        .await
        .into_diagnostic()
        .context("getting connection from pool")?;

    let ns = AccountState::NS;
    let mut batch = HashMap::new();

    tracing::info!("Querying accounts...");
    for (i, row) in conn
        .query(&query, &[])
        .await
        .into_diagnostic()
        .context("executing query")?
        .iter()
        .enumerate()
    {
        if i % 100 == 1 {
            tracing::info!(i = i, "Processing accounts...");
        }
        let key = account_key(
            row.try_get("key")
                .into_diagnostic()
                .context("getting from row")?,
        )?;

        let account = AccountState {
            registered_at: from_row!(row, Option<i64>, "registered_at")
                .map(|x| x.try_into().unwrap()),
            controlled_amount: from_row_bigint!(row, "controlled_amount"),
            rewards_sum: from_row_bigint!(row, "rewards_sum"),
            withdrawals_sum: from_row_bigint!(row, "withdrawals_sum"),
            reserves_sum: from_row_bigint!(row, "reserves_sum"),
            treasury_sum: from_row_bigint!(row, "treasury_sum"),
            withdrawable_amount: from_row_bigint!(row, "withdrawable_amount"),
            pool_id: from_row!(row, Option<String>, "pool_id")
                .map(|x| bech32::decode(&x).unwrap().1),
            active_slots: from_row!(row, Option<Json<serde_json::Value>>, "active_slots")
                .map(|x| {
                    x.0.as_array()
                        .unwrap_or(&vec![])
                        .iter()
                        .filter_map(|x| x.as_u64())
                        .collect()
                })
                .unwrap_or_default(),
            seen_addresses: from_row!(row, Option<Json<serde_json::Value>>, "seen_addresses")
                .map(|x| {
                    x.0.as_array()
                        .unwrap_or(&vec![])
                        .iter()
                        .map(|x| {
                            Address::from_bech32(x.as_str().unwrap())
                                .map(|x| x.to_vec())
                                .into_diagnostic()
                                .context("parsing seen addresses")
                        })
                        .collect::<miette::Result<HashSet<Vec<u8>>>>()
                })
                .transpose()?
                .unwrap_or_default(),
            rewards: from_row!(row, Option<Json<serde_json::Value>>, "reward_log")
                .map(|x| {
                    x.0.as_array()
                        .unwrap_or(&vec![])
                        .iter()
                        .filter_map(|x| {
                            x.as_object().map(|data| RewardLog {
                                epoch: data
                                    .get("epoch")
                                    .map(|epoch| epoch.as_u64().unwrap() as u32)
                                    .unwrap(),
                                amount: data
                                    .get("amount")
                                    .map(|amount| amount.as_str().unwrap().parse().unwrap())
                                    .unwrap(),
                                pool_id: data
                                    .get("pool_id")
                                    .map(|pool| bech32::decode(pool.as_str().unwrap()).unwrap().1)
                                    .unwrap(),
                                as_leader: data
                                    .get("as_leader")
                                    .map(|as_leader| as_leader.as_bool().unwrap())
                                    .unwrap(),
                            })
                        })
                        .collect()
                })
                .unwrap_or_default(),
            ..Default::default()
        };

        batch.insert(
            EntityKey::from(key),
            minicbor::to_vec(account)
                .into_diagnostic()
                .context("encoding entity")?,
        );
    }
    tracing::info!("Finished processing accounts.");

    tracing::info!("Writing accounts...");
    state
        .write_entity_batch(ns, batch)
        .into_diagnostic()
        .context("writing entity")?;
    tracing::info!("Finished writing accounts.");

    Ok(())
}

pub async fn handle_asset_state(
    args: &Args,
    pool: &Pool<PostgresConnectionManager<NoTls>>,
    state: &dolos_redb3::StateStore,
    registry: &Handlebars<'static>,
) -> miette::Result<()> {
    let query = registry
        .render(
            "assets",
            &serde_json::json!({ "epoch": args.epoch, "limit": match args.limit {
            Some(limit) => format!("LIMIT {limit}"),
            None => "".to_string()
        } }),
        )
        .into_diagnostic()
        .context("rendering query")?;

    let conn = pool
        .get()
        .await
        .into_diagnostic()
        .context("getting connection from pool")?;

    let ns = AccountState::NS;
    let mut batch = HashMap::new();

    tracing::info!("Querying assets...");
    for (i, row) in conn
        .query(&query, &[])
        .await
        .into_diagnostic()
        .context("executing query")?
        .iter()
        .enumerate()
    {
        if i % 100 == 1 {
            tracing::info!(i = i, "Processing assets...");
        }
        let key = hex::decode(from_row!(row, &str, "key"))
            .into_diagnostic()
            .context("decoding asset key")?;

        let asset = AssetState {
            quantity_bytes: from_row!(row, String, "quantity")
                .parse::<u128>()
                .into_diagnostic()
                .context("parsing asset quantity")?
                .to_be_bytes(),
            initial_tx: from_row!(row, Option<String>, "initial_tx")
                .map(|x| hex::decode(&x).unwrap().as_slice().into()),
            initial_slot: from_row!(row, Option<i64>, "initial_slot").map(|x| x as u64),
            mint_tx_count: from_row!(row, i64, "mint_tx_count") as u64,
        };

        batch.insert(
            EntityKey::from(key),
            minicbor::to_vec(asset)
                .into_diagnostic()
                .context("encoding entity")?,
        );
    }

    tracing::info!("Finished processing assets.");

    tracing::info!("Writing assets...");
    state
        .write_entity_batch(ns, batch)
        .into_diagnostic()
        .context("writing entity")?;
    tracing::info!("Finished writing assets.");

    Ok(())
}

pub async fn handle_cursor(
    args: &Args,
    pool: &Pool<PostgresConnectionManager<NoTls>>,
    state: &dolos_redb3::StateStore,
    registry: &Handlebars<'static>,
) -> miette::Result<()> {
    let query = registry
        .render("cursor", &&serde_json::json!({ "epoch": args.epoch }))
        .into_diagnostic()
        .context("rendering query")?;

    let conn = pool
        .get()
        .await
        .into_diagnostic()
        .context("getting connection from pool")?;

    tracing::info!("Querying cursor...");
    let row = conn
        .query_one(&query, &[])
        .await
        .into_diagnostic()
        .context("executing query")?;
    tracing::info!("Finished querying cursor.");

    let slot = from_row!(row, i64, "slot") as u64;

    state
        .set_cursor(slot)
        .into_diagnostic()
        .context("writing cursor")?;
    tracing::info!("Finished setting cursor.");

    Ok(())
}

pub async fn handle_era_summaries(
    args: &Args,
    pool: &Pool<PostgresConnectionManager<NoTls>>,
    state: &dolos_redb3::StateStore,
    registry: &Handlebars<'static>,
) -> miette::Result<()> {
    let query = registry
        .render(
            "era_summaries",
            &&serde_json::json!({ "epoch": args.epoch }),
        )
        .into_diagnostic()
        .context("rendering query")?;

    let conn = pool
        .get()
        .await
        .into_diagnostic()
        .context("getting connection from pool")?;

    tracing::info!("Querying era summaries...");
    let rows = conn
        .query(&query, &[])
        .await
        .into_diagnostic()
        .context("executing query")?;
    tracing::info!("Finished querying era summaries.");

    let genesis = args.network.load_included_genesis();

    let mut it = rows.iter().zip(rows.iter().skip(1)).peekable();
    while let Some((prev, next)) = it.next() {
        let epoch = from_row!(prev, i32, "epoch") as u64;
        let slot = from_row!(prev, i64, "slot") as u64;
        let start = from_row!(prev, NaiveDateTime, "start_time");
        let end = from_row!(prev, NaiveDateTime, "end_time");
        let epoch_end = from_row!(next, i32, "epoch") as u64;
        let slot_end = from_row!(next, i64, "slot") as u64;

        let key = (epoch as u16).to_be_bytes().as_slice().into();

        let era = EraSummary {
            start: EraBoundary {
                epoch,
                slot,
                timestamp: start.and_utc().timestamp() as u64,
            },
            end: Some(EraBoundary {
                epoch: epoch_end,
                slot: slot_end,
                timestamp: end.and_utc().timestamp() as u64,
            }),
            // TODO: This will break in preprod and mainnet, but for now it will do
            epoch_length: genesis.shelley.epoch_length.unwrap() as u64,
            slot_length: genesis.shelley.slot_length.unwrap() as u64,
        };

        state
            .write_entity_typed(&key, &era)
            .into_diagnostic()
            .context("writing era")?;

        if it.peek().is_none() {
            let key = (epoch_end as u16).to_be_bytes().as_slice().into();
            let curr = EraSummary {
                start: EraBoundary {
                    epoch: epoch_end,
                    slot: slot_end,
                    timestamp: end.and_utc().timestamp() as u64,
                },
                end: None,
                epoch_length: genesis.shelley.epoch_length.unwrap() as u64,
                slot_length: genesis.shelley.slot_length.unwrap() as u64,
            };

            state
                .write_entity_typed(&key, &curr)
                .into_diagnostic()
                .context("writing era")?;
        }
    }

    tracing::info!("Finished setting cursor.");

    Ok(())
}

pub async fn handle_pool_state(
    args: &Args,
    pool: &Pool<PostgresConnectionManager<NoTls>>,
    state: &dolos_redb3::StateStore,
    registry: &Handlebars<'static>,
) -> miette::Result<()> {
    let query = registry
        .render(
            "pools",
            &&serde_json::json!({ "epoch": args.epoch, "limit": match args.limit {
            Some(limit) => format!("LIMIT {limit}"),
            None => "".to_string()
        } }),
        )
        .into_diagnostic()
        .context("rendering query")?;

    let conn = pool
        .get()
        .await
        .into_diagnostic()
        .context("getting connection from pool")?;

    tracing::info!("Querying pools...");
    let rows = conn
        .query(&query, &[])
        .await
        .into_diagnostic()
        .context("executing query")?;
    tracing::info!("Finished querying pools.");

    let ns = AccountState::NS;
    let mut batch = HashMap::new();

    for (i, row) in rows.iter().enumerate() {
        if i % 100 == 1 {
            tracing::info!(i = i, "Processing pools...");
        }
        let key = hex::decode(
            row.try_get::<_, &str>("key")
                .into_diagnostic()
                .context("getting from row")?,
        )
        .into_diagnostic()
        .context("decoding pool vrf_keyhash")?;

        let pool = PoolState {
            vrf_keyhash: key.as_slice().into(),
            reward_account: Address::from_bech32(from_row!(row, &str, "reward_account"))
                .map(|x| x.to_vec())
                .into_diagnostic()
                .context("parsing reward_account")?,
            declared_pledge: from_row_bigint!(row, "declared_pledge"),
            margin_cost: RationalNumber {
                numerator: (1_f64 / from_row!(row, f64, "margin_cost")).round() as u64,
                denominator: 1,
            },
            fixed_cost: from_row_bigint!(row, "fixed_cost"),
            active_stake: from_row_bigint!(row, "active_stake"),
            live_stake: from_row_bigint!(row, "live_stake"),
            blocks_minted: from_row!(row, i64, "blocks_minted") as u32,
            live_saturation: from_row!(row, f64, "live_saturation"),
            pool_owners: from_row!(row, Option<Json<serde_json::Value>>, "owners")
                .map(|x| {
                    x.0.as_array()
                        .unwrap_or(&vec![])
                        .iter()
                        .map(|x| match Address::from_bech32(x.as_str().unwrap()) {
                            Ok(address) => {
                                if let Address::Stake(stake) = address {
                                    Ok(*stake.payload().as_hash())
                                } else {
                                    bail!("address is not a stake address")
                                }
                            }
                            Err(err) => Err(err).into_diagnostic().context("parsing address"),
                        })
                        .collect::<miette::Result<_>>()
                })
                .transpose()?
                .unwrap_or_default(),
            relays: from_row!(row, Option<Json<serde_json::Value>>, "relays")
                .map(|x| {
                    x.0.as_array()
                        .unwrap_or(&vec![])
                        .iter()
                        .map(|x| {
                            let data = x.as_object().unwrap();
                            if let Some(Value::String(dnssrv)) = data.get("dns_srv_name") {
                                Relay::MultiHostName(dnssrv.to_string())
                            } else if let Some(Value::String(dns)) = data.get("dns") {
                                Relay::SingleHostName(
                                    if let Some(Value::Number(a)) = data.get("port") {
                                        Some(a.as_u64().unwrap() as u32)
                                    } else {
                                        None
                                    },
                                    dns.to_string(),
                                )
                            } else {
                                let port = match data.get("port") {
                                    Some(Value::Number(x)) => Some(x.as_u64().unwrap() as u32),
                                    _ => None,
                                };
                                let ipv4 = match data.get("ipv4") {
                                    Some(Value::String(x)) => Some(
                                        Ipv4Addr::from_str(x.as_str())
                                            .unwrap()
                                            .octets()
                                            .to_vec()
                                            .into(),
                                    ),
                                    _ => None,
                                };
                                let ipv6 = match data.get("ipv6") {
                                    Some(Value::String(x)) => Some(
                                        Ipv6Addr::from_str(x.as_str())
                                            .unwrap()
                                            .octets()
                                            .to_vec()
                                            .into(),
                                    ),
                                    _ => None,
                                };

                                Relay::SingleHostAddr(port, ipv4, ipv6)
                            }
                        })
                        .collect()
                })
                .unwrap_or_default(),
            metadata: match from_row!(row, Option<Json<serde_json::Value>>, "relays") {
                Some(json) => {
                    if let Value::Object(x) = json.0 {
                        Some(PoolMetadata {
                            url: x.get("url").unwrap().as_str().unwrap().to_string(),
                            hash: hex::decode(x.get("hash").unwrap().as_str().unwrap())
                                .unwrap()
                                .into(),
                        })
                    } else {
                        None
                    }
                }
                None => None,
            },
        };

        batch.insert(
            EntityKey::from(key),
            minicbor::to_vec(pool)
                .into_diagnostic()
                .context("encoding entity")?,
        );
    }

    tracing::info!("Finished processing pools.");

    tracing::info!("Writing pools...");
    state
        .write_entity_batch(ns, batch)
        .into_diagnostic()
        .context("writing entity")?;
    tracing::info!("Finished writing pools.");

    Ok(())
}
