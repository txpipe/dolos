use std::{
    collections::HashMap,
    net::{Ipv4Addr, Ipv6Addr},
    path::PathBuf,
    str::FromStr,
};

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use dolos_cardano::{build_schema, include, AccountState, AssetState, FixedNamespace, PoolState};
use dolos_core::{EntityKey, Genesis, StateStore as _};
use handlebars::Handlebars;
use miette::{bail, Context, IntoDiagnostic};
use pallas::{
    codec::minicbor,
    ledger::{
        addresses::Address,
        primitives::{conway::DRep, PoolMetadata, RationalNumber, Relay},
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
        match from_row!($row, Option<String>, $name) {
            Some(x) => x.parse().ok(),
            None => None,
        }
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

    handle_account_state(args, &pool, &state, &registry).await?;
    handle_asset_state(args, &pool, &state, &registry).await?;
    handle_pool_state(args, &pool, &state, &registry).await?;

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
        if i % 1000 == 1 {
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
            active_stake: from_row_bigint!(row, "active_stake"),
            wait_stake: from_row_bigint!(row, "wait_stake"),
            rewards_sum: from_row_bigint!(row, "rewards_sum"),
            withdrawals_sum: from_row_bigint!(row, "withdrawals_sum"),
            reserves_sum: from_row_bigint!(row, "reserves_sum"),
            treasury_sum: from_row_bigint!(row, "treasury_sum"),
            drep: from_row!(row, Option<String>, "drep_id")
                .map(|drep_id| -> miette::Result<DRep> {
                    match drep_id.as_str() {
                        "drep_always_abstain" => Ok(DRep::Abstain),
                        "drep_always_no_confidence" => Ok(DRep::NoConfidence),
                        x => {
                            let bytes = bech32::decode(x)
                                .into_diagnostic()
                                .context("decoding drep")?
                                .1;
                            if from_row!(row, bool, "drep_id_has_script") {
                                Ok(DRep::Script(bytes.as_slice().into()))
                            } else {
                                Ok(DRep::Key(bytes.as_slice().into()))
                            }
                        }
                    }
                })
                .transpose()?,
            pool_id: from_row!(row, Option<String>, "pool_id")
                .map(|x| bech32::decode(&x).unwrap().1),
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
        if i % 1000 == 1 {
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
            wait_stake: from_row_bigint!(row, "wait_stake"),
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
