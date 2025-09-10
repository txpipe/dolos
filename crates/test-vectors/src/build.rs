use std::{collections::HashSet, path::PathBuf, str::FromStr};

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use dolos_cardano::{build_schema, AccountState, RewardLog};
use dolos_core::State3Store as _;
use handlebars::Handlebars;
use miette::{Context, IntoDiagnostic};
use pallas::ledger::addresses::Address;
use tokio_postgres::types::Json;
use tokio_postgres::NoTls;

use crate::{queries::init_registry, utils::account_key};

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

    // Handle accounts
    handle_account_state(args.epoch, &pool, &state, &registry).await?;

    Ok(())
}

pub async fn handle_account_state(
    epoch: u64,
    pool: &Pool<PostgresConnectionManager<NoTls>>,
    state: &dolos_redb3::StateStore,
    registry: &Handlebars<'static>,
) -> miette::Result<()> {
    let query = registry
        .render("accounts", &serde_json::json!({ "epoch": epoch }))
        .into_diagnostic()
        .context("rendering query")?;

    let conn = pool
        .get()
        .await
        .into_diagnostic()
        .context("getting connection from pool")?;

    for row in conn
        .query(&query, &[])
        .await
        .into_diagnostic()
        .context("executing query")?
    {
        let key = account_key(
            row.try_get("key")
                .into_diagnostic()
                .context("getting from row")?,
        )?;

        let registered_at = row
            .try_get::<_, Option<i64>>("registered_at")
            .into_diagnostic()
            .context("getting registered_at")?;
        let account = AccountState {
            registered_at: registered_at.map(|x| x.try_into().unwrap()),
            controlled_amount: row
                .try_get::<_, Option<String>>("controlled_amount")
                .into_diagnostic()
                .context("getting controlled_amount")?
                .map(|x| x.parse().unwrap())
                .unwrap_or_default(),
            rewards_sum: row
                .try_get::<_, Option<String>>("rewards_sum")
                .into_diagnostic()
                .context("getting rewards_sum")?
                .map(|x| x.parse().unwrap())
                .unwrap_or_default(),
            withdrawals_sum: row
                .try_get::<_, Option<String>>("withdrawals_sum")
                .into_diagnostic()
                .context("getting withdrawals_sum")?
                .map(|x| x.parse().unwrap())
                .unwrap_or_default(),
            reserves_sum: row
                .try_get::<_, Option<String>>("reserves_sum")
                .into_diagnostic()
                .context("getting reserves_sum")?
                .map(|x| x.parse().unwrap())
                .unwrap_or_default(),
            treasury_sum: row
                .try_get::<_, Option<String>>("treasury_sum")
                .into_diagnostic()
                .context("getting treasury_sum")?
                .map(|x| x.parse().unwrap())
                .unwrap_or_default(),
            withdrawable_amount: row
                .try_get::<_, Option<String>>("withdrawable_amount")
                .into_diagnostic()
                .context("getting withdrawable_amount")?
                .map(|x| x.parse().unwrap())
                .unwrap_or_default(),
            pool_id: row
                .try_get::<_, Option<String>>("pool_id")
                .into_diagnostic()
                .context("getting pool_id")?
                .map(|x| bech32::decode(&x).unwrap().1),
            active_slots: row
                .try_get::<_, Option<Json<serde_json::Value>>>("active_slots")
                .into_diagnostic()
                .context("getting active_slots")?
                .map(|x| {
                    x.0.as_array()
                        .unwrap_or(&vec![])
                        .iter()
                        .filter_map(|x| x.as_u64())
                        .collect()
                })
                .unwrap_or_default(),
            seen_addresses: row
                .try_get::<_, Option<Json<serde_json::Value>>>("seen_addresses")
                .into_diagnostic()
                .context("getting seen_addresses")?
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
            rewards: row
                .try_get::<_, Option<Json<serde_json::Value>>>("reward_log")
                .into_diagnostic()
                .context("getting rewards")?
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

        state
            .write_entity_typed(&key.into(), &account)
            .into_diagnostic()
            .context("writing entity")?;
    }

    Ok(())
}
