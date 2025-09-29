use std::{
    net::{Ipv4Addr, Ipv6Addr},
    path::PathBuf,
    str::FromStr,
};

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use chrono::NaiveDateTime;
use dolos_cardano::{
    build_schema, include, AccountState, AssetState, DRepState, EpochState, EraBoundary,
    EraSummary, PParamValue, PParamsSet, PoolState, EPOCH_KEY_GO, EPOCH_KEY_MARK, EPOCH_KEY_SET,
};
use dolos_core::{EntityKey, Genesis, StateStore as _, StateWriter};
use handlebars::Handlebars;
use miette::{bail, Context, IntoDiagnostic};
use pallas::ledger::{
    addresses::Address,
    primitives::{conway::DRep, ExUnitPrices, ExUnits, PoolMetadata, RationalNumber, Relay},
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

macro_rules! from_row_parse {
    ($row:ident, $type:ty, $name:literal) => {{
        let val = from_row!($row, String, $name);
        let val = val.parse().unwrap();
        val
    }};
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

macro_rules! from_row_ratio {
    ($row:ident, $column:literal) => {{
        let val = from_row!($row, f64, $column);
        let val = num_rational::Rational64::approximate_float(val).unwrap();
        let val = pallas::ledger::primitives::RationalNumber {
            numerator: *val.numer() as u64,
            denominator: *val.denom() as u64,
        };

        val
    }};
}

#[derive(Debug, Clone, PartialEq, Eq, clap::ValueEnum)]
#[non_exhaustive]
#[allow(clippy::enum_variant_names)]
pub enum KnownNetwork {
    CardanoMainnet,
    CardanoPreprod,
    CardanoPreview,
}

impl KnownNetwork {
    pub fn load_included_genesis(&self) -> Genesis {
        match self {
            KnownNetwork::CardanoMainnet => include::mainnet::load(),
            KnownNetwork::CardanoPreprod => include::preprod::load(),
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

    #[arg(long)]
    namespace: Option<String>,
}

#[tokio::main]
pub async fn run(args: &Args) -> miette::Result<()> {
    let schema = build_schema();
    let state = dolos_redb3::state::StateStore::open(
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

    if let Some(namespace) = args.namespace.as_ref() {
        match namespace.as_str() {
            "accounts" => handle_account_state(args, &pool, &state, &registry).await?,
            "assets" => handle_asset_state(args, &pool, &state, &registry).await?,
            "dreps" => handle_drep_state(args, &pool, &state, &registry).await?,
            "epochs" => handle_epoch_state(args, &pool, &state, &registry).await?,
            "era-summaries" => handle_era_summaries(args, &pool, &state, &registry).await?,
            "pools" => handle_pool_state(args, &pool, &state, &registry).await?,
            _ => bail!("invalid namespace"),
        }
    } else {
        handle_account_state(args, &pool, &state, &registry).await?;
        handle_asset_state(args, &pool, &state, &registry).await?;
        handle_drep_state(args, &pool, &state, &registry).await?;
        handle_epoch_state(args, &pool, &state, &registry).await?;
        handle_era_summaries(args, &pool, &state, &registry).await?;
        handle_pool_state(args, &pool, &state, &registry).await?;
    }

    Ok(())
}

pub async fn handle_account_state(
    args: &Args,
    pool: &Pool<PostgresConnectionManager<NoTls>>,
    state: &dolos_redb3::state::StateStore,
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

    let writer = state.start_writer().into_diagnostic()?;

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
            active_drep: from_row!(row, Option<String>, "drep_id")
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

            latest_pool: None,
            active_pool: from_row!(row, Option<String>, "pool_id")
                .map(|x| bech32::decode(&x).unwrap().1),
            latest_drep: None,
            deposit: Default::default(),
        };

        writer
            .write_entity_typed::<AccountState>(&EntityKey::from(key), &account)
            .into_diagnostic()?;
    }
    tracing::info!("Finished processing accounts.");

    tracing::info!("Writing accounts...");
    writer
        .commit()
        .into_diagnostic()
        .context("writing entity")?;
    tracing::info!("Finished writing accounts.");

    Ok(())
}

pub async fn handle_asset_state(
    args: &Args,
    pool: &Pool<PostgresConnectionManager<NoTls>>,
    state: &dolos_redb3::state::StateStore,
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

    let writer = state.start_writer().into_diagnostic()?;

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

        writer
            .write_entity_typed::<AssetState>(&EntityKey::from(key), &asset)
            .into_diagnostic()?;
    }

    tracing::info!("Finished processing assets.");

    tracing::info!("committing assets...");
    writer
        .commit()
        .into_diagnostic()
        .context("writing entity")?;
    tracing::info!("Finished writing assets.");

    Ok(())
}

pub async fn handle_era_summaries(
    args: &Args,
    pool: &Pool<PostgresConnectionManager<NoTls>>,
    state: &dolos_redb3::state::StateStore,
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
    let writer = state.start_writer().into_diagnostic()?;

    let mut it = rows.iter().zip(rows.iter().skip(1)).peekable();
    while let Some((prev, next)) = it.next() {
        let epoch = from_row!(prev, i32, "epoch") as u64;
        let slot = from_row!(prev, i64, "slot") as u64;
        let start = from_row!(prev, NaiveDateTime, "start_time");
        let end = from_row!(prev, NaiveDateTime, "end_time");
        let (epoch_length, slot_length) = if from_row!(prev, i32, "protocol_major") < 6 {
            (4320, 20)
        } else {
            (
                genesis.shelley.epoch_length.unwrap() as u64,
                genesis.shelley.slot_length.unwrap() as u64,
            )
        };
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

            epoch_length,
            slot_length,
        };

        dbg!(&era);

        writer
            .write_entity_typed(&key, &era)
            .into_diagnostic()
            .context("writing era")?;

        if it.peek().is_none() {
            let key = (epoch_end as u16).to_be_bytes().as_slice().into();
            let (epoch_length, slot_length) = if from_row!(prev, i32, "protocol_major") < 2 {
                (21600, 20)
            } else {
                (
                    genesis.shelley.epoch_length.unwrap() as u64,
                    genesis.shelley.slot_length.unwrap() as u64,
                )
            };

            let curr = EraSummary {
                start: EraBoundary {
                    epoch: epoch_end,
                    slot: slot_end,
                    timestamp: end.and_utc().timestamp() as u64,
                },
                end: None,
                epoch_length,
                slot_length,
            };

            writer
                .write_entity_typed(&key, &curr)
                .into_diagnostic()
                .context("writing era")?;
        }
    }

    tracing::info!("Writing era summaries...");
    writer
        .commit()
        .into_diagnostic()
        .context("writing entity")?;
    tracing::info!("Finished writing era summaries.");

    Ok(())
}

pub async fn handle_pool_state(
    args: &Args,
    pool: &Pool<PostgresConnectionManager<NoTls>>,
    state: &dolos_redb3::state::StateStore,
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

    let writer = state.start_writer().into_diagnostic()?;

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
            __live_stake: from_row_bigint!(row, "live_stake"),
            blocks_minted_total: from_row!(row, i64, "blocks_minted") as u32,
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
            register_slot: Default::default(), // TODO: add register_slot
            retiring_epoch: Default::default(), // TODO: add retiring_epoch
            is_retired: Default::default(),
            blocks_minted_epoch: Default::default(),
            deposit: Default::default(),
        };

        writer
            .write_entity_typed::<PoolState>(&EntityKey::from(key), &pool)
            .into_diagnostic()?;
    }

    tracing::info!("Finished processing pools.");

    tracing::info!("Writing pools...");
    writer
        .commit()
        .into_diagnostic()
        .context("writing entity")?;
    tracing::info!("Finished writing pools.");

    Ok(())
}

macro_rules! pp_col {
    ($pparams:ident, $variant:ident, $row:ident, $ty:ty, $column:literal) => {
        let val = from_row!($row, $ty, $column);
        let val = TryFrom::try_from(val).unwrap();
        $pparams.set(PParamValue::$variant(val))
    };
    ($pparams:ident, $variant:ident, $row:ident, parse, $column:literal) => {
        let val = from_row!($row, String, $column);
        let val = val.parse().unwrap();
        $pparams.set(PParamValue::$variant(val))
    };
    ($pparams:ident, $variant:ident, $row:ident, $column:literal) => {
        let val = from_row!($row, i32, $column);
        let val = TryFrom::try_from(val).unwrap();
        $pparams.set(PParamValue::$variant(val))
    };
}

macro_rules! pp_col_parse {
    ($pparams:ident, $variant:ident, $row:ident, $column:literal) => {
        let val = from_row!($row, String, $column);
        let val = val.parse().unwrap();
        $pparams.set(PParamValue::$variant(val))
    };
}

macro_rules! pp_col_ratio {
    ($pparams:ident, $variant:ident, $row:ident, $column:literal) => {
        let val = from_row_ratio!($row, $column);
        $pparams.set(PParamValue::$variant(val))
    };
}

pub async fn handle_epoch_state(
    args: &Args,
    pool: &Pool<PostgresConnectionManager<NoTls>>,
    state: &dolos_redb3::state::StateStore,
    registry: &Handlebars<'static>,
) -> miette::Result<()> {
    let query = registry
        .render(
            "epochs",
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

    tracing::info!("Querying epochs...");
    let rows = conn
        .query(&query, &[])
        .await
        .into_diagnostic()
        .context("executing query")?;
    tracing::info!("Finished querying epochs.");

    let writer = state.start_writer().into_diagnostic()?;

    let key_for_idx = |idx: u32| match idx {
        0 => EntityKey::from(EPOCH_KEY_MARK),
        1 => EntityKey::from(EPOCH_KEY_SET),
        2 => EntityKey::from(EPOCH_KEY_GO),
        _ => unreachable!(),
    };

    for (i, row) in rows.iter().enumerate() {
        if i % 100 == 1 {
            tracing::info!(i = i, "Processing epochs...");
        }

        let protocol_major = from_row!(row, i32, "protocol_major_ver");

        let mut pp = PParamsSet::new(protocol_major as u16);

        pp_col!(pp, MinFeeA, row, "min_fee_a");
        pp_col!(pp, MinFeeB, row, "min_fee_b");
        pp_col!(pp, MaxBlockBodySize, row, "max_block_size");
        pp_col!(pp, MaxTransactionSize, row, "max_tx_size");
        pp_col!(pp, MaxBlockHeaderSize, row, "max_block_header_size");
        pp_col_parse!(pp, KeyDeposit, row, "key_deposit");
        pp_col_parse!(pp, PoolDeposit, row, "pool_deposit");
        pp_col!(pp, DesiredNumberOfStakePools, row, i32, "e_max");
        //pp_col!(pp, OptimalPoolCount, "n_opt");
        // pp_col!(pp, ProtocolVersion, row, "protocol_minor_ver");

        let protocol_minor = from_row!(row, i32, "protocol_minor_ver");

        pp.set(PParamValue::ProtocolVersion((
            protocol_major as u64,
            protocol_minor as u64,
        )));

        pp_col_ratio!(pp, PoolPledgeInfluence, row, "a0");
        pp_col_ratio!(pp, ExpansionRate, row, "rho");
        pp_col_ratio!(pp, TreasuryGrowthRate, row, "tau");
        pp_col_ratio!(pp, DecentralizationConstant, row, "decentralisation_param");
        pp_col_ratio!(
            pp,
            MinFeeRefScriptCostPerByte,
            row,
            "min_fee_ref_script_cost_per_byte"
        );
        pp_col_parse!(pp, MinUtxoValue, row, "min_utxo");
        pp_col_parse!(pp, MinPoolCost, row, "min_pool_cost");

        //pp_col!(pp, Nonce, "nonce");
        //pp_col!(pp, ExtraEntropy, row, "extra_entropy");

        // TODO: parse cost models
        // pp_col!(pp, CostModelsForScriptLanguages, row, "cost_models");

        pp.set(PParamValue::ExecutionCosts(ExUnitPrices {
            mem_price: from_row_ratio!(row, "price_mem"),
            step_price: from_row_ratio!(row, "price_step"),
        }));

        pp.set(PParamValue::MaxTxExUnits(ExUnits {
            mem: from_row_parse!(row, i32, "max_tx_ex_mem"),
            steps: from_row_parse!(row, i32, "max_tx_ex_steps"),
        }));

        pp.set(PParamValue::MaxBlockExUnits(ExUnits {
            mem: from_row_parse!(row, i32, "max_block_ex_mem"),
            steps: from_row_parse!(row, i32, "max_block_ex_steps"),
        }));

        pp_col_parse!(pp, MaxValueSize, row, "max_val_size");
        pp_col!(pp, CollateralPercentage, row, i32, "collateral_percent");
        pp_col!(pp, MaxCollateralInputs, row, i32, "max_collateral_inputs");

        pp_col_parse!(pp, AdaPerUtxoByte, row, "coins_per_utxo_word");
        //pp_col!(pp, PvtMotionNoConfidence, "pvt_motion_no_confidence");
        //pp_col!(pp, PvtCommitteeNormal, "pvt_committee_normal");
        //pp_col!(pp, PvtCommitteeNoConfidence, "pvt_committee_no_confidence");
        //pp_col!(pp, PvtHardForkInitiation, "pvt_hard_fork_initiation");
        //pp_col!(pp, DvtMotionNoConfidence, "dvt_motion_no_confidence");
        //pp_col!(pp, DvtCommitteeNormal, "dvt_committee_normal");
        //pp_col!(pp, DvtCommitteeNoConfidence, "dvt_committee_no_confidence");
        //pp_col!(pp, DvtUpdateToConstitution, "dvt_update_to_constitution");
        //pp_col!(pp, DvtHardForkInitiation, "dvt_hard_fork_initiation");
        //pp_col!(pp, DvtPpNetworkGroup, "dvt_p_p_network_group");
        //pp_col!(pp, DvtPpEconomicGroup, "dvt_p_p_economic_group");
        //pp_col!(pp, DvtPpTechnicalGroup, "dvt_p_p_technical_group");
        //pp_col!(pp, DvtPpGovGroup, "dvt_p_p_gov_group");
        //pp_col!(pp, DvtTreasuryWithdrawal, "dvt_treasury_withdrawal");
        //pp_col!(pp, CommitteeMinSize, "committee_min_size");
        //pp_col!(pp, CommitteeMaxTermLength, "committee_max_term_length");
        //pp_col!(pp, GovActionLifetime, "gov_action_lifetime");
        //pp_col!(pp, GovActionDeposit, "gov_action_deposit");
        pp_col!(pp, DrepDeposit, row, i32, "drep_deposit");
        //pp_col!(pp, DrepActivity, "drep_activity");
        //pp_col!(pp, PvtPpSecurityGroup, "pvtpp_security_group");
        //pp_col!(pp, PvtPpSecurityGroup, "pvt_p_p_security_group");

        let deposits_stake: u64 = from_row_bigint!(row, "deposits_stake");
        let deposits_drep: u64 = from_row_bigint!(row, "deposits_drep");
        let deposits_proposal: u64 = from_row_bigint!(row, "deposits_proposal");

        let epoch_state = EpochState {
            pparams: pp,
            // for some reason dbsync 1-index numeration, so we subtract 1
            number: from_row!(row, i32, "epoch_no") as u32 - 1,
            active_stake: 0, // from_row_bigint!(row, "active_stake"),
            deposits: deposits_stake + deposits_drep + deposits_proposal,
            reserves: from_row_bigint!(row, "reserves"),
            treasury: from_row_bigint!(row, "treasury"),
            utxos: from_row_bigint!(row, "utxo"),
            gathered_fees: from_row_bigint!(row, "fees"),
            gathered_deposits: 0, // from_row_bigint!(row, "gathered_deposits"),
            decayed_deposits: 0,  // from_row_bigint!(row, "decayed_deposits"),
            rewards_to_distribute: None,
            rewards_to_treasury: None,
            largest_stable_slot: Default::default(), // todo!(),
            nonces: Default::default(),
            blocks_minted: Default::default(),
        };

        writer
            .write_entity_typed::<EpochState>(&key_for_idx(i as u32), &epoch_state)
            .into_diagnostic()?;
    }

    tracing::info!("Finished processing epochs.");

    tracing::info!("Writing epochs...");
    writer
        .commit()
        .into_diagnostic()
        .context("writing entity")?;
    tracing::info!("Finished writing epochs.");

    Ok(())
}

pub async fn handle_drep_state(
    args: &Args,
    pool: &Pool<PostgresConnectionManager<NoTls>>,
    state: &dolos_redb3::state::StateStore,
    registry: &Handlebars<'static>,
) -> miette::Result<()> {
    let query = registry
        .render(
            "dreps",
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

    let writer = state.start_writer().into_diagnostic()?;

    tracing::info!("Querying dreps...");
    for (i, row) in conn
        .query(&query, &[])
        .await
        .into_diagnostic()
        .context("executing query")?
        .iter()
        .enumerate()
    {
        if i % 1000 == 1 {
            tracing::info!(i = i, "Processing dreps...");
        }

        let drep_id = match from_row!(row, &str, "drep_id") {
            "drep_always_abstain" => vec![0],
            "drep_always_no_confidence" => vec![1],
            drep_id => {
                bech32::decode(drep_id)
                    .into_diagnostic()
                    .context("decoding drep")?
                    .1
            }
        };

        let initial_slot = from_row!(row, Option<i64>, "initial_slot").map(|x| x as u64);
        let voting_power = from_row!(row, String, "voting_power")
            .parse::<u64>()
            .into_diagnostic()
            .context("parsing drep voting power")?;
        let last_active_slot = from_row!(row, Option<i64>, "last_active_slot").map(|x| x as u64);
        let retired = from_row!(row, bool, "retired");

        let drep = DRepState {
            initial_slot,
            voting_power,
            last_active_slot,
            retired,
            __drep_id: Default::default(),
            expired: Default::default(),
            deposit: Default::default(),
        };

        writer
            .write_entity_typed::<DRepState>(&EntityKey::from(drep_id), &drep)
            .into_diagnostic()?;
    }

    tracing::info!("Finished processing dreps.");

    tracing::info!("committing dreps...");
    writer
        .commit()
        .into_diagnostic()
        .context("writing entity")?;
    tracing::info!("Finished writing dreps.");

    Ok(())
}
