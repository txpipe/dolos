use std::marker::PhantomData;

use comfy_table::Table;
use dolos_cardano::{model::AccountState, EpochState, EraSummary, PoolState};
use itertools::Itertools;
use miette::{Context, IntoDiagnostic};

use dolos::prelude::*;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// namespace to dump
    #[arg(long)]
    namespace: String,

    /// count of entities to dump
    #[arg(long, default_value = "100")]
    count: usize,
}

trait TableRow: Entity {
    fn header() -> Vec<&'static str>;
    fn row(&self, key: &EntityKey) -> Vec<String>;
}

macro_rules! format_epoch_value {
    ($stake:expr, $pool:expr) => {
        format!(
            "{} ({})",
            $stake.map(|x| x.to_string()).unwrap_or_default(),
            $pool
                .as_ref()
                .and_then(|x| x.map(|y| hex::encode(y)[..3].to_string()))
                .unwrap_or_default()
        )
    };
}

impl TableRow for AccountState {
    fn header() -> Vec<&'static str> {
        vec![
            "cred",
            "reg",
            "dereg",
            "stake (-3)",
            "stake (-2)",
            "stake (-1)",
            "live stake",
            "rewards",
            "withdrawals",
            "epoch version",
        ]
    }

    fn row(&self, key: &EntityKey) -> Vec<String> {
        vec![
            format!("{}", hex::encode(key)),
            format!("{}", self.registered_at.unwrap_or_default()),
            format!("{}", self.deregistered_at.unwrap_or_default()),
            format_epoch_value!(self.total_stake.go(), self.pool.go()),
            format_epoch_value!(self.total_stake.set(), self.pool.set()),
            format_epoch_value!(self.total_stake.mark(), self.pool.mark()),
            format_epoch_value!(Some(self.live_stake()), Some(self.pool.live())),
            format!("{}", self.rewards_sum),
            format!("{}", self.withdrawals_sum),
            format!(
                "{},{},{}",
                self.total_stake.epoch().unwrap_or_default(),
                self.pool.epoch().unwrap_or_default(),
                self.drep.epoch().unwrap_or_default(),
            ),
        ]
    }
}

impl TableRow for EpochState {
    fn header() -> Vec<&'static str> {
        vec![
            "number",
            "version",
            "pot reserves",
            "pot utxos",
            "pot treasury",
            "pot deposits",
            "pot rewards",
            "pot fees",
            "gathered fees",
            "pparams",
            "nonce",
        ]
    }

    fn row(&self, _key: &EntityKey) -> Vec<String> {
        vec![
            format!("{}", self.number),
            format!(
                "{}",
                self.pparams.active().protocol_major().unwrap_or_default()
            ),
            format!("{}", self.initial_pots.reserves),
            format!("{}", self.initial_pots.utxos),
            format!("{}", self.initial_pots.treasury),
            format!("{}", self.initial_pots.obligations()),
            format!("{}", self.initial_pots.rewards),
            format!("{}", self.initial_pots.fees),
            format!("{}", self.rolling.live().gathered_fees),
            format!("{}", self.pparams.live().len()),
            format!(
                "{}",
                self.nonces
                    .as_ref()
                    .map(|x| hex::encode(x.active))
                    .unwrap_or("".to_string())
            ),
        ]
    }
}

impl TableRow for EraSummary {
    fn header() -> Vec<&'static str> {
        vec![
            "key",
            "start epoch",
            "start slot",
            "start timestamp",
            "end epoch",
            "end slot",
            "end timestamp",
            "epoch length",
            "slot length",
        ]
    }

    fn row(&self, key: &EntityKey) -> Vec<String> {
        vec![
            format!("{}", hex::encode(key)),
            format!("{}", self.start.epoch),
            format!("{}", self.start.slot),
            format!("{}", self.start.timestamp),
            format!("{}", self.end.as_ref().map(|x| x.epoch).unwrap_or_default()),
            format!("{}", self.end.as_ref().map(|x| x.slot).unwrap_or_default()),
            format!(
                "{}",
                self.end.as_ref().map(|x| x.timestamp).unwrap_or_default()
            ),
            format!("{}", self.epoch_length),
            format!("{}", self.slot_length),
        ]
    }
}

const POOL_HRP: bech32::Hrp = bech32::Hrp::parse_unchecked("pool");

impl TableRow for PoolState {
    fn header() -> Vec<&'static str> {
        vec![
            "pool hex",
            "pool bech32",
            "registered at",
            "retiring epoch",
            "pending (go)",
            "pending (set)",
            "pending (mark)",
            "pending (live)",
        ]
    }

    fn row(&self, key: &EntityKey) -> Vec<String> {
        let entity_key = key.clone();
        let pool_hash = entity_key.as_ref()[..28].try_into().unwrap();
        let pool_hex = hex::encode(pool_hash);
        let pool_bech32 = bech32::encode::<bech32::Bech32>(POOL_HRP, pool_hash).unwrap();

        vec![
            format!("{}", pool_hex),
            format!("{}", pool_bech32),
            format!("{}", self.register_slot),
            format!(
                "{}",
                self.retiring_epoch
                    .map(|x| x.to_string())
                    .unwrap_or_default()
            ),
            format!(
                "{} ({})",
                self.snapshot.go().map(|x| x.is_pending).unwrap_or_default(),
                self.snapshot.epoch().unwrap_or_default() - 3,
            ),
            format!(
                "{} ({})",
                self.snapshot
                    .set()
                    .map(|x| x.is_pending)
                    .unwrap_or_default(),
                self.snapshot.epoch().unwrap_or_default() - 2
            ),
            format!(
                "{} ({})",
                self.snapshot
                    .mark()
                    .map(|x| x.is_pending)
                    .unwrap_or_default(),
                self.snapshot.epoch().unwrap_or_default() - 1
            ),
            format!(
                "{} ({})",
                self.snapshot.live().is_pending,
                self.snapshot.epoch().unwrap_or_default()
            ),
        ]
    }
}

// impl TableRow for RewardLog {
//     fn header() -> Vec<&'static str> {
//         vec!["key", "epoch", "amount", "pool id", "as leader"]
//     }

//     fn row(&self, key: &EntityKey) -> Vec<String> {
//         vec![
//             format!("{}", hex::encode(key)),
//             format!("{}", self.epoch),
//             format!("{}", hex::encode(&self.pool_id)),
//             format!("{}", self.amount),
//             format!("{}", self.as_leader),
//         ]
//     }
// }

enum Formatter<T: TableRow> {
    Table(Table, PhantomData<T>),
    // TODO
    // Json,
}

impl<T: TableRow> Formatter<T> {
    fn new_table() -> Self {
        let mut table = Table::new();
        table.set_header(T::header());

        Self::Table(table, PhantomData::<T>)
    }

    fn write(&mut self, key: EntityKey, value: T) {
        match self {
            Formatter::Table(table, _) => {
                let row = value.row(&key);
                table.add_row(row);
            }
        }
    }

    fn flush(self) {
        match self {
            Formatter::Table(table, _) => println!("{table}"),
        }
    }
}

fn dump_state<T: TableRow>(
    state: &impl StateStore,
    ns: Namespace,
    count: usize,
) -> miette::Result<()> {
    let mut formatter = Formatter::<T>::new_table();

    state
        .iter_entities_typed::<T>(ns, None)
        .into_diagnostic()
        .context("iterating entities")?
        .take(count)
        .for_each(|x| match x {
            Ok((key, value)) => formatter.write(key, value),
            Err(e) => panic!("{e}"),
        });

    formatter.flush();

    Ok(())
}

fn dump_account(state: &impl StateStore, ns: Namespace, count: usize) -> miette::Result<()> {
    let mut formatter = Formatter::<AccountState>::new_table();

    state
        .iter_entities_typed::<AccountState>(ns, None)
        .into_diagnostic()
        .context("iterating entities")?
        .take(count)
        .filter_ok(|x| x.1.pool.go().is_some())
        .for_each(|x| match x {
            Ok((key, value)) => formatter.write(key, value),
            Err(e) => panic!("{e}"),
        });

    formatter.flush();

    Ok(())
}

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let state = crate::common::open_state_store(config)?;

    match args.namespace.as_str() {
        "eras" => dump_state::<EraSummary>(&state, "eras", args.count)?,
        "epochs" => dump_state::<EpochState>(&state, "epochs", args.count)?,
        "accounts" => dump_account(&state, "accounts", args.count)?,
        "pools" => dump_state::<PoolState>(&state, "pools", args.count)?,
        //"rewards" => dump_state::<RewardState>(&state, "rewards", args.count)?,
        _ => todo!(),
    }

    Ok(())
}
