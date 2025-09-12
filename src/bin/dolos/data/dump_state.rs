use std::marker::PhantomData;

use comfy_table::Table;
use dolos_cardano::{
    model::AccountState, EpochState, EraSummary, PoolState, RewardLog, EPOCH_KEY_MARK,
};
use miette::{bail, Context, IntoDiagnostic};

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

impl TableRow for AccountState {
    fn header() -> Vec<&'static str> {
        vec!["cred", "live stake", "seen addresses", "pool id"]
    }

    fn row(&self, key: &EntityKey) -> Vec<String> {
        vec![
            format!("{}", hex::encode(key)),
            format!("{}", self.live_stake),
            format!("{}", self.seen_addresses.len()),
            format!(
                "{}",
                self.pool_id
                    .as_ref()
                    .map(|x| hex::encode(x))
                    .unwrap_or_default()
            ),
        ]
    }
}

impl TableRow for EpochState {
    fn header() -> Vec<&'static str> {
        vec![
            "number",
            "version",
            "pparams",
            "gathered fees",
            "decayed deposits",
            "deposits",
            "reserves",
            "treasury",
            "end reserves",
            "to treasury",
            "to distribute",
        ]
    }

    fn row(&self, key: &EntityKey) -> Vec<String> {
        vec![
            format!("{}", self.number),
            format!("{}", self.pparams.protocol_major().unwrap_or_default()),
            format!("{}", self.pparams.len()),
            format!("{}", self.gathered_fees),
            format!("{}", self.decayed_deposits),
            format!("{}", self.deposits),
            format!("{}", self.reserves),
            format!("{}", self.treasury),
            format!("{}", self.rewards_to_treasury.unwrap_or_default()),
            format!("{}", self.rewards_to_distribute.unwrap_or_default()),
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

impl TableRow for PoolState {
    fn header() -> Vec<&'static str> {
        vec![
            "key",
            "vrf keyhash",
            "reward account",
            "active stake",
            "live stake",
            "blocks minted",
        ]
    }

    fn row(&self, key: &EntityKey) -> Vec<String> {
        vec![
            format!("{}", hex::encode(key)),
            format!("{}", self.vrf_keyhash),
            format!("{}", hex::encode(&self.reward_account)),
            format!("{}", self.active_stake),
            format!("{}", self.live_stake),
            format!("{}", self.blocks_minted),
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

        Self::Table(table, PhantomData::<T>::default())
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

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let state = crate::common::open_state_store(config)?;

    match args.namespace.as_str() {
        "eras" => dump_state::<EraSummary>(&state, "eras", args.count)?,
        "epochs" => dump_state::<EpochState>(&state, "epochs", args.count)?,
        "accounts" => dump_state::<AccountState>(&state, "accounts", args.count)?,
        "pools" => dump_state::<PoolState>(&state, "pools", args.count)?,
        //"rewards" => dump_state::<RewardState>(&state, "rewards", args.count)?,
        _ => todo!(),
    }

    Ok(())
}
