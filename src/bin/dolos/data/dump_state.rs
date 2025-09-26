use std::marker::PhantomData;

use comfy_table::Table;
use dolos_cardano::{model::AccountState, EpochState, EraSummary, PoolState};
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

impl TableRow for AccountState {
    fn header() -> Vec<&'static str> {
        vec![
            "cred",
            "active stake",
            "wait stake",
            "rewards sum",
            "withdrawals sum",
            "latest pool",
            "active pool",
            "drep",
        ]
    }

    fn row(&self, key: &EntityKey) -> Vec<String> {
        vec![
            format!("{}", hex::encode(key)),
            format!("{}", self.active_stake),
            format!("{}", self.wait_stake),
            format!("{}", self.rewards_sum),
            format!("{}", self.withdrawals_sum),
            format!(
                "{}",
                self.latest_pool
                    .as_ref()
                    .map(hex::encode)
                    .unwrap_or_default()
            ),
            format!(
                "{}",
                self.active_pool
                    .as_ref()
                    .map(hex::encode)
                    .unwrap_or_default()
            ),
            format!("{}", self.latest_drep.is_some()),
        ]
    }
}

impl TableRow for EpochState {
    fn header() -> Vec<&'static str> {
        vec![
            "number",
            "version",
            "gathered fees",
            "gathered deposits",
            "decayed deposits",
            "reserves",
            "utxos",
            "treasury",
            "to treasury",
            "to distribute",
            "nonce",
        ]
    }

    fn row(&self, _key: &EntityKey) -> Vec<String> {
        vec![
            format!("{}", self.number),
            format!("{}", self.pparams.protocol_major().unwrap_or_default()),
            format!("{}", self.gathered_fees),
            format!("{}", self.gathered_deposits),
            format!("{}", self.decayed_deposits),
            format!("{}", self.reserves),
            format!("{}", self.utxos),
            format!("{}", self.treasury),
            format!("{}", self.rewards_to_treasury.unwrap_or_default()),
            format!("{}", self.rewards_to_distribute.unwrap_or_default()),
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

impl TableRow for PoolState {
    fn header() -> Vec<&'static str> {
        vec![
            "key",
            "vrf keyhash",
            "reward account",
            "active stake",
            "wait stake",
            "blocks minted",
        ]
    }

    fn row(&self, key: &EntityKey) -> Vec<String> {
        vec![
            format!("{}", hex::encode(key)),
            format!("{}", self.vrf_keyhash),
            format!("{}", hex::encode(&self.reward_account)),
            format!("{}", self.active_stake),
            format!("{}", self.wait_stake),
            format!("{}", self.blocks_minted_total),
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
