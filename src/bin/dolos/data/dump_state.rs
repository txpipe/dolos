use std::marker::PhantomData;

use comfy_table::Table;
use dolos_cardano::{model::AccountState, EpochState, EraSummary};
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
        vec!["cred", "controlled amount", "seen addresses", "pool id"]
    }

    fn row(&self, key: &EntityKey) -> Vec<String> {
        vec![
            format!("{}", self.controlled_amount),
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
            "key",
            "number",
            "version",
            "pparams",
            "gathered fees",
            "decayed deposits",
            "rewards",
        ]
    }

    fn row(&self, key: &EntityKey) -> Vec<String> {
        vec![
            format!("{}", hex::encode(key)),
            format!("{}", self.number),
            format!("{}", self.pparams.protocol_major().unwrap_or_default()),
            format!("{}", self.pparams.len()),
            format!("{}", self.gathered_fees),
            format!("{}", self.decayed_deposits),
            format!("{}", self.rewards),
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
    state: &impl State3Store,
    ns: Namespace,
    count: usize,
) -> miette::Result<()> {
    let mut formatter = Formatter::<T>::new_table();

    state
        .iter_entities_typed::<T>(ns, None)
        .into_diagnostic()
        .context("iterating entities")?
        //.filter_ok(|(_, val)| val.controlled_amount > 0)
        //.filter_ok(|(_, val)| val.pool_id.is_some())
        .take(count)
        .for_each(|x| match x {
            Ok((key, value)) => formatter.write(key, value),
            Err(_) => todo!(),
        });

    formatter.flush();

    Ok(())
}

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let state = crate::common::open_state3_store(config)?;

    match args.namespace.as_str() {
        "eras" => dump_state::<EraSummary>(&state, "eras", args.count)?,
        "epochs" => dump_state::<EpochState>(&state, "epochs", args.count)?,
        "accounts" => dump_state::<AccountState>(&state, "accounts", args.count)?,
        _ => todo!(),
    }

    Ok(())
}
