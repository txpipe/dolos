use comfy_table::Table;
use dolos_cardano::model::AccountState;
use dolos_core::StateStore;
use miette::IntoDiagnostic as _;
use pallas::crypto::hash::Hash;
use std::collections::HashMap;

#[derive(Debug, clap::Args)]
pub struct Args {}

enum Formatter {
    Table(Table),
    // TODO
    // Json,
}

impl Formatter {
    fn new_table() -> Self {
        let mut table = Table::new();
        table.set_header(vec!["pool", "delegation"]);

        Self::Table(table)
    }

    fn write(&mut self, pool_id: Hash<28>, delegation: u128) {
        match self {
            Formatter::Table(table) => {
                table.add_row(vec![
                    format!("{}", hex::encode(pool_id)),
                    format!("{}", delegation),
                ]);
            }
        }
    }

    fn flush(self) {
        match self {
            Formatter::Table(table) => println!("{table}"),
        }
    }
}

pub fn compute_spdd(store: &impl StateStore) -> miette::Result<HashMap<Hash<28>, u128>> {
    let mut by_pool = HashMap::<Hash<28>, u128>::new();

    let all_accounts = store
        .iter_entities_typed::<AccountState>("accounts", None)
        .into_diagnostic()?;

    for record in all_accounts {
        let (_, value) = record.into_diagnostic()?;

        if let Some(pool_id) = value.pool.latest {
            let entry = by_pool.entry(pool_id).or_insert(0);
            *entry += value.live_stake() as u128;
        }
    }

    Ok(by_pool)
}

pub fn run(config: &crate::Config, _args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let state = crate::common::open_state_store(config)?;

    let spdd = compute_spdd(&state)?;

    let mut formatter = Formatter::new_table();

    for (pool_id, delegation) in spdd {
        formatter.write(pool_id, delegation);
    }

    formatter.flush();

    Ok(())
}
