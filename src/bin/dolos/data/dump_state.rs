use comfy_table::Table;
use dolos_cardano::model::AccountState;
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

enum Formatter {
    Table(Table),
    // TODO
    // Json,
}

impl Formatter {
    fn new_table() -> Self {
        let mut table = Table::new();
        table.set_header(vec!["cred", "controlled amount", "seen addresses"]);

        Self::Table(table)
    }

    fn write(&mut self, key: Vec<u8>, value: AccountState) {
        match self {
            Formatter::Table(table) => {
                table.add_row(vec![
                    format!("{}", hex::encode(&key)),
                    format!("{}", value.controlled_amount),
                    format!("{}", value.seen_addresses.len()),
                    format!(
                        "{}",
                        value.pool_id.map(|x| hex::encode(x)).unwrap_or_default()
                    ),
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

pub fn run(config: &crate::Config, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing(&config.logging)?;

    let state = crate::common::open_state3_store(config)?;

    let mut formatter = Formatter::new_table();

    let start = &[0u8; 32].as_slice();
    let end = &[255u8; 32].as_slice();

    state
        .iter_entities_typed::<AccountState>(start..end)
        .into_diagnostic()
        .context("iterating entities")?
        //.filter_ok(|(_, val)| val.controlled_amount > 0)
        //.filter_ok(|(_, val)| val.pool_id.is_some())
        .take(args.count)
        .for_each(|x| match x {
            Ok((key, value)) => formatter.write(key, value),
            Err(_) => todo!(),
        });

    formatter.flush();

    Ok(())
}
