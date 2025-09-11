use handlebars::Handlebars;
use miette::{Context, IntoDiagnostic};

pub const ACCOUNTS: &str = include_str!("accounts.sql");
pub const POOLS: &str = include_str!("pools.sql");

pub fn init_registry() -> miette::Result<Handlebars<'static>> {
    let mut reg = Handlebars::new();
    reg.register_template_string("accounts", ACCOUNTS)
        .into_diagnostic()
        .context("registering template")?;
    reg.register_template_string("pools", POOLS)
        .into_diagnostic()
        .context("registering template")?;

    Ok(reg)
}
