use handlebars::Handlebars;
use miette::{Context, IntoDiagnostic};

pub const ACCOUNTS: &str = include_str!("accounts.sql");
pub const ASSETS: &str = include_str!("assets.sql");
pub const POOLS: &str = include_str!("pools.sql");
pub const EPOCHS: &str = include_str!("epochs.sql");
pub const DREPS: &str = include_str!("dreps.sql");

pub fn init_registry() -> miette::Result<Handlebars<'static>> {
    let mut reg = Handlebars::new();
    reg.register_template_string("accounts", ACCOUNTS)
        .into_diagnostic()
        .context("registering template")?;
    reg.register_template_string("assets", ASSETS)
        .into_diagnostic()
        .context("registering template")?;
    reg.register_template_string("pools", POOLS)
        .into_diagnostic()
        .context("registering template")?;
    reg.register_template_string("epochs", EPOCHS)
        .into_diagnostic()
        .context("registering template")?;
    reg.register_template_string("dreps", DREPS)
        .into_diagnostic()
        .context("registering template")?;

    Ok(reg)
}
