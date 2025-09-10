use handlebars::Handlebars;
use miette::{Context, IntoDiagnostic};

pub const ACCOUNTS: &str = include_str!("accounts.sql");

pub fn init_registry() -> miette::Result<Handlebars<'static>> {
    let mut reg = Handlebars::new();
    reg.register_template_string("accounts", ACCOUNTS)
        .into_diagnostic()
        .context("registering template")?;
    Ok(reg)
}
