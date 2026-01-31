//! test-instance subcommands.

use anyhow::Result;
use clap::Subcommand;
use xshell::Shell;

pub mod create;
pub mod delete;

#[derive(Debug, Subcommand)]
pub enum TestInstanceCmd {
    /// Create a test instance
    Create(create::CreateArgs),

    /// Delete a test instance directory
    Delete(delete::DeleteArgs),
}

pub fn run(sh: &Shell, cmd: TestInstanceCmd) -> Result<()> {
    match cmd {
        TestInstanceCmd::Create(args) => create::run(sh, &args),
        TestInstanceCmd::Delete(args) => delete::run(&args),
    }
}
