pub mod dictionary;
pub mod load;
pub mod measure;
pub mod minibf;
pub mod report;
pub mod storage;

#[derive(clap::Subcommand)]
pub enum Cmd {
    #[command(subcommand, about = "Storage, codec and import benchmarks")]
    Storage(storage::Cmd),
    #[command(subcommand, about = "Minibf endpoint and HTTP benchmarks")]
    Minibf(minibf::Cmd),
    #[command(about = "Render shared benchmark records")]
    Report {
        #[arg(required = true)]
        files: Vec<std::path::PathBuf>,
    },
}

pub fn run(command: Cmd) -> anyhow::Result<()> {
    match command {
        Cmd::Storage(command) => storage::run(command),
        Cmd::Minibf(command) => minibf::dispatch(command),
        Cmd::Report { files } => {
            print!("{}", report::render(&report::load(&files)?));
            Ok(())
        }
    }
}

pub fn parse_list<T: std::str::FromStr>(s: &str) -> anyhow::Result<Vec<T>>
where
    T::Err: std::fmt::Display,
{
    s.split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(|p| p.parse::<T>().map_err(|e| anyhow::anyhow!("{p}: {e}")))
        .collect()
}

/// The command line as a shell would need it typed, so a recorded command
/// replays.
pub fn command_line() -> String {
    std::env::args()
        .map(|arg| shell_word(&arg))
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_word(arg: &str) -> String {
    let plain = |c: char| c.is_ascii_alphanumeric() || "-_./=,:@+%".contains(c);
    if !arg.is_empty() && arg.chars().all(plain) {
        arg.to_string()
    } else {
        format!("'{}'", arg.replace('\'', "'\\''"))
    }
}
