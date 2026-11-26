//! `dolos health`: whether the running node is live, or ready to serve, read
//! from the status file it keeps. What a supervisor's exec probe runs.
//!
//! It takes no lock and opens no store, so it runs beside the node. It exits
//! 0 when the check passes, and 3 with one line on stderr naming the failed
//! check when it does not.

use std::net::ToSocketAddrs as _;
use std::num::NonZeroU64;
use std::time::{Duration, SystemTime};

use dolos::health::check::{self, Bounds, Endpoint, Failure, Mode};
use dolos::health::Command;
use dolos_core::config::RootConfig;

use crate::output::{emit, Format, FormatArgs, Probe};

#[derive(Debug, clap::Args)]
#[command(group(clap::ArgGroup::new("mode").required(true).args(["live", "ready"])))]
pub struct Args {
    /// Pass while the node runs and its sync is not wedged; a node catching
    /// up or retrying a lost upstream is live
    #[arg(long)]
    live: bool,

    /// Pass while the node is live, follows its upstream and is within the
    /// lag bound
    #[arg(long)]
    ready: bool,

    /// Also connect to each serving port the configuration enables
    #[arg(long)]
    check_ports: bool,

    /// How old the last status write may be; overrides
    /// `health.max_write_age_seconds`
    #[arg(long, value_name = "SECONDS")]
    max_write_age_seconds: Option<NonZeroU64>,

    /// How long the sync may stand still with the upstream ahead; overrides
    /// `health.max_stall_seconds`
    #[arg(long, value_name = "SECONDS")]
    max_stall_seconds: Option<NonZeroU64>,

    /// How far behind the upstream's tip a ready node may be; overrides
    /// `health.max_lag_slots`
    #[arg(long, value_name = "SLOTS")]
    max_lag_slots: Option<u64>,

    /// How old the local tip may be for a ready node; overrides
    /// `health.max_tip_age_seconds`
    #[arg(long, value_name = "SECONDS")]
    max_tip_age_seconds: Option<NonZeroU64>,

    #[command(flatten)]
    format: FormatArgs,
}

impl Args {
    fn mode(&self) -> Mode {
        if self.ready {
            Mode::Ready
        } else {
            Mode::Live
        }
    }

    /// The configuration's bounds, with the flags given over them.
    fn bounds(&self, config: &RootConfig) -> Bounds {
        let seconds = |seconds: NonZeroU64| Duration::from_secs(seconds.get());
        let mut bounds = Bounds::from(&config.health);

        if let Some(age) = self.max_write_age_seconds {
            bounds.max_write_age = seconds(age);
        }
        if let Some(stall) = self.max_stall_seconds {
            bounds.max_stall = seconds(stall);
        }
        if let Some(lag) = self.max_lag_slots {
            bounds.max_lag_slots = lag;
        }
        if let Some(age) = self.max_tip_age_seconds {
            bounds.max_tip_age = Some(seconds(age));
        }

        bounds
    }
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    let mut verdict = check::probe(
        args.mode(),
        &args.bounds(config),
        &config.status_path(),
        &config.status_lock_path(),
        SystemTime::now(),
    );

    if verdict.passed && args.check_ports {
        let command = verdict.status.as_ref().map(|status| status.command);

        if let Err(failure) = endpoints(config, command).and_then(|ends| check::ports(&ends)) {
            verdict.fail(failure);
        }
    }

    if verdict.passed || args.format.format == Format::Json {
        emit(&verdict, &args.format)?;
    }

    if verdict.passed {
        Ok(())
    } else {
        Err(Probe(verdict.to_string()).into())
    }
}

/// Each serving listener the configuration enables, by name. The relay
/// listens only under `daemon`.
fn endpoints(
    config: &RootConfig,
    command: Option<Command>,
) -> Result<Vec<(&'static str, Endpoint)>, Failure> {
    let serve = &config.serve;
    let mut endpoints = Vec::new();

    let mut named = |name: &'static str, address: &str| -> Result<(), Failure> {
        let resolved = address
            .to_socket_addrs()
            .and_then(|mut addresses| {
                addresses
                    .next()
                    .ok_or_else(|| std::io::Error::other("it resolves to no address"))
            })
            .map_err(|error| Failure::PortClosed {
                name: name.into(),
                address: address.into(),
                error: error.to_string(),
            })?;

        endpoints.push((name, Endpoint::local(resolved)));
        Ok(())
    };

    if let Some(grpc) = &serve.grpc {
        named("grpc", &grpc.listen_address)?;
    }
    if let Some(minibf) = &serve.minibf {
        named("minibf", &minibf.listen_address.to_string())?;
    }
    if let Some(minikupo) = &serve.minikupo {
        named("minikupo", &minikupo.listen_address.to_string())?;
    }
    if let Some(trp) = &serve.trp {
        named("trp", &trp.listen_address.to_string())?;
    }
    if let (Some(relay), Some(Command::Daemon)) = (&config.relay, command) {
        named("relay", &relay.listen_address)?;
    }

    #[cfg(unix)]
    if let Some(ouroboros) = &serve.ouroboros {
        endpoints.push(("ouroboros", Endpoint::Unix(ouroboros.listen_path.clone())));
    }

    Ok(endpoints)
}
