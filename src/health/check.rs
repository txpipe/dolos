//! The checks `dolos health` runs over a node's status file and lock: whether
//! the node is live, and whether it is ready to serve.
//!
//! The checks read the two files only. They take no lock, open no store and
//! call no network, except the opt-in connection to each serving port.

use std::fmt;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use dolos_core::config::HealthConfig;
use serde::Serialize;

use super::{lock_held, unix_ms, Command, Status, SyncStatus, FORMAT};
use crate::sync::SyncState;

/// How long the port check waits for one port to accept.
const CONNECT_PATIENCE: Duration = Duration::from_secs(2);

/// Which question a check answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Mode {
    /// The node runs and its pipeline is not wedged: restarting it would not
    /// help.
    Live,
    /// The node is live and level with its upstream: it can take traffic.
    Ready,
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Live => "live",
            Self::Ready => "ready",
        })
    }
}

/// The bounds a status is judged by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bounds {
    pub max_write_age: Duration,
    pub max_stall: Duration,
    pub max_lag_slots: u64,
    pub max_tip_age: Option<Duration>,
}

impl From<&HealthConfig> for Bounds {
    fn from(config: &HealthConfig) -> Self {
        Self {
            max_write_age: config.max_write_age(),
            max_stall: config.max_stall(),
            max_lag_slots: config.max_lag_slots,
            max_tip_age: config.max_tip_age(),
        }
    }
}

/// The check that failed, and the values behind it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "check", rename_all = "kebab-case")]
pub enum Failure {
    NoLock {
        path: PathBuf,
    },
    LockUnreadable {
        path: PathBuf,
        error: String,
    },
    NoFile {
        path: PathBuf,
    },
    Unreadable {
        path: PathBuf,
        error: String,
    },
    Unparsable {
        path: PathBuf,
        error: String,
    },
    UnknownFormat {
        path: PathBuf,
        format: u64,
    },
    StaleWrite {
        age_seconds: u64,
        max_write_age_seconds: u64,
    },
    Wedged {
        local_tip_slot: Option<u64>,
        upstream_tip_slot: Option<u64>,
        stalled_seconds: u64,
        max_stall_seconds: u64,
    },
    NotFollowing {
        state: SyncState,
    },
    Lagging {
        lag_slots: u64,
        max_lag_slots: u64,
    },
    TipAgeUnknown {
        local_tip_slot: Option<u64>,
    },
    OldTip {
        age_seconds: u64,
        max_tip_age_seconds: u64,
    },
    PortClosed {
        name: String,
        address: String,
        error: String,
    },
}

fn slot(slot: &Option<u64>) -> String {
    slot.map_or_else(|| "none".into(), |slot| slot.to_string())
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoLock { path } => {
                write!(f, "no process holds the status lock {}", path.display())
            }
            Self::LockUnreadable { path, error } => {
                write!(
                    f,
                    "the status lock {} can't be probed: {error}",
                    path.display()
                )
            }
            Self::NoFile { path } => write!(f, "there is no status file {}", path.display()),
            Self::Unreadable { path, error } => {
                write!(
                    f,
                    "the status file {} can't be read: {error}",
                    path.display()
                )
            }
            Self::Unparsable { path, error } => {
                write!(
                    f,
                    "the status file {} does not parse: {error}",
                    path.display()
                )
            }
            Self::UnknownFormat { path, format } => write!(
                f,
                "the status file {} has format {format}, and this release reads {FORMAT}",
                path.display()
            ),
            Self::StaleWrite {
                age_seconds,
                max_write_age_seconds,
            } => write!(
                f,
                "the status was written {age_seconds}s ago, over max_write_age_seconds \
                 {max_write_age_seconds}"
            ),
            Self::Wedged {
                local_tip_slot,
                upstream_tip_slot,
                stalled_seconds,
                max_stall_seconds,
            } => write!(
                f,
                "the pipeline has not advanced for {stalled_seconds}s with the upstream ahead \
                 (local tip slot {}, upstream tip slot {}), over max_stall_seconds \
                 {max_stall_seconds}",
                slot(local_tip_slot),
                slot(upstream_tip_slot)
            ),
            Self::NotFollowing { state } => write!(
                f,
                "the sync is {}, not following",
                serde_json::to_value(state)
                    .ok()
                    .and_then(|state| state.as_str().map(str::to_owned))
                    .unwrap_or_else(|| format!("{state:?}"))
            ),
            Self::Lagging {
                lag_slots,
                max_lag_slots,
            } => write!(
                f,
                "the local tip is {lag_slots} slots behind the upstream's, over max_lag_slots \
                 {max_lag_slots}"
            ),
            Self::TipAgeUnknown { local_tip_slot } => write!(
                f,
                "the wall-clock time of the local tip (slot {}) is unknown, and \
                 max_tip_age_seconds is set",
                slot(local_tip_slot)
            ),
            Self::OldTip {
                age_seconds,
                max_tip_age_seconds,
            } => write!(
                f,
                "the local tip is {age_seconds}s old, over max_tip_age_seconds \
                 {max_tip_age_seconds}"
            ),
            Self::PortClosed {
                name,
                address,
                error,
            } => write!(f, "the {name} port {address} does not accept: {error}"),
        }
    }
}

/// What a check found: the status it read, if any, and the failed check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Verdict {
    pub mode: Mode,
    pub passed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<Failure>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<Status>,
}

impl Verdict {
    fn new(mode: Mode, status: Option<Status>, failure: Option<Failure>) -> Self {
        Self {
            mode,
            passed: failure.is_none(),
            failure,
            status,
        }
    }

    /// Fail the verdict on `failure`, if it passed.
    pub fn fail(&mut self, failure: Failure) {
        if self.passed {
            self.passed = false;
            self.failure = Some(failure);
        }
    }
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(failure) = &self.failure {
            return write!(f, "not {}: {failure}", self.mode);
        }

        write!(f, "{}", self.mode)?;

        if let Some(status) = &self.status {
            let command = match status.command {
                Command::Daemon => "daemon",
                Command::Serve => "serve",
            };
            write!(f, ": {command} pid {}", status.pid)?;

            if let Some(sync) = &status.sync {
                write!(f, ", local tip slot {}", slot(&sync.local_tip_slot))?;
                if let Some(lag) = sync.lag_slots {
                    write!(f, ", {lag} slots behind the upstream")?;
                }
            }
        }

        Ok(())
    }
}

/// Check the node whose status file is `path`, with its lock at `lock`, as
/// of `now`. The checks run in order, and the first to fail is the verdict:
/// the lock is held, the file parses in a known format, it was written
/// recently, then the mode's own rule.
pub fn probe(mode: Mode, bounds: &Bounds, path: &Path, lock: &Path, now: SystemTime) -> Verdict {
    match lock_held(lock) {
        Ok(true) => (),
        Ok(false) => return Verdict::new(mode, None, Some(Failure::NoLock { path: lock.into() })),
        Err(error) => {
            let failure = Failure::LockUnreadable {
                path: lock.into(),
                error: error.to_string(),
            };
            return Verdict::new(mode, None, Some(failure));
        }
    }

    let status = match read(path) {
        Ok(status) => status,
        Err(failure) => return Verdict::new(mode, None, Some(failure)),
    };

    let failure = judge(mode, bounds, &status, now).err();
    Verdict::new(mode, Some(status), failure)
}

/// Read the status at `path`, telling a missing file, one that doesn't parse
/// and one in a format this release doesn't know apart.
fn read(path: &Path) -> Result<Status, Failure> {
    let unparsable = |error: &dyn fmt::Display| Failure::Unparsable {
        path: path.into(),
        error: error.to_string(),
    };

    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(Failure::NoFile { path: path.into() })
        }
        Err(error) => {
            return Err(Failure::Unreadable {
                path: path.into(),
                error: error.to_string(),
            })
        }
    };

    let document: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|error| unparsable(&error))?;

    let format = document
        .get("format")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| unparsable(&"no format version"))?;

    if format != u64::from(FORMAT) {
        return Err(Failure::UnknownFormat {
            path: path.into(),
            format,
        });
    }

    serde_json::from_value(document).map_err(|error| unparsable(&error))
}

/// How long before `now` the instant `unix_ms` was, none in the future.
fn age(now: SystemTime, unix_ms_then: u64) -> Duration {
    Duration::from_millis(unix_ms(now).saturating_sub(unix_ms_then))
}

/// Judge a status read under a held lock, as of `now`.
pub fn judge(mode: Mode, bounds: &Bounds, status: &Status, now: SystemTime) -> Result<(), Failure> {
    let written = age(now, status.written_unix_ms);
    if written > bounds.max_write_age {
        return Err(Failure::StaleWrite {
            age_seconds: written.as_secs(),
            max_write_age_seconds: bounds.max_write_age.as_secs(),
        });
    }

    // A node that does not sync is live while it writes, and ready with it.
    let Some(sync) = &status.sync else {
        return Ok(());
    };

    wedged(bounds, status, sync, now)?;

    if mode == Mode::Ready {
        ready(bounds, sync, now)?;
    }

    Ok(())
}

/// A pipeline is wedged when a session is open, the upstream's tip is ahead
/// and nothing advanced for longer than the bound. A node retrying a lost
/// upstream is not: a restart would not bring the upstream back.
fn wedged(
    bounds: &Bounds,
    status: &Status,
    sync: &SyncStatus,
    now: SystemTime,
) -> Result<(), Failure> {
    let session = matches!(sync.state, SyncState::CatchingUp | SyncState::Following);
    let behind = sync.lag_slots.is_some_and(|lag| lag > 0);

    if !session || !behind {
        return Ok(());
    }

    let stalled = age(now, sync.advanced_unix_ms.unwrap_or(status.started_unix_ms));

    if stalled > bounds.max_stall {
        return Err(Failure::Wedged {
            local_tip_slot: sync.local_tip_slot,
            upstream_tip_slot: sync.upstream_tip_slot,
            stalled_seconds: stalled.as_secs(),
            max_stall_seconds: bounds.max_stall.as_secs(),
        });
    }

    Ok(())
}

/// A live node is ready when it follows its upstream within the lag bound,
/// and its tip within the age bound when one is set.
fn ready(bounds: &Bounds, sync: &SyncStatus, now: SystemTime) -> Result<(), Failure> {
    if sync.state != SyncState::Following {
        return Err(Failure::NotFollowing { state: sync.state });
    }

    // An emulator upstream has no tip, so nothing to lag behind.
    let lag = sync.lag_slots.unwrap_or(0);
    if lag > bounds.max_lag_slots {
        return Err(Failure::Lagging {
            lag_slots: lag,
            max_lag_slots: bounds.max_lag_slots,
        });
    }

    let Some(max_tip_age) = bounds.max_tip_age else {
        return Ok(());
    };

    let Some(tip) = sync.local_tip_unix_ms else {
        return Err(Failure::TipAgeUnknown {
            local_tip_slot: sync.local_tip_slot,
        });
    };

    let tip_age = age(now, tip);
    if tip_age > max_tip_age {
        return Err(Failure::OldTip {
            age_seconds: tip_age.as_secs(),
            max_tip_age_seconds: max_tip_age.as_secs(),
        });
    }

    Ok(())
}

/// Where a serving API listens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    Tcp(SocketAddr),
    #[cfg(unix)]
    Unix(PathBuf),
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp(address) => write!(f, "{address}"),
            #[cfg(unix)]
            Self::Unix(path) => write!(f, "{}", path.display()),
        }
    }
}

impl Endpoint {
    /// The address a local client reaches a listener on: a wildcard bind is
    /// reached on loopback.
    pub fn local(mut address: SocketAddr) -> Self {
        if address.ip().is_unspecified() {
            address.set_ip(match address {
                SocketAddr::V4(_) => std::net::Ipv4Addr::LOCALHOST.into(),
                SocketAddr::V6(_) => std::net::Ipv6Addr::LOCALHOST.into(),
            });
        }

        Self::Tcp(address)
    }

    fn connect(&self) -> std::io::Result<()> {
        match self {
            Self::Tcp(address) => TcpStream::connect_timeout(address, CONNECT_PATIENCE).map(drop),
            #[cfg(unix)]
            Self::Unix(path) => std::os::unix::net::UnixStream::connect(path).map(drop),
        }
    }
}

/// Connect to each named endpoint in turn; the first that refuses fails.
pub fn ports(endpoints: &[(&str, Endpoint)]) -> Result<(), Failure> {
    for (name, endpoint) in endpoints {
        if let Err(error) = endpoint.connect() {
            return Err(Failure::PortClosed {
                name: (*name).into(),
                address: endpoint.to_string(),
                error: error.to_string(),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests;
