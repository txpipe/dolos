//! `dolos daemon` as a native OS service: one systemd unit or launchd agent
//! per named instance, each recording the binary, configuration and working
//! directory it runs with.
//!
//! The service manager owns background execution and restarts; nothing here
//! supervises. A [`ServiceBackend`] keeps a manager's files under a directory
//! it is given and runs the manager's commands through a [`CommandRunner`], so
//! tests install over fixture directories and register nothing. An install or
//! uninstall holds an exclusive lock on that directory from its checks to its
//! last change, so that two installs cannot both pass the conflict check.
//!
//! Configurations are read from their files alone: a service starts without
//! the installing shell's `DOLOS_` variables.

mod conflicts;
mod launchd;
mod systemd;

pub use launchd::Launchd;
pub use systemd::Systemd;

use std::borrow::Cow;
use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant};

use dolos_cardano::networks::ResolvedNetwork;
use dolos_core::config::{Protocol, RootConfig, DEFAULT_CONFIG_FILE};
use serde::Serialize;

use conflicts::Resources;

/// An instance's name: 1 to 48 of `a-z`, `0-9` and `-`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Name(String);

impl Name {
    pub const MAX_LEN: usize = 48;

    pub fn parse(value: &str) -> Result<Self, ServiceError> {
        let valid = (1..=Self::MAX_LEN).contains(&value.len())
            && value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');

        if valid {
            Ok(Self(value.to_string()))
        } else {
            Err(ServiceError::InvalidName(value.to_string()))
        }
    }

    /// `{chain.protocol}-{chain.network}`: the name an instance gets when
    /// none is given. A known network is named as the catalog spells it,
    /// since it loads in any case.
    pub fn derive(config: &RootConfig) -> Result<Self, ServiceError> {
        let protocol = match config.chain.protocol {
            Protocol::Cardano => "cardano",
        };

        let network = match ResolvedNetwork::resolve(&config.chain) {
            Ok(ResolvedNetwork::Known(network)) => network.name,
            _ => &config.chain.network,
        };

        Self::parse(&format!("{protocol}-{network}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Manager {
    Systemd,
    Launchd,
}

impl fmt::Display for Manager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Systemd => "systemd",
            Self::Launchd => "launchd",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    User,
    System,
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::User => "user",
            Self::System => "system",
        })
    }
}

/// When an installed service starts on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Autostart {
    Login,
    Boot,
}

impl fmt::Display for Autostart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Login => "login",
            Self::Boot => "boot",
        })
    }
}

/// What an installation runs: `<binary> --config <config> daemon`, in
/// `working_dir`.
#[derive(Debug, Clone)]
pub struct Unit {
    pub name: Name,
    pub binary: PathBuf,
    pub config: PathBuf,
    pub working_dir: PathBuf,
}

impl Unit {
    pub fn arguments(&self) -> Result<[&str; 4], ServiceError> {
        Ok([
            text(&self.binary)?,
            "--config",
            text(&self.config)?,
            "daemon",
        ])
    }
}

/// The line every service file Dolos writes carries, and that it removes
/// only files carrying.
const MARKER: &str = "Written by `dolos service install`.";

/// `path` as text a service file can hold.
fn text(path: &Path) -> Result<&str, ServiceError> {
    path.to_str()
        .filter(|text| !text.chars().any(char::is_control))
        .ok_or_else(|| ServiceError::UnsupportedPath(path.to_path_buf()))
}

/// The configuration and working directory a service file runs `dolos` with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recorded {
    pub config: PathBuf,
    pub working_dir: PathBuf,
}

impl Recorded {
    /// From a command line: its `--config`, else `dolos.toml`, against
    /// `working_dir`.
    fn from_arguments(arguments: &[String], working_dir: PathBuf) -> Self {
        let mut config = None;
        let mut words = arguments.iter().skip(1);

        while let Some(word) = words.next() {
            if word == "--config" || word == "-c" {
                config = words.next().cloned();
            } else if let Some(value) = word.strip_prefix("--config=") {
                config = Some(value.to_string());
            } else if let Some(value) = word.strip_prefix("-c") {
                config = Some(value.trim_start_matches('=').to_string());
            }
        }

        let config = config.unwrap_or_else(|| DEFAULT_CONFIG_FILE.to_string());

        Self {
            config: working_dir.join(config),
            working_dir,
        }
    }
}

/// The service manager's own commands for one instance.
#[derive(Debug, Clone, Serialize)]
pub struct Commands {
    pub start: String,
    pub stop: String,
    pub restart: String,
    pub status: String,
    pub logs: String,
}

impl fmt::Display for Commands {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "start:   {}", self.start)?;
        writeln!(f, "stop:    {}", self.stop)?;
        writeln!(f, "restart: {}", self.restart)?;
        writeln!(f, "status:  {}", self.status)?;
        write!(f, "logs:    {}", self.logs)
    }
}

/// Runs the service manager's commands.
pub trait CommandRunner {
    /// Runs `program` with `args`: what it printed, or `Err` with what it
    /// reported when it does not exit 0.
    fn run(&self, program: &str, args: &[&str]) -> Result<String, String>;
}

impl<R: CommandRunner + ?Sized> CommandRunner for &R {
    fn run(&self, program: &str, args: &[&str]) -> Result<String, String> {
        (**self).run(program, args)
    }
}

/// Runs the program on `PATH`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemRunner;

impl CommandRunner for SystemRunner {
    fn run(&self, program: &str, args: &[&str]) -> Result<String, String> {
        let output = std::process::Command::new(program)
            .args(args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .map_err(|error| format!("could not run {program}: {error}"))?;

        if output.status.success() {
            return Ok(String::from_utf8_lossy(&output.stdout).into_owned());
        }

        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();

        Err(if stderr.is_empty() {
            output.status.to_string()
        } else {
            stderr
        })
    }
}

/// A service manager command that failed.
#[derive(Debug, Clone)]
pub struct Failure {
    /// The command, as a shell would run it.
    pub command: String,
    /// What it reported.
    pub detail: String,
}

impl Failure {
    fn of(program: &str, args: &[&str], detail: String) -> Self {
        let command = std::iter::once(program)
            .chain(args.iter().copied())
            .map(shell_quote)
            .collect::<Vec<_>>()
            .join(" ");

        Self { command, detail }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "`{}` failed: {}", self.command, self.detail)
    }
}

/// A registration that failed part way.
#[derive(Debug)]
pub struct RegisterError {
    pub failure: Failure,
    /// Whether the manager read the file, and must be told it is gone.
    pub loaded: bool,
    /// What undoing the registration could not undo.
    pub cleanup: Vec<String>,
}

/// One service manager in one scope.
pub trait ServiceBackend {
    fn manager(&self) -> Manager;

    fn scope(&self) -> Scope;

    /// Where the scope's service files live.
    fn dir(&self) -> &Path;

    /// What the manager calls `name`'s service.
    fn identity(&self, name: &Name) -> String;

    /// `name`'s service file, in [`Self::dir`].
    fn file(&self, name: &Name) -> PathBuf;

    /// What the manager calls the service a file in [`Self::dir`] installs,
    /// for Dolos's own files.
    fn identity_of(&self, file_name: &str) -> Option<String>;

    /// `unit`'s service file.
    fn render(&self, unit: &Unit) -> Result<String, ServiceError>;

    /// What a service file runs `dolos` with, when it can be read back.
    fn read(&self, contents: &str) -> Option<Recorded>;

    /// Refuses when this process may not change the scope.
    fn check_access(&self) -> Result<(), ServiceError>;

    /// Whether the manager runs `name`'s service, will restart it, or keeps
    /// it loaded.
    fn active(&self, name: &Name) -> bool;

    /// Registers the service in `name`'s written file to start on its own,
    /// and starts it now when `start`. On failure, it undoes what it
    /// registered; the file is removed next, then [`Self::forget`] runs when
    /// the error says the manager loaded it.
    fn register(&self, name: &Name, start: bool) -> Result<(), RegisterError>;

    /// Stops and unregisters `name`'s service, whose file is removed next.
    fn unregister(&self, name: &Name) -> Result<(), Failure>;

    /// Tells the manager a service file was removed.
    fn forget(&self) -> Result<(), Failure>;

    fn autostart(&self) -> Autostart;

    fn commands(&self, name: &Name) -> Commands;

    /// What the operator should know beyond the commands.
    fn notes(&self, name: &Name) -> Vec<String>;
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("`{0}` is not a service name: use 1 to 48 of a-z, 0-9 and -")]
    InvalidName(String),

    #[error("system services can only be changed as root")]
    NeedsRoot,

    #[error("HOME is not set, and the user's services live under it")]
    NoHome,

    #[error("{identity} is already installed at {}; uninstall it first to replace it", file.display())]
    AlreadyInstalled { identity: String, file: PathBuf },

    #[error("{identity} is not installed: there is no {}", file.display())]
    NotInstalled { identity: String, file: PathBuf },

    #[error("{} was not written by `dolos service install`, so it is left as it is", file.display())]
    Foreign { identity: String, file: PathBuf },

    #[error("{identity} has no file at {}, but the service manager still has it; stop it with `{stop}`", file.display())]
    Orphaned {
        identity: String,
        file: PathBuf,
        stop: String,
    },

    #[error("this configuration conflicts with {identity} ({}): {reason}", file.display())]
    Conflict {
        identity: String,
        file: PathBuf,
        reason: String,
    },

    #[error("{} cannot be written into a service file", .0.display())]
    UnsupportedPath(PathBuf),

    #[error(
        "another process has held {} locked for {}s; run this again once it is done",
        .0.display(),
        PATIENCE.as_secs()
    )]
    Busy(PathBuf),

    #[error("registering {identity} failed: {failure}; the install was undone{}", also(.cleanup))]
    Register {
        identity: String,
        failure: Failure,
        cleanup: Vec<String>,
    },

    #[error("stopping {identity} failed: {failure}; nothing was removed")]
    Unregister { identity: String, failure: Failure },

    #[error("{} was removed, but {failure}", file.display())]
    Forget { file: PathBuf, failure: Failure },

    #[error("{}: {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

fn also(cleanup: &[String]) -> String {
    if cleanup.is_empty() {
        String::new()
    } else {
        format!(", except: {}", cleanup.join("; "))
    }
}

fn io_error(path: &Path) -> impl FnOnce(io::Error) -> ServiceError + '_ {
    move |source| ServiceError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// What [`install`] is asked to install.
pub struct Install<'a> {
    pub name: Name,
    /// The `dolos` binary the service runs, by absolute path.
    pub binary: PathBuf,
    /// The configuration file, whose directory becomes the working directory.
    pub config_file: PathBuf,
    /// The configuration, as loaded from `config_file`.
    pub config: &'a RootConfig,
    pub start: bool,
}

#[derive(Debug, Serialize)]
pub struct Installed {
    pub name: Name,
    pub identity: String,
    pub manager: Manager,
    pub scope: Scope,
    pub file: PathBuf,
    pub binary: PathBuf,
    pub config: PathBuf,
    pub working_directory: PathBuf,
    /// `storage.path`, against the working directory.
    pub storage: PathBuf,
    pub starts_at: Autostart,
    pub started: bool,
    pub commands: Commands,
    pub notes: Vec<String>,
    /// Installations that could not be checked for conflicts; for stderr.
    #[serde(skip)]
    pub warnings: Vec<String>,
}

impl fmt::Display for Installed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let started = if self.started {
            "started now"
        } else {
            "not started now"
        };

        writeln!(
            f,
            "installed {} as {} ({} {})",
            self.name, self.identity, self.manager, self.scope
        )?;
        writeln!(f, "file:    {}", self.file.display())?;
        writeln!(
            f,
            "runs:    {} --config {} daemon",
            shell_quote(&self.binary.to_string_lossy()),
            shell_quote(&self.config.to_string_lossy())
        )?;
        writeln!(f, "in:      {}", self.working_directory.display())?;
        writeln!(f, "storage: {}", self.storage.display())?;
        writeln!(f, "starts:  at {}; {started}", self.starts_at)?;
        write!(f, "\n{}", self.commands)?;

        for note in &self.notes {
            write!(f, "\n\n{note}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct Uninstalled {
    pub name: Name,
    pub identity: String,
    pub manager: Manager,
    pub scope: Scope,
    /// The service file, now removed.
    pub file: PathBuf,
    /// The configuration it ran, left in place with its data; `None` when the
    /// file could not be read back.
    pub config: Option<PathBuf>,
    pub commands: Commands,
}

impl fmt::Display for Uninstalled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "uninstalled {}: stopped {} ({} {}) and removed its file",
            self.name, self.identity, self.manager, self.scope
        )?;
        writeln!(f, "file:    {}", self.file.display())?;

        if let Some(config) = &self.config {
            writeln!(f, "kept:    {} and its data", config.display())?;
        }

        write!(f, "\n{}", self.commands)
    }
}

/// Installs `request` with `backend`, refusing a name the scope already has
/// and a configuration whose files or listeners another instance in the
/// scope uses. On a failure after writing, removes what it wrote.
pub fn install(
    backend: &dyn ServiceBackend,
    request: &Install<'_>,
) -> Result<Installed, ServiceError> {
    backend.check_access()?;

    // systemd refuses a working directory with `..`. Links stay, so that the
    // service follows one the operator moves.
    let config_file = without_dots(&request.config_file).map_err(io_error(&request.config_file))?;
    let working_dir = config_file
        .parent()
        .map_or_else(|| PathBuf::from("/"), Path::to_path_buf);

    let unit = Unit {
        name: request.name.clone(),
        binary: request.binary.clone(),
        config: config_file,
        working_dir,
    };
    let contents = backend.render(&unit)?;

    let dir = backend.dir();
    let lock = Lock::take(dir, PATIENCE, true)?
        .ok_or_else(|| io_error(dir)(io::ErrorKind::NotFound.into()))?;
    let installed = write_and_register(backend, request, &unit, &contents);

    if installed.is_err() {
        lock.undo();
    }

    installed
}

fn write_and_register(
    backend: &dyn ServiceBackend,
    request: &Install<'_>,
    unit: &Unit,
    contents: &str,
) -> Result<Installed, ServiceError> {
    let name = &request.name;
    let identity = backend.identity(name);
    let file = backend.file(name);

    if fs::symlink_metadata(&file).is_ok() {
        return Err(if ours(&file) {
            ServiceError::AlreadyInstalled { identity, file }
        } else {
            ServiceError::Foreign { identity, file }
        });
    }

    if backend.active(name) {
        return Err(ServiceError::Orphaned {
            identity,
            file,
            stop: backend.commands(name).stop,
        });
    }

    let ours = Resources::of(request.config, &unit.working_dir);
    let mut warnings = Vec::new();

    for (other, other_file) in installed(backend)? {
        let theirs = fs::read_to_string(&other_file)
            .map_err(|error| error.to_string())
            .and_then(|contents| {
                backend
                    .read(&contents)
                    .ok_or_else(|| "it does not say which configuration it runs".to_string())
            })
            .and_then(|recorded| {
                dolos_core::config::load_file(Some(&recorded.config))
                    .map(|config| Resources::of(&config, &recorded.working_dir))
                    .map_err(|error| error.to_string())
            });

        match theirs {
            Ok(theirs) => {
                if let Some(reason) = ours.clash(&theirs) {
                    return Err(ServiceError::Conflict {
                        identity: other,
                        file: other_file,
                        reason,
                    });
                }
            }
            Err(why) => warnings.push(format!(
                "{other} ({}) was not checked for conflicts: {why}",
                other_file.display()
            )),
        }
    }

    if let Err(error) = write_new(&file, contents) {
        return Err(match error.kind() {
            io::ErrorKind::AlreadyExists => ServiceError::AlreadyInstalled { identity, file },
            _ => io_error(&file)(error),
        });
    }

    if let Err(mut error) = backend.register(name, request.start) {
        if let Err(removing) = fs::remove_file(&file) {
            error
                .cleanup
                .push(format!("could not remove {}: {removing}", file.display()));
        }

        if error.loaded {
            if let Err(failure) = backend.forget() {
                error.cleanup.push(failure.to_string());
            }
        }

        return Err(ServiceError::Register {
            identity,
            failure: error.failure,
            cleanup: error.cleanup,
        });
    }

    Ok(Installed {
        name: name.clone(),
        identity,
        manager: backend.manager(),
        scope: backend.scope(),
        file,
        storage: unit.working_dir.join(&request.config.storage.path),
        binary: unit.binary.clone(),
        config: unit.config.clone(),
        working_directory: unit.working_dir.clone(),
        starts_at: backend.autostart(),
        started: request.start,
        commands: backend.commands(name),
        notes: backend.notes(name),
        warnings,
    })
}

/// Stops and unregisters `name`'s service and removes its file. The
/// configuration and data it ran with stay as they are, and so does a file of
/// the same name that Dolos did not write.
pub fn uninstall(backend: &dyn ServiceBackend, name: &Name) -> Result<Uninstalled, ServiceError> {
    backend.check_access()?;

    let identity = backend.identity(name);
    let file = backend.file(name);

    // Checked under the lock, which another uninstall may hold while it
    // removes this file.
    let lock = Lock::take(backend.dir(), PATIENCE, false)?;

    if lock.is_none() || fs::symlink_metadata(&file).is_err() {
        return Err(if backend.active(name) {
            ServiceError::Orphaned {
                identity,
                file,
                stop: backend.commands(name).stop,
            }
        } else {
            ServiceError::NotInstalled { identity, file }
        });
    }

    if !ours(&file) {
        return Err(ServiceError::Foreign { identity, file });
    }

    let recorded = fs::read_to_string(&file)
        .ok()
        .and_then(|contents| backend.read(&contents));

    backend
        .unregister(name)
        .map_err(|failure| ServiceError::Unregister {
            identity: identity.clone(),
            failure,
        })?;

    fs::remove_file(&file).map_err(io_error(&file))?;

    backend.forget().map_err(|failure| ServiceError::Forget {
        file: file.clone(),
        failure,
    })?;

    Ok(Uninstalled {
        name: name.clone(),
        identity,
        manager: backend.manager(),
        scope: backend.scope(),
        file,
        config: recorded.map(|recorded| recorded.config),
        commands: backend.commands(name),
    })
}

/// The Dolos services installed in `backend`'s scope, by identity, in file
/// name order.
fn installed(backend: &dyn ServiceBackend) -> Result<Vec<(String, PathBuf)>, ServiceError> {
    let dir = backend.dir();

    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io_error(dir)(error)),
    };

    let mut found = Vec::new();

    for entry in entries {
        let path = entry.map_err(io_error(dir))?.path();
        let identity = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| backend.identity_of(name));

        if let Some(identity) = identity {
            found.push((identity, path));
        }
    }

    found.sort_by(|a, b| a.1.cmp(&b.1));

    Ok(found)
}

/// Whether `file` is a service file Dolos wrote: a regular file carrying
/// [`MARKER`], unlike a mask or a hand-written unit of the same name.
fn ours(file: &Path) -> bool {
    fs::symlink_metadata(file).is_ok_and(|metadata| metadata.is_file())
        && fs::read_to_string(file).is_ok_and(|contents| contents.contains(MARKER))
}

/// How long an install or uninstall waits for another to release the scope.
/// Any user who can open the directory can lock it.
const PATIENCE: Duration = Duration::from_secs(30);

/// An exclusive lock on a scope's directory, held until dropped.
struct Lock {
    _handle: fs::File,
    /// The directories made to take it, outermost first.
    created: Vec<PathBuf>,
}

impl Lock {
    /// Waits up to `patience` for `dir`, made first with its missing
    /// ancestors when `create`; `None` when it is missing and not made.
    /// Failing, it leaves what it made: only the lock's holder removes them.
    fn take(dir: &Path, patience: Duration, create: bool) -> Result<Option<Self>, ServiceError> {
        let deadline = Instant::now() + patience;
        let mut created = Vec::new();

        loop {
            if create {
                created.extend(create_dirs(dir)?);
            }

            let handle = match fs::File::open(dir) {
                Ok(handle) => handle,
                Err(error) if error.kind() == io::ErrorKind::NotFound && !create => {
                    return Ok(None)
                }
                // Removed by a failed install since it was made.
                Err(error)
                    if error.kind() == io::ErrorKind::NotFound && Instant::now() < deadline =>
                {
                    continue
                }
                Err(error) => return Err(io_error(dir)(error)),
            };

            loop {
                match handle.try_lock() {
                    Ok(()) => break,
                    Err(fs::TryLockError::WouldBlock) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    Err(fs::TryLockError::WouldBlock) => {
                        return Err(ServiceError::Busy(dir.to_path_buf()))
                    }
                    Err(fs::TryLockError::Error(error)) => return Err(io_error(dir)(error)),
                }
            }

            // A failed install removes the directory it made, perhaps while
            // this one waited on it: then this lock holds nothing.
            if still_at(&handle, dir).map_err(io_error(dir))? {
                return Ok(Some(Self {
                    _handle: handle,
                    created,
                }));
            }
        }
    }

    /// Removes the directories it made, when they are still empty, and only
    /// then releases the lock: an install waiting on them makes them again.
    fn undo(self) {
        remove_dirs(&self.created);
    }
}

/// Whether `handle` is still the directory at `dir`.
#[cfg(unix)]
fn still_at(handle: &fs::File, dir: &Path) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt as _;

    let held = handle.metadata()?;

    match fs::metadata(dir) {
        Ok(current) => Ok(held.dev() == current.dev() && held.ino() == current.ino()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

#[cfg(not(unix))]
fn still_at(_handle: &fs::File, dir: &Path) -> io::Result<bool> {
    Ok(dir.is_dir())
}

/// Creates `dir` and its missing ancestors, returning those it created,
/// outermost first. None is writable by group or others, whatever the umask.
fn create_dirs(dir: &Path) -> Result<Vec<PathBuf>, ServiceError> {
    let missing: Vec<PathBuf> = dir
        .ancestors()
        .take_while(|ancestor| !ancestor.as_os_str().is_empty() && !ancestor.exists())
        .map(Path::to_path_buf)
        .collect();

    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);

    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o755);

    builder.create(dir).map_err(io_error(dir))?;

    Ok(missing.into_iter().rev().collect())
}

/// Removes the directories [`create_dirs`] created, when they are still
/// empty.
fn remove_dirs(created: &[PathBuf]) {
    for dir in created.iter().rev() {
        let _ = fs::remove_dir(dir);
    }
}

/// Writes a new `file`, which neither group nor others may write, whatever
/// the umask: launchd refuses such a file.
fn write_new(file: &Path, contents: &str) -> io::Result<()> {
    use io::Write as _;

    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);

    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o644);

    let mut handle = options.open(file)?;

    handle
        .write_all(contents.as_bytes())
        .and_then(|()| handle.sync_all())
        .inspect_err(|_| {
            let _ = fs::remove_file(file);
        })
}

/// `word` as one word for a POSIX shell.
pub fn shell_quote(word: &str) -> Cow<'_, str> {
    let plain = !word.is_empty()
        && word
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "/._-+,:=@%".contains(c));

    if plain {
        Cow::Borrowed(word)
    } else {
        Cow::Owned(format!("'{}'", word.replace('\'', r"'\''")))
    }
}

/// The `dolos` binary a `scope` service runs, by an absolute path naming it
/// as it was run: `invoked` (`argv[0]`), against `cwd` or found on `path`
/// (`PATH`), when it leads to `exe`, the running binary. Otherwise `exe`.
/// Linux reports the running binary through its symlinks, into a versioned
/// directory a package manager's upgrade removes (Homebrew's
/// `Cellar/dolos/<version>`), while the name it was run by stays.
///
/// A system service runs the binary by its canonical path instead: under
/// `sudo`, the name it was run by may be a link its invoker owns, which root
/// would then follow at every start.
pub fn locate_binary(
    scope: Scope,
    invoked: Option<&OsStr>,
    path: Option<&OsStr>,
    cwd: Option<&Path>,
    exe: &Path,
) -> io::Result<PathBuf> {
    let absolute = |path: &Path| match cwd {
        _ if path.is_absolute() => Some(path.to_path_buf()),
        Some(cwd) => Some(cwd.join(path)),
        None => None,
    };

    let exe = absolute(exe).ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
    let real = fs::canonicalize(&exe)?;

    if scope == Scope::System {
        return Ok(real);
    }

    let invoked = invoked
        .map(Path::new)
        .filter(|word| !word.as_os_str().is_empty());
    let candidates: Vec<PathBuf> = match invoked {
        Some(word) if word.parent().is_some_and(|dir| !dir.as_os_str().is_empty()) => {
            absolute(word).into_iter().collect()
        }
        Some(word) => path
            .map(|path| {
                std::env::split_paths(path)
                    .filter_map(|dir| absolute(&dir))
                    .map(|dir| dir.join(word))
                    .collect()
            })
            .unwrap_or_default(),
        None => Vec::new(),
    };

    let found = candidates
        .into_iter()
        .chain([exe])
        .filter_map(|candidate| without_dots(&candidate).ok())
        .find(|candidate| fs::canonicalize(candidate).is_ok_and(|found| found == real));

    Ok(found.unwrap_or(real))
}

/// `path`, absolute, without `.` and `..`: each `..` steps out of where the
/// part before it leads, as the kernel steps, and the rest stays as written,
/// symlinks included.
fn without_dots(path: &Path) -> io::Result<PathBuf> {
    let mut resolved = PathBuf::new();

    for component in std::path::absolute(path)?.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                resolved = resolved.canonicalize()?;
                resolved.pop();
            }
            other => resolved.push(other),
        }
    }

    Ok(resolved)
}

/// The user's home directory, under which user services live.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn home() -> Result<PathBuf, ServiceError> {
    std::env::var_os("HOME")
        .filter(|home| !home.is_empty())
        .map(PathBuf::from)
        .ok_or(ServiceError::NoHome)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn dots_go_as_the_kernel_resolves_them_and_links_stay() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join("real/x")).unwrap();
        std::fs::create_dir_all(root.join("other")).unwrap();
        std::os::unix::fs::symlink(root.join("real/x"), root.join("link")).unwrap();

        let resolve = |path: &str| without_dots(&root.join(path)).unwrap();

        assert_eq!(resolve("link/./dolos.toml"), root.join("link/dolos.toml"));
        assert_eq!(resolve("other/../link/x"), root.join("link/x"));
        // `link/..` is `real`, where the kernel steps out of the link's target.
        assert_eq!(resolve("link/../y"), root.join("real/y"));
        assert_eq!(resolve("link/../x/../../link"), root.join("link"));
    }

    #[test]
    fn a_held_scope_is_waited_for_then_given_up() {
        let dir = tempfile::tempdir().unwrap();
        let held = fs::File::open(dir.path()).unwrap();

        // An exclusive lock waits even for a shared one.
        held.lock_shared().unwrap();

        let started = Instant::now();
        let error = Lock::take(dir.path(), Duration::from_millis(300), false)
            .err()
            .unwrap();

        assert!(matches!(error, ServiceError::Busy(_)), "{error}");
        assert!(started.elapsed() >= Duration::from_millis(300));
        assert!(error.to_string().contains("30s"), "{error}");

        drop(held);
        assert!(Lock::take(dir.path(), Duration::ZERO, false)
            .unwrap()
            .is_some());
        assert!(
            Lock::take(&dir.path().join("missing"), Duration::ZERO, false)
                .unwrap()
                .is_none()
        );
    }
}
