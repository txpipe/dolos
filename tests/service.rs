//! `service install | uninstall` over fixture directories: systemd user and
//! system units and launchd agents, written under a temporary root, with a
//! command runner that records what it is asked to run instead of running it.
//! The command-line tests run the binary with a temporary home and a `PATH`
//! holding only recording stand-ins for `systemctl` and `launchctl`. Nothing
//! here registers a service with a live service manager.

#![cfg(unix)]

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Duration;

use dolos::service::{
    install, locate_binary, uninstall, CommandRunner, Install, Launchd, Name, Recorded,
    ServiceBackend, ServiceError, SystemRunner, Systemd, Unit,
};
use dolos_core::config::{load_file, Compatibility, RootConfig};

/// Records each command it is asked to run and fails those containing a
/// pattern in `fail`. Asked whether a service runs (`is-active`, `print`), it
/// says yes only for the identities in `active`.
#[derive(Default)]
struct Recorder {
    calls: RefCell<Vec<String>>,
    fail: RefCell<Vec<&'static str>>,
    active: RefCell<Vec<String>>,
}

impl Recorder {
    fn failing(pattern: &'static str) -> Self {
        let recorder = Self::default();
        recorder.fail_on(pattern);
        recorder
    }

    fn fail_on(&self, pattern: &'static str) {
        self.fail.borrow_mut().push(pattern);
    }

    /// Has the manager run the service `identity` names.
    fn activate(&self, identity: String) {
        self.active.borrow_mut().push(identity);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.borrow().clone()
    }
}

impl CommandRunner for Recorder {
    fn run(&self, program: &str, args: &[&str]) -> Result<(), String> {
        let call = std::iter::once(program)
            .chain(args.iter().copied())
            .collect::<Vec<_>>()
            .join(" ");
        self.calls.borrow_mut().push(call.clone());

        if args.contains(&"is-active") || args.first() == Some(&"print") {
            let active = self
                .active
                .borrow()
                .iter()
                .any(|identity| call.ends_with(identity.as_str()));

            return if active {
                Ok(())
            } else {
                Err("not running".into())
            };
        }

        match self
            .fail
            .borrow()
            .iter()
            .find(|pattern| call.contains(**pattern))
        {
            Some(pattern) => Err(format!("{pattern}: simulated failure")),
            None => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    SystemdUser,
    SystemdSystem,
    Launchd,
}

const KINDS: [Kind; 3] = [Kind::SystemdUser, Kind::SystemdSystem, Kind::Launchd];

const SYSTEMD: [Kind; 2] = [Kind::SystemdUser, Kind::SystemdSystem];

const UID: u32 = 501;

struct Instance {
    file: PathBuf,
    config: RootConfig,
}

/// A machine under a temporary root, by its canonical path: a user's home,
/// and one directory per instance under `srv/`.
struct Machine {
    _dir: tempfile::TempDir,
    root: PathBuf,
}

impl Machine {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join("home/operator")).unwrap();

        Self { _dir: dir, root }
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn home(&self) -> PathBuf {
        self.root.join("home/operator")
    }

    fn binary(&self) -> PathBuf {
        self.root.join("opt/dolos/bin/dolos")
    }

    fn backend<'a>(&self, kind: Kind, runner: &'a Recorder) -> Box<dyn ServiceBackend + 'a> {
        match kind {
            Kind::SystemdUser => Box::new(Systemd::user(&self.home(), runner)),
            Kind::SystemdSystem => Box::new(Systemd::system(&self.root, true, runner)),
            Kind::Launchd => Box::new(Launchd::new(&self.home(), UID, runner)),
        }
    }

    /// `srv/{dir}/dolos.toml` on preview, storing under `storage` and serving
    /// minibf on `port`, with a file in its storage directory.
    fn instance(&self, dir: &str, storage: &str, port: u16) -> Instance {
        self.instance_with(dir, storage, &config("preview", storage, port))
    }

    /// `srv/{dir}/dolos.toml` holding `text`, with a file in its storage
    /// directory, `storage`.
    fn instance_with(&self, dir: &str, storage: &str, text: &str) -> Instance {
        let dir = self.root.join("srv").join(dir);
        std::fs::create_dir_all(&dir).unwrap();

        let file = dir.join("dolos.toml");
        std::fs::write(&file, text).unwrap();

        let data = dir.join(storage);
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("chain.db"), dir.to_string_lossy().as_bytes()).unwrap();

        Instance {
            config: load_file(Some(&file)).unwrap(),
            file,
        }
    }

    fn request<'a>(&self, instance: &'a Instance, name: Option<&str>, start: bool) -> Install<'a> {
        let name = match name {
            Some(name) => Name::parse(name).unwrap(),
            None => Name::derive(&instance.config).unwrap(),
        };

        Install {
            name,
            binary: self.binary(),
            config_file: instance.file.clone(),
            config: &instance.config,
            start,
        }
    }

    /// Every file under the root, with its bytes.
    fn files(&self) -> BTreeMap<PathBuf, Vec<u8>> {
        files(&self.root)
    }
}

/// A configuration on `network`, storing under `storage` and serving minibf on
/// `port`.
fn config(network: &str, storage: &str, port: u16) -> String {
    format!(
        "compatibility = \"{}\"\n\n\
         [chain]\nprotocol = \"cardano\"\nnetwork = \"{network}\"\n\n\
         [storage]\npath = \"{storage}\"\n\n\
         [serve.minibf]\nlisten_address = \"0.0.0.0:{port}\"\n",
        Compatibility::current()
    )
}

fn files(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut found = BTreeMap::new();
    let mut pending = vec![dir.to_path_buf()];

    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };

        for entry in entries {
            let path = entry.unwrap().path();

            if path.is_dir() {
                pending.push(path);
            } else {
                found.insert(path.clone(), std::fs::read(&path).unwrap());
            }
        }
    }

    found
}

fn name(value: &str) -> Name {
    Name::parse(value).unwrap()
}

/// What `kind` calls `name`'s service.
fn identity(kind: Kind, name: &str) -> String {
    match kind {
        Kind::Launchd => format!("io.txpipe.dolos.{name}"),
        _ => format!("dolos-{name}.service"),
    }
}

fn systemctl(kind: Kind) -> &'static str {
    match kind {
        Kind::SystemdUser => "systemctl --user",
        _ => "systemctl",
    }
}

/// Asks whether `name`'s service runs.
fn query(kind: Kind, name: &str) -> String {
    match kind {
        Kind::Launchd => format!("launchctl print gui/{UID}/io.txpipe.dolos.{name}"),
        _ => format!("{} is-active --quiet dolos-{name}.service", systemctl(kind)),
    }
}

/// What installing `name` runs, without `--start`.
fn registration(kind: Kind, name: &str) -> Vec<String> {
    let mut calls = vec![query(kind, name)];

    if kind != Kind::Launchd {
        calls.push(format!("{} daemon-reload", systemctl(kind)));
        calls.push(format!("{} enable dolos-{name}.service", systemctl(kind)));
    }

    calls
}

/// What uninstalling `name` runs; launchd boots an agent out only when it is
/// loaded.
fn unregistration(kind: Kind, name: &str, loaded: bool) -> Vec<String> {
    match kind {
        Kind::Launchd => {
            let mut calls = vec![query(kind, name)];

            if loaded {
                calls.push(format!(
                    "launchctl bootout gui/{UID}/io.txpipe.dolos.{name}"
                ));
            }

            calls
        }
        _ => vec![
            format!("{} disable --now dolos-{name}.service", systemctl(kind)),
            format!("{} daemon-reload", systemctl(kind)),
        ],
    }
}

/// Installs `installed`, each by its name, then refuses `candidate`: the
/// conflicting unit and the reason, after checking nothing changed.
fn refusal(
    kind: Kind,
    machine: &Machine,
    installed: &[(&Instance, &str)],
    candidate: (&Instance, &str),
) -> (String, String) {
    let runner = Recorder::default();
    let backend = machine.backend(kind, &runner);

    for (instance, name) in installed {
        install(
            backend.as_ref(),
            &machine.request(instance, Some(name), false),
        )
        .unwrap();
    }

    let before = machine.files();
    let calls = runner.calls();

    let error = install(
        backend.as_ref(),
        &machine.request(candidate.0, Some(candidate.1), false),
    )
    .unwrap_err();

    let message = error.to_string();
    let ServiceError::Conflict {
        identity, reason, ..
    } = error
    else {
        panic!("{kind:?}: {message}");
    };

    assert!(message.contains(&identity), "{message}");
    assert_eq!(machine.files(), before, "{kind:?}");
    assert_eq!(
        runner.calls()[calls.len()..],
        [query(kind, candidate.1)],
        "{kind:?}"
    );

    (identity, reason)
}

#[test]
fn names_are_validated_and_derived() {
    for valid in ["cardano-mainnet", "a", "0-analytics-", &"x".repeat(48)] {
        assert!(Name::parse(valid).is_ok(), "{valid}");
    }

    for invalid in ["", "Mainnet", "my_node", "a.b", "a b", &"x".repeat(49)] {
        assert!(
            matches!(Name::parse(invalid), Err(ServiceError::InvalidName(_))),
            "{invalid}"
        );
    }

    let machine = Machine::new();
    let preview = machine.instance("preview", "data", 3000);

    assert_eq!(
        Name::derive(&preview.config).unwrap().as_str(),
        "cardano-preview"
    );

    // A known network loads in any case, and is named as the catalog spells it.
    let mixed = machine.instance_with("mixed", "data", &config("PreProd", "data", 3000));

    assert_eq!(
        Name::derive(&mixed.config).unwrap().as_str(),
        "cardano-preprod"
    );
}

#[test]
fn two_instances_install_side_by_side() {
    for kind in KINDS {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let analytics = machine.instance("analytics", "data", 3001);
        let runner = Recorder::default();
        let backend = machine.backend(kind, &runner);

        let derived = install(backend.as_ref(), &machine.request(&preview, None, false)).unwrap();
        let explicit = install(
            backend.as_ref(),
            &machine.request(&analytics, Some("analytics"), false),
        )
        .unwrap();

        let (dir, derived_file, explicit_file) = match kind {
            Kind::SystemdUser => (
                machine.home().join(".config/systemd/user"),
                "dolos-cardano-preview.service",
                "dolos-analytics.service",
            ),
            Kind::SystemdSystem => (
                machine.root().join("etc/systemd/system"),
                "dolos-cardano-preview.service",
                "dolos-analytics.service",
            ),
            Kind::Launchd => (
                machine.home().join("Library/LaunchAgents"),
                "io.txpipe.dolos.cardano-preview.plist",
                "io.txpipe.dolos.analytics.plist",
            ),
        };

        assert_eq!(derived.name.as_str(), "cardano-preview", "{kind:?}");
        assert_eq!(explicit.name.as_str(), "analytics", "{kind:?}");
        assert_eq!(derived.file, dir.join(derived_file), "{kind:?}");
        assert_eq!(explicit.file, dir.join(explicit_file), "{kind:?}");
        assert!(
            derived.file.is_file() && explicit.file.is_file(),
            "{kind:?}"
        );
        assert!(derived.warnings.is_empty() && explicit.warnings.is_empty());
        assert!(!derived.started);

        let mut expected = registration(kind, "cardano-preview");
        expected.extend(registration(kind, "analytics"));
        assert_eq!(runner.calls(), expected, "{kind:?}");
    }
}

#[test]
fn start_also_starts_it() {
    for kind in KINDS {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let runner = Recorder::default();
        let backend = machine.backend(kind, &runner);

        let installed = install(backend.as_ref(), &machine.request(&preview, None, true)).unwrap();

        let mut expected = registration(kind, "cardano-preview");
        expected.push(match kind {
            Kind::Launchd => format!("launchctl bootstrap gui/{UID} {}", installed.file.display()),
            _ => format!("{} start dolos-cardano-preview.service", systemctl(kind)),
        });

        assert!(installed.started);
        assert_eq!(runner.calls(), expected, "{kind:?}");
    }
}

#[test]
fn the_unit_runs_absolute_paths_in_the_config_directory() {
    for kind in KINDS {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let runner = Recorder::default();
        let backend = machine.backend(kind, &runner);

        let installed = install(backend.as_ref(), &machine.request(&preview, None, false)).unwrap();
        let contents = std::fs::read_to_string(&installed.file).unwrap();
        let dir = preview.file.parent().unwrap();

        assert!(installed.binary.is_absolute() && installed.config.is_absolute());
        assert_eq!(installed.binary, machine.binary());
        assert_eq!(installed.config, preview.file);
        assert_eq!(installed.working_directory, dir);
        assert_eq!(installed.storage, dir.join("data"));

        assert!(
            contents.contains(&*machine.binary().to_string_lossy()),
            "{contents}"
        );
        assert!(
            contents.contains(&*preview.file.to_string_lossy()),
            "{contents}"
        );
        assert_eq!(
            backend.read(&contents),
            Some(Recorded {
                config: preview.file.clone(),
                working_dir: dir.to_path_buf(),
            }),
            "{kind:?}"
        );
    }
}

/// systemd refuses a working directory with `..`, and the daemon resolves
/// `..` in its storage path from where a symlink leads, so the unit records
/// the configuration by its canonical path.
#[test]
fn the_configuration_is_recorded_by_its_canonical_path() {
    let machine = Machine::new();
    let preview = machine.instance("preview", "data", 3000);
    let dir = preview.file.parent().unwrap();

    std::fs::create_dir_all(machine.root().join("srv/other")).unwrap();
    std::os::unix::fs::symlink(dir, machine.root().join("srv/linked")).unwrap();

    // Relative to this process's working directory, as `--config` may be.
    let cwd = std::env::current_dir().unwrap().canonicalize().unwrap();
    let relative = cwd
        .components()
        .skip(1)
        .map(|_| "..")
        .collect::<PathBuf>()
        .join(preview.file.strip_prefix("/").unwrap());

    for config_file in [
        machine.root().join("srv/other/../preview/dolos.toml"),
        machine.root().join("srv/linked/dolos.toml"),
        relative,
    ] {
        for kind in KINDS {
            let runner = Recorder::default();
            let backend = machine.backend(kind, &runner);
            let request = Install {
                config_file: config_file.clone(),
                ..machine.request(&preview, None, false)
            };

            let installed = install(backend.as_ref(), &request).unwrap();
            let contents = std::fs::read_to_string(&installed.file).unwrap();

            assert_eq!(installed.config, preview.file, "{kind:?}: {config_file:?}");
            assert_eq!(installed.working_directory, dir, "{kind:?}");
            assert!(
                !contents.contains("..") && !contents.contains("linked"),
                "{kind:?}: {contents}"
            );

            uninstall(backend.as_ref(), &installed.name).unwrap();
        }
    }
}

#[test]
fn paths_needing_escapes_read_back_as_written() {
    let machine = Machine::new();
    let runner = Recorder::default();

    let unit = Unit {
        name: name("odd"),
        binary: PathBuf::from("/opt/my dolos/bin/dolos"),
        config: PathBuf::from("/srv/100% \"odd\" & <x>/$USER/dolos.toml"),
        working_dir: PathBuf::from("/srv/100% \"odd\" & <x>/$USER"),
    };

    for kind in KINDS {
        let backend = machine.backend(kind, &runner);
        let contents = backend.render(&unit).unwrap();

        match kind {
            Kind::Launchd => {
                assert!(
                    contents.contains("<string>/srv/100% \"odd\" &amp; &lt;x&gt;/$USER</string>"),
                    "{contents}"
                );
                assert!(!contents.contains("& <x>"), "{contents}");
            }
            _ => {
                assert!(
                    contents.contains("WorkingDirectory=/srv/100%% \"odd\" & <x>/$USER\n"),
                    "{contents}"
                );
                assert!(
                    contents.contains(
                        r#"ExecStart="/opt/my dolos/bin/dolos" --config "/srv/100%% \"odd\" & <x>/$$USER/dolos.toml" daemon"#
                    ),
                    "{contents}"
                );
            }
        }

        assert_eq!(
            backend.read(&contents),
            Some(Recorded {
                config: unit.config.clone(),
                working_dir: unit.working_dir.clone(),
            }),
            "{kind:?}: {contents}"
        );
    }

    let unit = Unit {
        working_dir: PathBuf::from("/srv/line\nbreak"),
        ..unit
    };

    assert!(matches!(
        machine.backend(Kind::SystemdUser, &runner).render(&unit),
        Err(ServiceError::UnsupportedPath(_))
    ));
}

#[cfg(target_os = "macos")]
#[test]
fn rendered_plists_lint() {
    let machine = Machine::new();
    let runner = Recorder::default();
    let backend = machine.backend(Kind::Launchd, &runner);

    let unit = Unit {
        name: name("odd"),
        binary: PathBuf::from("/opt/my dolos/bin/dolos"),
        config: PathBuf::from("/srv/100% \"odd\" & <x>/$USER/dolos.toml"),
        working_dir: PathBuf::from("/srv/100% \"odd\" & <x>/$USER"),
    };
    let file = machine.root().join("odd.plist");
    std::fs::write(&file, backend.render(&unit).unwrap()).unwrap();

    let lint = Command::new("plutil")
        .arg("-lint")
        .arg(&file)
        .output()
        .unwrap();

    assert!(lint.status.success(), "{lint:?}");
}

/// systemd reads a `WorkingDirectory=` value to the end of its line,
/// unquoted: it drops a trailing space and joins the next line after a
/// trailing `\`.
#[test]
fn a_working_directory_systemd_would_misread_is_refused() {
    let machine = Machine::new();
    let runner = Recorder::default();

    for dir in ["/srv/dolos\\", "/srv/dolos "] {
        let unit = Unit {
            name: name("odd"),
            binary: PathBuf::from("/usr/local/bin/dolos"),
            config: Path::new(dir).join("dolos.toml"),
            working_dir: PathBuf::from(dir),
        };

        for kind in SYSTEMD {
            assert!(
                matches!(
                    machine.backend(kind, &runner).render(&unit),
                    Err(ServiceError::UnsupportedPath(_))
                ),
                "{kind:?}: {dir:?}"
            );
        }

        let launchd = machine.backend(Kind::Launchd, &runner);
        let contents = launchd.render(&unit).unwrap();

        assert_eq!(
            launchd.read(&contents),
            Some(Recorded {
                config: unit.config.clone(),
                working_dir: unit.working_dir.clone(),
            })
        );
    }
}

#[test]
fn rendered_units() {
    let runner = Recorder::default();
    let unit = Unit {
        name: name("cardano-preview"),
        binary: PathBuf::from("/usr/local/bin/dolos"),
        config: PathBuf::from("/srv/dolos/preview/dolos.toml"),
        working_dir: PathBuf::from("/srv/dolos/preview"),
    };

    let service = |wants: &str, wanted_by: &str| {
        format!(
            "# Written by `dolos service install`. To change it, uninstall and install again.\n\
             [Unit]\n\
             Description=Dolos (cardano-preview)\n\
             {wants}\n\
             [Service]\n\
             WorkingDirectory=/srv/dolos/preview\n\
             ExecStart=/usr/local/bin/dolos --config /srv/dolos/preview/dolos.toml daemon\n\
             Restart=on-failure\n\
             RestartSec=5\n\
             \n\
             [Install]\n\
             WantedBy={wanted_by}\n"
        )
    };

    assert_eq!(
        Systemd::user(Path::new("/home/operator"), &runner)
            .render(&unit)
            .unwrap(),
        service("", "default.target")
    );
    assert_eq!(
        Systemd::system(Path::new("/"), true, &runner)
            .render(&unit)
            .unwrap(),
        service(
            "After=network-online.target\nWants=network-online.target\n",
            "multi-user.target"
        )
    );
    assert_eq!(
        Launchd::new(Path::new("/Users/operator"), UID, &runner)
            .render(&unit)
            .unwrap(),
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<!-- Written by `dolos service install`. To change it, uninstall and install again. -->
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>io.txpipe.dolos.cardano-preview</string>
	<key>ProgramArguments</key>
	<array>
		<string>/usr/local/bin/dolos</string>
		<string>--config</string>
		<string>/srv/dolos/preview/dolos.toml</string>
		<string>daemon</string>
	</array>
	<key>WorkingDirectory</key>
	<string>/srv/dolos/preview</string>
	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<dict>
		<key>SuccessfulExit</key>
		<false/>
	</dict>
	<key>StandardOutPath</key>
	<string>/Users/operator/Library/Logs/dolos-cardano-preview.log</string>
	<key>StandardErrorPath</key>
	<string>/Users/operator/Library/Logs/dolos-cardano-preview.log</string>
</dict>
</plist>
"#
    );
}

/// The configuration a unit edited by hand runs, as the manager and `dolos`
/// read it.
#[test]
fn hand_edited_units_read_back() {
    let runner = Recorder::default();
    let user = Systemd::user(Path::new("/home/operator"), &runner);
    let system = Systemd::system(Path::new("/"), true, &runner);
    let launchd = Launchd::new(Path::new("/Users/operator"), UID, &runner);

    let unit = |working_dir: Option<&str>, exec: &str| {
        let working_dir = working_dir
            .map(|dir| format!("WorkingDirectory={dir}\n"))
            .unwrap_or_default();

        format!("[Service]\n{working_dir}ExecStart={exec}\n")
    };
    let recorded = |config: &str, working_dir: &str| {
        Some(Recorded {
            config: config.into(),
            working_dir: working_dir.into(),
        })
    };

    assert_eq!(
        user.read(&unit(
            Some("/srv/a"),
            "/usr/bin/dolos --config=/etc/dolos/a.toml daemon"
        )),
        recorded("/etc/dolos/a.toml", "/srv/a")
    );
    assert_eq!(
        user.read(&unit(Some("/srv/a"), "/usr/bin/dolos -c a.toml daemon")),
        recorded("/srv/a/a.toml", "/srv/a")
    );
    assert_eq!(
        user.read(&unit(Some("-/srv/a"), "/usr/bin/dolos -ca.toml daemon")),
        recorded("/srv/a/a.toml", "/srv/a")
    );
    assert_eq!(
        user.read(&unit(Some("~"), "/usr/bin/dolos -c=a.toml daemon")),
        recorded("/home/operator/a.toml", "/home/operator")
    );
    assert_eq!(
        user.read(&unit(None, "/usr/bin/dolos daemon")),
        recorded("/home/operator/dolos.toml", "/home/operator")
    );
    assert_eq!(
        system.read(&unit(None, "-/usr/bin/dolos daemon")),
        recorded("/dolos.toml", "/")
    );
    assert_eq!(system.read("[Service]\nWorkingDirectory=/srv/a\n"), None);

    assert_eq!(
        launchd.read(
            "<plist><dict><key>ProgramArguments</key><array>\
             <string>/usr/bin/dolos</string><string>daemon</string>\
             </array></dict></plist>"
        ),
        recorded("/dolos.toml", "/")
    );
}

#[test]
fn a_name_in_use_is_refused_and_nothing_changes() {
    for kind in KINDS {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let other = machine.instance("other", "data", 3001);
        let runner = Recorder::default();
        let backend = machine.backend(kind, &runner);

        install(backend.as_ref(), &machine.request(&preview, None, false)).unwrap();
        let before = machine.files();
        let calls = runner.calls();

        // Again, and over another configuration: the name decides, before
        // the configuration is compared with the installed one.
        for instance in [&preview, &other] {
            let error =
                install(backend.as_ref(), &machine.request(instance, None, true)).unwrap_err();

            assert!(
                matches!(&error, ServiceError::AlreadyInstalled { identity, .. }
                    if *identity == backend.identity(&name("cardano-preview"))),
                "{kind:?}: {error}"
            );
        }

        assert_eq!(machine.files(), before, "{kind:?}");
        assert_eq!(runner.calls(), calls, "{kind:?}");
    }
}

/// A mask (a link to `/dev/null`) or a unit written by hand under a name Dolos
/// would use is neither replaced nor removed.
#[test]
fn a_file_dolos_did_not_write_is_left_alone() {
    for kind in KINDS {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let runner = Recorder::default();
        let backend = machine.backend(kind, &runner);

        std::fs::create_dir_all(backend.dir()).unwrap();
        let mask = backend.file(&name("cardano-preview"));
        std::os::unix::fs::symlink("/dev/null", &mask).unwrap();
        std::fs::write(
            backend.file(&name("by-hand")),
            "[Service]\nExecStart=/usr/local/bin/dolos daemon\n",
        )
        .unwrap();
        let before = machine.files();

        let error = install(backend.as_ref(), &machine.request(&preview, None, true)).unwrap_err();
        assert!(
            matches!(error, ServiceError::Foreign { .. }),
            "{kind:?}: {error}"
        );

        for foreign in ["cardano-preview", "by-hand"] {
            let error = uninstall(backend.as_ref(), &name(foreign)).unwrap_err();
            assert!(
                matches!(error, ServiceError::Foreign { .. }),
                "{kind:?}: {error}"
            );
        }

        assert!(mask.is_symlink(), "{kind:?}");
        assert_eq!(machine.files(), before, "{kind:?}");
        assert!(runner.calls().is_empty(), "{:?}", runner.calls());
    }
}

/// A service whose file was removed by hand still runs; installing over it
/// would leave the old process running, and only the manager can stop it.
#[test]
fn a_service_running_without_its_file_is_reported() {
    for kind in KINDS {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let runner = Recorder::default();
        let backend = machine.backend(kind, &runner);
        let name = name("cardano-preview");

        runner.activate(backend.identity(&name));
        let before = machine.files();

        let error = install(backend.as_ref(), &machine.request(&preview, None, true)).unwrap_err();
        assert!(
            matches!(&error, ServiceError::Orphaned { stop, .. }
                if *stop == backend.commands(&name).stop),
            "{kind:?}: {error}"
        );
        assert!(error.to_string().contains(&backend.commands(&name).stop));

        let error = uninstall(backend.as_ref(), &name).unwrap_err();
        assert!(
            matches!(&error, ServiceError::Orphaned { stop, .. }
                if *stop == backend.commands(&name).stop),
            "{kind:?}: {error}"
        );

        assert_eq!(machine.files(), before, "{kind:?}");
        assert!(!backend.dir().exists(), "{kind:?}");
        assert_eq!(
            runner.calls(),
            [
                query(kind, "cardano-preview"),
                query(kind, "cardano-preview")
            ],
            "{kind:?}"
        );
    }
}

#[test]
fn overlapping_storage_is_refused_either_way() {
    for kind in KINDS {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let inside = machine.instance("inside", "../preview/data/archive", 3001);
        let around = machine.instance("around", "../preview", 3002);

        let (identity, reason) = refusal(
            kind,
            &machine,
            &[(&preview, "preview")],
            (&inside, "inside"),
        );
        assert_eq!(identity, self::identity(kind, "preview"));
        assert!(reason.contains("storage.path"), "{reason}");

        let (identity, reason) = refusal(kind, &machine, &[], (&around, "around"));
        assert_eq!(identity, self::identity(kind, "preview"));
        assert!(reason.contains("storage.path"), "{reason}");
    }
}

#[test]
fn a_store_outside_the_storage_path_counts() {
    for kind in KINDS {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let state = machine.root().join("srv/preview/data/state");
        let elsewhere = machine.instance_with(
            "elsewhere",
            "data",
            &format!(
                "{}\n[storage.state]\npath = \"{}\"\n",
                config("preview", "data", 3001),
                state.display()
            ),
        );

        let (identity, reason) = refusal(
            kind,
            &machine,
            &[(&preview, "preview")],
            (&elsewhere, "elsewhere"),
        );

        assert_eq!(identity, self::identity(kind, "preview"));
        assert!(
            reason.starts_with("storage.state.path") && reason.contains("its storage.path"),
            "{reason}"
        );
    }
}

#[test]
fn a_shared_port_is_refused_across_listeners() {
    for kind in KINDS {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let same_port = machine.instance("same-port", "data", 3000);
        let trp = machine.instance_with(
            "trp",
            "data",
            &format!(
                "{}\n[serve.trp]\nlisten_address = \"127.0.0.1:3000\"\n",
                config("preview", "data", 3001)
            ),
        );

        let (identity, reason) = refusal(
            kind,
            &machine,
            &[(&preview, "preview")],
            (&same_port, "same-port"),
        );
        assert_eq!(identity, self::identity(kind, "preview"));
        assert!(reason.contains("serve.minibf.listen_address"), "{reason}");

        let (identity, reason) = refusal(kind, &machine, &[], (&trp, "trp"));
        assert_eq!(identity, self::identity(kind, "preview"));
        assert!(
            reason.starts_with("serve.trp.listen_address")
                && reason.contains("its serve.minibf.listen_address"),
            "{reason}"
        );
    }
}

#[test]
fn a_shared_socket_is_refused() {
    for kind in KINDS {
        let machine = Machine::new();
        let socket = |port| {
            format!(
                "{}\n[serve.ouroboros]\nlisten_path = \"../dolos.socket\"\n",
                config("preview", "data", port)
            )
        };
        let a = machine.instance_with("a", "data", &socket(3000));
        let b = machine.instance_with("b", "data", &socket(3001));

        let (identity, reason) = refusal(kind, &machine, &[(&a, "a")], (&b, "b"));

        assert_eq!(identity, self::identity(kind, "a"));
        assert!(reason.contains("serve.ouroboros.listen_path"), "{reason}");
    }
}

#[test]
fn every_neighbour_is_checked() {
    for kind in KINDS {
        let machine = Machine::new();
        let a = machine.instance("a", "data", 3000);
        let b = machine.instance("b", "data", 3001);
        let c = machine.instance("c", "data", 3001);

        let (identity, _) = refusal(kind, &machine, &[(&a, "a"), (&b, "b")], (&c, "c"));

        assert_eq!(identity, self::identity(kind, "b"));
    }
}

/// A unit written by hand may name its directory through a symlink; its
/// `../data` is then next to the link's target, where the daemon writes.
#[test]
fn a_neighbours_paths_resolve_through_symlinks() {
    for kind in KINDS {
        let machine = Machine::new();
        let linked = machine.instance("real/x", "../data", 3000);
        let link = machine.root().join("srv/link");
        std::os::unix::fs::symlink(linked.file.parent().unwrap(), &link).unwrap();

        let runner = Recorder::default();
        let backend = machine.backend(kind, &runner);
        let unit = Unit {
            name: name("linked"),
            binary: machine.binary(),
            config: link.join("dolos.toml"),
            working_dir: link.clone(),
        };
        std::fs::create_dir_all(backend.dir()).unwrap();
        std::fs::write(backend.file(&unit.name), backend.render(&unit).unwrap()).unwrap();

        // `srv/data`, which `link/../data` spells, is apart from it.
        let apart = machine.instance("apart", "../data", 3001);
        install(
            backend.as_ref(),
            &machine.request(&apart, Some("apart"), false),
        )
        .unwrap();

        // `srv/real/data` is its store.
        let shared = machine.instance("real/y", "../data", 3002);
        let (identity, reason) = refusal(kind, &machine, &[], (&shared, "shared"));

        assert_eq!(identity, self::identity(kind, "linked"));
        assert!(reason.contains("storage.path"), "{reason}");
    }
}

#[test]
fn an_unreadable_neighbour_is_a_warning() {
    for kind in KINDS {
        let machine = Machine::new();
        let gone = machine.instance("gone", "data", 3000);
        let preview = machine.instance("preview", "data", 3000);
        let runner = Recorder::default();
        let backend = machine.backend(kind, &runner);

        install(
            backend.as_ref(),
            &machine.request(&gone, Some("gone"), false),
        )
        .unwrap();
        std::fs::remove_file(&gone.file).unwrap();

        let installed = install(backend.as_ref(), &machine.request(&preview, None, false)).unwrap();

        assert_eq!(installed.warnings.len(), 1, "{kind:?}");
        assert!(
            installed.warnings[0].contains(&backend.identity(&name("gone"))),
            "{:?}",
            installed.warnings
        );
    }
}

/// An install holds the scope from its conflict check to its write, so one
/// running beside it checks against what the other wrote.
#[test]
fn concurrent_installs_take_turns() {
    for kind in KINDS {
        let machine = Machine::new();
        let first = machine.instance("first", "data", 3000);
        let second = machine.instance("second", "data", 3000);
        let dir = machine
            .backend(kind, &Recorder::default())
            .dir()
            .to_path_buf();
        std::fs::create_dir_all(&dir).unwrap();

        let held = std::fs::File::open(&dir).unwrap();
        held.lock().unwrap();

        let (sender, receiver) = mpsc::channel();

        std::thread::scope(|scope| {
            scope.spawn(|| {
                let runner = Recorder::default();
                let backend = machine.backend(kind, &runner);
                let result = install(
                    backend.as_ref(),
                    &machine.request(&second, Some("second"), false),
                );

                sender.send(result.map(|_| ())).unwrap();
            });

            assert!(
                matches!(
                    receiver.recv_timeout(Duration::from_millis(500)),
                    Err(RecvTimeoutError::Timeout)
                ),
                "{kind:?}: the install did not wait for the scope"
            );

            // What the holder installs meanwhile.
            let runner = Recorder::default();
            let backend = machine.backend(kind, &runner);
            let unit = Unit {
                name: name("first"),
                binary: machine.binary(),
                config: first.file.clone(),
                working_dir: first.file.parent().unwrap().to_path_buf(),
            };
            std::fs::write(backend.file(&unit.name), backend.render(&unit).unwrap()).unwrap();
            drop(held);

            let error = receiver.recv().unwrap().unwrap_err();
            assert!(
                matches!(&error, ServiceError::Conflict { identity, .. }
                    if *identity == self::identity(kind, "first")),
                "{kind:?}: {error}"
            );
        });
    }
}

#[test]
fn explicit_name_uninstall_needs_no_configuration() {
    for kind in KINDS {
        let machine = Machine::new();
        let analytics = machine.instance("analytics", "data", 3000);
        let runner = Recorder::default();
        let backend = machine.backend(kind, &runner);

        let installed = install(
            backend.as_ref(),
            &machine.request(&analytics, Some("analytics"), true),
        )
        .unwrap();
        runner.activate(backend.identity(&installed.name));
        std::fs::remove_dir_all(machine.root().join("srv")).unwrap();
        let installing = runner.calls().len();

        let uninstalled = uninstall(backend.as_ref(), &name("analytics")).unwrap();

        assert_eq!(uninstalled.file, installed.file);
        assert_eq!(uninstalled.config.as_ref(), Some(&analytics.file));
        assert!(!installed.file.exists(), "{kind:?}");
        assert_eq!(
            runner.calls()[installing..],
            unregistration(kind, "analytics", true),
            "{kind:?}"
        );
    }
}

/// Without `--start`, a launchd agent was never loaded, and there is nothing
/// to boot out.
#[test]
fn derived_name_uninstall_keeps_the_other_instance_and_all_data() {
    for kind in KINDS {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let analytics = machine.instance("analytics", "data", 3001);
        let runner = Recorder::default();
        let backend = machine.backend(kind, &runner);

        let derived = install(backend.as_ref(), &machine.request(&preview, None, false)).unwrap();
        let explicit = install(
            backend.as_ref(),
            &machine.request(&analytics, Some("analytics"), false),
        )
        .unwrap();

        let srv = machine.root().join("srv");
        let data_and_config = files(&srv);
        let other = std::fs::read(&explicit.file).unwrap();
        let installing = runner.calls().len();

        let name = Name::derive(&preview.config).unwrap();
        uninstall(backend.as_ref(), &name).unwrap();

        assert!(!derived.file.exists(), "{kind:?}");
        assert_eq!(std::fs::read(&explicit.file).unwrap(), other, "{kind:?}");
        assert_eq!(files(&srv), data_and_config, "{kind:?}");
        assert!(data_and_config.contains_key(&preview.file));
        assert!(data_and_config.contains_key(&srv.join("preview/data/chain.db")));
        assert_eq!(
            runner.calls()[installing..],
            unregistration(kind, "cardano-preview", false),
            "{kind:?}"
        );
    }
}

#[test]
fn uninstalling_what_is_not_installed_fails() {
    for kind in KINDS {
        let machine = Machine::new();
        let runner = Recorder::default();
        let backend = machine.backend(kind, &runner);

        let error = uninstall(backend.as_ref(), &name("absent")).unwrap_err();

        assert!(
            matches!(error, ServiceError::NotInstalled { .. }),
            "{kind:?}: {error}"
        );
        assert_eq!(runner.calls(), [query(kind, "absent")], "{kind:?}");
    }
}

#[test]
fn a_failing_daemon_reload_leaves_no_file_behind() {
    for kind in SYSTEMD {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let runner = Recorder::failing("daemon-reload");
        let backend = machine.backend(kind, &runner);
        let before = machine.files();

        let error = install(backend.as_ref(), &machine.request(&preview, None, false)).unwrap_err();

        let ServiceError::Register { failure, .. } = &error else {
            panic!("{kind:?}: {error}");
        };
        assert_eq!(
            failure.command,
            format!("{} daemon-reload", systemctl(kind))
        );
        assert_eq!(machine.files(), before, "{kind:?}");
        assert!(
            !backend.dir().exists(),
            "{kind:?}: the unit directory stays"
        );
        assert_eq!(
            runner.calls(),
            registration(kind, "cardano-preview")[..2],
            "{kind:?}"
        );
    }
}

/// systemd has read the unit by then, and must read that it is gone.
#[test]
fn a_failing_enable_reloads_after_removing_the_file() {
    for kind in SYSTEMD {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let runner = Recorder::failing(" enable ");
        let backend = machine.backend(kind, &runner);
        let before = machine.files();

        let error = install(backend.as_ref(), &machine.request(&preview, None, false)).unwrap_err();

        assert!(
            matches!(&error, ServiceError::Register { failure, cleanup, .. }
                if failure.command.ends_with("enable dolos-cardano-preview.service")
                    && cleanup.is_empty()),
            "{kind:?}: {error}"
        );
        assert_eq!(machine.files(), before, "{kind:?}");

        let mut expected = registration(kind, "cardano-preview");
        expected.push(format!("{} daemon-reload", systemctl(kind)));
        assert_eq!(runner.calls(), expected, "{kind:?}");
    }
}

#[test]
fn a_failing_start_is_undone() {
    for kind in KINDS {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let fail = match kind {
            Kind::Launchd => "bootstrap",
            _ => " start ",
        };
        let runner = Recorder::failing(fail);
        let backend = machine.backend(kind, &runner);
        let before = machine.files();

        let error = install(backend.as_ref(), &machine.request(&preview, None, true)).unwrap_err();

        assert!(
            matches!(&error, ServiceError::Register { cleanup, .. } if cleanup.is_empty()),
            "{kind:?}: {error}"
        );
        assert_eq!(machine.files(), before, "{kind:?}");
        assert!(!backend.dir().exists(), "{kind:?}");

        let unit = "dolos-cardano-preview.service";
        let mut expected = registration(kind, "cardano-preview");
        expected.extend(match kind {
            Kind::Launchd => vec![format!(
                "launchctl bootstrap gui/{UID} {}",
                backend.file(&name("cardano-preview")).display()
            )],
            _ => vec![
                format!("{} start {unit}", systemctl(kind)),
                format!("{} disable --now {unit}", systemctl(kind)),
                format!("{} daemon-reload", systemctl(kind)),
            ],
        });
        assert_eq!(runner.calls(), expected, "{kind:?}");
    }
}

#[test]
fn what_undoing_a_failed_start_leaves_is_reported() {
    for kind in SYSTEMD {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let runner = Recorder::failing(" start ");
        runner.fail_on("disable");
        let backend = machine.backend(kind, &runner);
        let before = machine.files();

        let error = install(backend.as_ref(), &machine.request(&preview, None, true)).unwrap_err();
        let message = error.to_string();

        let ServiceError::Register { cleanup, .. } = &error else {
            panic!("{kind:?}: {error}");
        };
        assert_eq!(cleanup.len(), 1, "{cleanup:?}");
        assert!(cleanup[0].contains("disable --now"), "{cleanup:?}");
        assert!(message.contains("except: "), "{message}");
        assert_eq!(machine.files(), before, "{kind:?}");
    }
}

#[test]
fn a_failing_stop_removes_nothing() {
    for kind in KINDS {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let runner = Recorder::default();
        let backend = machine.backend(kind, &runner);

        let installed = install(backend.as_ref(), &machine.request(&preview, None, true)).unwrap();
        runner.activate(backend.identity(&installed.name));
        runner.fail_on(match kind {
            Kind::Launchd => "bootout",
            _ => "disable",
        });

        let error = uninstall(backend.as_ref(), &installed.name).unwrap_err();

        assert!(
            matches!(error, ServiceError::Unregister { .. }),
            "{kind:?}: {error}"
        );
        assert!(installed.file.is_file(), "{kind:?}");
    }
}

#[test]
fn a_failing_reload_after_uninstalling_is_reported() {
    for kind in SYSTEMD {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let runner = Recorder::default();
        let backend = machine.backend(kind, &runner);

        let installed = install(backend.as_ref(), &machine.request(&preview, None, false)).unwrap();
        runner.fail_on("daemon-reload");

        let error = uninstall(backend.as_ref(), &installed.name).unwrap_err();

        assert!(
            matches!(&error, ServiceError::Forget { failure, .. }
                if failure.command == format!("{} daemon-reload", systemctl(kind))),
            "{kind:?}: {error}"
        );
        assert!(!installed.file.exists(), "{kind:?}");
    }
}

#[test]
fn system_units_need_root() {
    let machine = Machine::new();
    let preview = machine.instance("preview", "data", 3000);
    let runner = Recorder::default();
    let backend = Systemd::system(machine.root(), false, &runner);
    let before = machine.files();

    let error = install(&backend, &machine.request(&preview, None, false)).unwrap_err();
    assert!(matches!(error, ServiceError::NeedsRoot), "{error}");

    let error = uninstall(&backend, &name("cardano-preview")).unwrap_err();
    assert!(matches!(error, ServiceError::NeedsRoot), "{error}");

    assert_eq!(machine.files(), before);
    assert!(!machine.root().join("etc").exists());
    assert!(runner.calls().is_empty());
}

#[test]
fn reports_print_the_native_commands() {
    for kind in KINDS {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let runner = Recorder::default();
        let backend = machine.backend(kind, &runner);

        let installed = install(backend.as_ref(), &machine.request(&preview, None, false)).unwrap();
        let text = installed.to_string();
        let json = serde_json::to_value(&installed).unwrap();

        assert_eq!(json["name"], "cardano-preview");
        assert_eq!(json["identity"], backend.identity(&installed.name));
        assert_eq!(json["file"], installed.file.to_string_lossy().as_ref());

        for command in ["start", "stop", "restart", "status", "logs"] {
            let line = json["commands"][command].as_str().unwrap();
            assert!(text.contains(line), "{kind:?}: {text}");
        }

        for note in json["notes"].as_array().unwrap() {
            assert!(text.contains(note.as_str().unwrap()), "{kind:?}: {text}");
        }

        let status = json["commands"]["status"].as_str().unwrap();
        let logs = json["commands"]["logs"].as_str().unwrap();

        match kind {
            Kind::SystemdUser => {
                assert_eq!(
                    status,
                    "systemctl --user status dolos-cardano-preview.service"
                );
                assert_eq!(
                    logs,
                    "journalctl --user -u dolos-cardano-preview.service -f"
                );
                assert!(text.contains("loginctl enable-linger"), "{text}");
                assert_eq!(json["starts_at"], "login");
            }
            Kind::SystemdSystem => {
                assert_eq!(status, "systemctl status dolos-cardano-preview.service");
                assert!(text.contains("it runs as root"), "{text}");
                assert_eq!(json["starts_at"], "boot");
            }
            Kind::Launchd => {
                let log = machine
                    .home()
                    .join("Library/Logs/dolos-cardano-preview.log");

                assert_eq!(
                    status,
                    format!("launchctl print gui/{UID}/io.txpipe.dolos.cardano-preview")
                );
                assert_eq!(logs, format!("tail -f {}", log.display()));
                assert!(
                    text.contains(&format!("its log, {}, is never rotated", log.display())),
                    "{text}"
                );
            }
        }

        let uninstalled = uninstall(backend.as_ref(), &installed.name).unwrap();
        let json = serde_json::to_value(&uninstalled).unwrap();

        assert_eq!(json["identity"], backend.identity(&installed.name));
        assert_eq!(json["commands"]["status"], status);
        assert!(uninstalled.to_string().contains(status));
    }
}

#[test]
fn the_system_runner_reports_what_failed() {
    assert_eq!(SystemRunner.run("true", &[]), Ok(()));
    assert_eq!(
        SystemRunner.run("sh", &["-c", "echo refused >&2; exit 3"]),
        Err("refused".to_string())
    );

    let silent = SystemRunner.run("sh", &["-c", "exit 3"]).unwrap_err();
    assert!(silent.contains('3'), "{silent}");

    let missing = SystemRunner.run("dolos-no-such-program", &[]).unwrap_err();
    assert!(
        missing.starts_with("could not run dolos-no-such-program"),
        "{missing}"
    );
}

/// Linux reports the running binary through its symlinks, as Homebrew's
/// versioned `Cellar` path; the unit names it as it was run.
#[test]
fn the_binary_is_recorded_by_the_path_it_was_run_as() {
    let machine = Machine::new();
    let root = machine.root();
    let real = root.join("Cellar/dolos/1.0/bin/dolos");
    let link = root.join("bin/dolos");
    let other = root.join("other/dolos");

    for file in [&real, &link, &other] {
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
    }

    std::fs::write(&real, "dolos 1.0").unwrap();
    std::fs::write(&other, "another dolos").unwrap();
    std::os::unix::fs::symlink(&real, &link).unwrap();

    let locate = |invoked: Option<&str>, path: Option<&OsStr>, exe: &Path| {
        locate_binary(invoked.map(OsStr::new), path, root, exe).unwrap()
    };
    let path = std::env::join_paths([other.parent().unwrap(), link.parent().unwrap()]).unwrap();

    assert_eq!(locate(link.to_str(), None, &real), link);
    assert_eq!(locate(Some("./bin/./dolos"), None, &real), link);
    assert_eq!(locate(Some("dolos"), Some(&path), &real), link);

    // A name leading to another file, or through `..`, is not taken.
    assert_eq!(locate(other.to_str(), None, &real), real);
    assert_eq!(locate(Some("bin/../bin/dolos"), None, &real), real);
    assert_eq!(locate(Some("dolos"), None, &real), real);
    assert_eq!(locate(None, None, &real), real);

    // As macOS reports it, through the link.
    assert_eq!(locate(None, None, &link), link);
    assert_eq!(locate(None, None, Path::new("bin/dolos")), link);
}

/// The binary refuses these before it reads anything, so no service manager is
/// reached.
#[test]
fn the_command_line_refuses_invalid_names() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.toml");

    for command in ["install", "uninstall"] {
        let output = Command::new(env!("CARGO_BIN_EXE_dolos"))
            .arg("--config")
            .arg(&missing)
            .args(["service", command, "Not_A_Name"])
            .output()
            .unwrap();

        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("not a service name"),
            "{output:?}"
        );
    }

    let help = Command::new(env!("CARGO_BIN_EXE_dolos"))
        .args(["service", "--help"])
        .output()
        .unwrap();
    let help = String::from_utf8_lossy(&help.stdout);

    assert!(
        help.contains("install") && help.contains("uninstall"),
        "{help}"
    );
}

#[cfg(target_os = "macos")]
#[test]
fn macos_refuses_system_scope() {
    let dir = tempfile::tempdir().unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_dolos"))
        .arg("--config")
        .arg(dir.path().join("missing.toml"))
        .args(["service", "install", "--system"])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--system"),
        "{output:?}"
    );
}

/// The binary itself, run by name from a `PATH` holding only a symlink to it
/// and recording stand-ins for `systemctl` and `launchctl`, with a temporary
/// home for its user services.
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod command_line {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    use super::*;

    /// Records its arguments, and reports every service as not running.
    const STAND_IN: &str = "#!/bin/sh\n\
        echo \"${0##*/} $*\" >> \"$SERVICE_CALLS\"\n\
        case \" $* \" in *\" print \"*|*\" is-active \"*) exit 3 ;; esac\n";

    const KIND: Kind = if cfg!(target_os = "macos") {
        Kind::Launchd
    } else {
        Kind::SystemdUser
    };

    struct Sandbox {
        machine: Machine,
    }

    impl Sandbox {
        fn new() -> Self {
            let machine = Machine::new();
            let bin = machine.root().join("bin");
            std::fs::create_dir_all(&bin).unwrap();
            std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_dolos"), bin.join("dolos")).unwrap();

            for manager in ["systemctl", "launchctl"] {
                let path = bin.join(manager);
                std::fs::write(&path, STAND_IN).unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            }

            Self { machine }
        }

        fn binary(&self) -> PathBuf {
            self.machine.root().join("bin/dolos")
        }

        /// Where the binary puts the user's services.
        fn services(&self) -> PathBuf {
            match KIND {
                Kind::Launchd => self.machine.home().join("Library/LaunchAgents"),
                _ => self.machine.home().join(".config/systemd/user"),
            }
        }

        fn calls(&self) -> Vec<String> {
            std::fs::read_to_string(self.machine.root().join("calls"))
                .map(|calls| calls.lines().map(String::from).collect())
                .unwrap_or_default()
        }

        /// `dolos` with `args`, run by name in `dir` under umask 002.
        fn dolos(&self, dir: &Path, args: &[&str]) -> Command {
            let mut command = Command::new("/bin/sh");

            command
                .args(["-c", "umask 002 && exec dolos \"$@\"", "sh"])
                .args(args)
                .env_clear()
                .env("PATH", self.machine.root().join("bin"))
                .env("HOME", self.machine.home())
                .env("SERVICE_CALLS", self.machine.root().join("calls"))
                .current_dir(dir);

            command
        }
    }

    /// `text` without whitespace or the rules miette wraps long lines with.
    fn flat(text: &str) -> String {
        text.chars()
            .filter(|c| !c.is_whitespace() && *c != '│')
            .collect()
    }

    fn stderr(output: &std::process::Output) -> String {
        flat(&String::from_utf8_lossy(&output.stderr))
    }

    /// What installing `name` runs, with the user's id unknown.
    fn assert_installing(calls: &[String], name: &str) {
        match KIND {
            Kind::Launchd => assert!(
                matches!(calls, [print] if print.starts_with("launchctl print gui/")
                    && print.ends_with(&format!("/io.txpipe.dolos.{name}"))),
                "{calls:?}"
            ),
            _ => assert_eq!(calls, registration(KIND, name)),
        }
    }

    #[test]
    fn installs_the_binary_by_the_path_it_was_run_as() {
        let sandbox = Sandbox::new();
        let preview = sandbox.machine.instance("preview", "data", 3000);
        let dir = preview.file.parent().unwrap();

        let output = sandbox
            .dolos(
                dir,
                &[
                    "--config",
                    "dolos.toml",
                    "service",
                    "install",
                    "--format",
                    "json",
                ],
            )
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");

        let installed: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        let file = PathBuf::from(installed["file"].as_str().unwrap());
        let binary = sandbox.binary();

        // The symlink on PATH, which outlives the versioned file it leads to.
        assert_eq!(installed["binary"], binary.to_str().unwrap());
        assert_eq!(installed["config"], preview.file.to_str().unwrap());
        assert_eq!(installed["working_directory"], dir.to_str().unwrap());
        assert_eq!(file.parent().unwrap(), sandbox.services());
        assert!(std::fs::read_to_string(&file)
            .unwrap()
            .contains(binary.to_str().unwrap()));
        assert_installing(&sandbox.calls(), "cardano-preview");

        // Under umask 002, nothing it writes is writable by group or others.
        assert_eq!(std::fs::metadata(&file).unwrap().mode() & 0o777, 0o644);

        for created in sandbox
            .services()
            .ancestors()
            .take_while(|dir| *dir != sandbox.machine.home())
        {
            assert_eq!(
                std::fs::metadata(created).unwrap().mode() & 0o777,
                0o755,
                "{created:?}"
            );
        }

        // `dolos.toml` in the working directory, by default.
        let output = sandbox
            .dolos(dir, &["service", "uninstall"])
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        assert!(!file.exists());
    }

    /// The service runs without the installing shell's `DOLOS_` variables, so
    /// they decide neither its name nor its conflicts.
    #[test]
    fn shell_overrides_reach_neither_the_service_nor_its_checks() {
        let sandbox = Sandbox::new();
        let preview = sandbox.machine.instance("preview", "data", 3000);
        let inside = sandbox.machine.instance("inside", "../preview/data", 3001);
        let dir = preview.file.parent().unwrap();

        let output = sandbox
            .dolos(dir, &["service", "install", "--format", "json"])
            .env("DOLOS_CHAIN__NETWORK", "preprod")
            .env("DOLOS_STORAGE__PATH", "elsewhere")
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");

        let installed: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(installed["name"], "cardano-preview");
        assert_eq!(installed["storage"], dir.join("data").to_str().unwrap());
        assert!(
            stderr(&output)
                .contains("ignoringDOLOS_CHAIN__NETWORK,DOLOS_STORAGE__PATHfromthisshell"),
            "{output:?}"
        );

        // `inside` stores in `preview/data` by its file.
        let output = sandbox
            .dolos(
                inside.file.parent().unwrap(),
                &["service", "install", "inside"],
            )
            .env("DOLOS_STORAGE__PATH", "data")
            .output()
            .unwrap();

        assert_eq!(output.status.code(), Some(1), "{output:?}");
        assert!(
            stderr(&output).contains(&identity(KIND, "cardano-preview"))
                && stderr(&output).contains("storage.path"),
            "{output:?}"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn system_scope_as_a_user_prints_the_sudo_command() {
        let sandbox = Sandbox::new();

        if std::fs::metadata(sandbox.machine.root()).unwrap().uid() == 0 {
            eprintln!("skipped: as root, `--system` would reach this machine's units");
            return;
        }

        let preview = sandbox.machine.instance("preview", "data", 3000);
        let output = sandbox
            .dolos(
                preview.file.parent().unwrap(),
                &["service", "install", "--system"],
            )
            .output()
            .unwrap();

        assert_eq!(output.status.code(), Some(1), "{output:?}");
        assert!(
            stderr(&output).contains(&flat(&format!(
                "sudo {} service install --system",
                sandbox.binary().display()
            ))),
            "{output:?}"
        );
        assert!(sandbox.calls().is_empty());
        assert!(!sandbox.machine.root().join("etc").exists());
    }
}
