//! `service install | uninstall` over fixture directories: systemd user and
//! system units and launchd agents, written under a temporary root, with a
//! command runner that records what it is asked to run instead of running it.
//! Nothing here registers a service with a live service manager.

#![cfg(unix)]

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use dolos::service::{
    install, uninstall, CommandRunner, Install, Launchd, Name, Recorded, ServiceBackend,
    ServiceError, Systemd, Unit,
};
use dolos_core::config::{load, Compatibility, RootConfig};

/// Records each command it is asked to run, and fails those containing
/// `fail`.
#[derive(Default)]
struct Recorder {
    calls: RefCell<Vec<String>>,
    fail: Option<&'static str>,
}

impl Recorder {
    fn failing(fail: &'static str) -> Self {
        Self {
            fail: Some(fail),
            ..Self::default()
        }
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

        match self.fail {
            Some(fail) if call.contains(fail) => Err(format!("{fail}: simulated failure")),
            _ => Ok(()),
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

const UID: u32 = 501;

struct Instance {
    file: PathBuf,
    config: RootConfig,
}

/// A machine under a temporary root: a user's home, and one directory per
/// instance under `srv/`.
struct Machine {
    root: tempfile::TempDir,
}

impl Machine {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("home/operator")).unwrap();

        Self { root }
    }

    fn home(&self) -> PathBuf {
        self.root.path().join("home/operator")
    }

    fn binary(&self) -> PathBuf {
        self.root.path().join("opt/dolos/bin/dolos")
    }

    fn backend<'a>(&self, kind: Kind, runner: &'a Recorder) -> Box<dyn ServiceBackend + 'a> {
        match kind {
            Kind::SystemdUser => Box::new(Systemd::user(&self.home(), runner)),
            Kind::SystemdSystem => Box::new(Systemd::system(self.root.path(), true, runner)),
            Kind::Launchd => Box::new(Launchd::new(&self.home(), UID, runner)),
        }
    }

    /// `srv/{dir}/dolos.toml`, storing under `storage` and serving minibf on
    /// `port`, with a file in its storage directory.
    fn instance(&self, dir: &str, storage: &str, port: u16) -> Instance {
        let dir = self.root.path().join("srv").join(dir);
        std::fs::create_dir_all(&dir).unwrap();

        let file = dir.join("dolos.toml");
        std::fs::write(
            &file,
            format!(
                "compatibility = \"{}\"\n\n\
                 [chain]\nprotocol = \"cardano\"\nnetwork = \"preview\"\n\n\
                 [storage]\npath = \"{storage}\"\n\n\
                 [serve.minibf]\nlisten_address = \"0.0.0.0:{port}\"\n",
                Compatibility::current()
            ),
        )
        .unwrap();

        let data = dir.join(storage);
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("chain.db"), dir.to_string_lossy().as_bytes()).unwrap();

        Instance {
            config: load(Some(&file)).unwrap(),
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
        files(self.root.path())
    }
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

fn systemctl(kind: Kind) -> &'static str {
    match kind {
        Kind::SystemdUser => "systemctl --user",
        _ => "systemctl",
    }
}

/// What installing `name` runs, without `--start`.
fn registration(kind: Kind, name: &str) -> Vec<String> {
    match kind {
        Kind::Launchd => Vec::new(),
        _ => vec![
            format!("{} daemon-reload", systemctl(kind)),
            format!("{} enable dolos-{name}.service", systemctl(kind)),
        ],
    }
}

/// What uninstalling `name` runs.
fn unregistration(kind: Kind, name: &str) -> Vec<String> {
    match kind {
        Kind::Launchd => vec![
            format!("launchctl print gui/{UID}/io.txpipe.dolos.{name}"),
            format!("launchctl bootout gui/{UID}/io.txpipe.dolos.{name}"),
        ],
        _ => vec![
            format!("{} disable --now dolos-{name}.service", systemctl(kind)),
            format!("{} daemon-reload", systemctl(kind)),
        ],
    }
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
                machine.root.path().join("etc/systemd/system"),
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

        let expected = match kind {
            Kind::Launchd => vec![format!(
                "launchctl bootstrap gui/{UID} {}",
                installed.file.display()
            )],
            _ => {
                let mut calls = registration(kind, "cardano-preview");
                calls.push(format!(
                    "{} start dolos-cardano-preview.service",
                    systemctl(kind)
                ));
                calls
            }
        };

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

        let error = install(backend.as_ref(), &machine.request(&other, None, true)).unwrap_err();

        assert!(
            matches!(&error, ServiceError::AlreadyInstalled { identity, .. }
                if *identity == backend.identity(&name("cardano-preview"))),
            "{kind:?}: {error}"
        );
        assert_eq!(machine.files(), before, "{kind:?}");
        assert_eq!(runner.calls(), calls, "{kind:?}");
    }
}

#[test]
fn overlapping_storage_is_refused_naming_the_unit() {
    for kind in KINDS {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let inside = machine.instance("inside", "../preview/data/archive", 3001);
        let runner = Recorder::default();
        let backend = machine.backend(kind, &runner);

        install(backend.as_ref(), &machine.request(&preview, None, false)).unwrap();
        let before = machine.files();
        let calls = runner.calls();

        let error = install(
            backend.as_ref(),
            &machine.request(&inside, Some("inside"), false),
        )
        .unwrap_err();

        let ServiceError::Conflict {
            identity, reason, ..
        } = &error
        else {
            panic!("{kind:?}: {error}");
        };
        assert_eq!(*identity, backend.identity(&name("cardano-preview")));
        assert!(reason.contains("storage.path"), "{reason}");
        assert!(error.to_string().contains(identity.as_str()), "{error}");
        assert_eq!(machine.files(), before, "{kind:?}");
        assert_eq!(runner.calls(), calls, "{kind:?}");
    }
}

#[test]
fn a_shared_port_is_refused_naming_the_unit() {
    for kind in KINDS {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let same_port = machine.instance("same-port", "data", 3000);
        let runner = Recorder::default();
        let backend = machine.backend(kind, &runner);

        install(backend.as_ref(), &machine.request(&preview, None, false)).unwrap();
        let before = machine.files();

        let error = install(
            backend.as_ref(),
            &machine.request(&same_port, Some("same-port"), false),
        )
        .unwrap_err();

        let ServiceError::Conflict {
            identity, reason, ..
        } = &error
        else {
            panic!("{kind:?}: {error}");
        };
        assert_eq!(*identity, backend.identity(&name("cardano-preview")));
        assert!(reason.contains("serve.minibf.listen_address"), "{reason}");
        assert_eq!(machine.files(), before, "{kind:?}");
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

#[test]
fn explicit_name_uninstall_needs_no_configuration() {
    for kind in KINDS {
        let machine = Machine::new();
        let analytics = machine.instance("analytics", "data", 3000);
        let runner = Recorder::default();
        let backend = machine.backend(kind, &runner);

        let installed = install(
            backend.as_ref(),
            &machine.request(&analytics, Some("analytics"), false),
        )
        .unwrap();
        std::fs::remove_dir_all(machine.root.path().join("srv")).unwrap();
        let installing = runner.calls().len();

        let uninstalled = uninstall(backend.as_ref(), &name("analytics")).unwrap();

        assert_eq!(uninstalled.file, installed.file);
        assert_eq!(uninstalled.config.as_ref(), Some(&analytics.file));
        assert!(!installed.file.exists(), "{kind:?}");
        assert_eq!(
            runner.calls()[installing..],
            unregistration(kind, "analytics"),
            "{kind:?}"
        );
    }
}

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

        let srv = machine.root.path().join("srv");
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
            unregistration(kind, "cardano-preview"),
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
        assert!(runner.calls().is_empty());
    }
}

#[test]
fn a_failing_daemon_reload_leaves_no_file_behind() {
    for kind in [Kind::SystemdUser, Kind::SystemdSystem] {
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
        assert_eq!(runner.calls().len(), 1, "{:?}", runner.calls());
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

        if kind != Kind::Launchd {
            let unit = "dolos-cardano-preview.service";
            let systemctl = systemctl(kind);

            assert_eq!(
                runner.calls(),
                [
                    format!("{systemctl} daemon-reload"),
                    format!("{systemctl} enable {unit}"),
                    format!("{systemctl} start {unit}"),
                    format!("{systemctl} disable --now {unit}"),
                    format!("{systemctl} daemon-reload"),
                ]
            );
        }
    }
}

#[test]
fn a_failing_stop_removes_nothing() {
    for kind in KINDS {
        let machine = Machine::new();
        let preview = machine.instance("preview", "data", 3000);
        let runner = Recorder::failing("bootout");
        let systemd_runner = Recorder::failing("disable");
        let runner = match kind {
            Kind::Launchd => &runner,
            _ => &systemd_runner,
        };
        let backend = machine.backend(kind, runner);

        let installed = install(backend.as_ref(), &machine.request(&preview, None, false)).unwrap();
        let error = uninstall(backend.as_ref(), &installed.name).unwrap_err();

        assert!(
            matches!(error, ServiceError::Unregister { .. }),
            "{kind:?}: {error}"
        );
        assert!(installed.file.is_file(), "{kind:?}");
    }
}

#[test]
fn system_units_need_root() {
    let machine = Machine::new();
    let preview = machine.instance("preview", "data", 3000);
    let runner = Recorder::default();
    let backend = Systemd::system(machine.root.path(), false, &runner);
    let before = machine.files();

    let error = install(&backend, &machine.request(&preview, None, false)).unwrap_err();
    assert!(matches!(error, ServiceError::NeedsRoot), "{error}");

    let error = uninstall(&backend, &name("cardano-preview")).unwrap_err();
    assert!(matches!(error, ServiceError::NeedsRoot), "{error}");

    assert_eq!(machine.files(), before);
    assert!(!machine.root.path().join("etc").exists());
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
                assert_eq!(json["starts_at"], "boot");
            }
            Kind::Launchd => {
                assert_eq!(
                    status,
                    format!("launchctl print gui/{UID}/io.txpipe.dolos.cardano-preview")
                );
                assert!(logs.starts_with("tail -f "), "{logs}");
                assert!(
                    logs.ends_with("Library/Logs/dolos-cardano-preview.log"),
                    "{logs}"
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
