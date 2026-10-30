//! systemd units, `dolos-<name>.service`: the user's, started at login, or
//! the system's, started at boot.

use std::path::{Path, PathBuf};

use super::{
    text, Autostart, CommandRunner, Commands, Failure, Manager, Name, Recorded, RegisterError,
    Scope, ServiceBackend, ServiceError, Unit, MARKER,
};

pub struct Systemd<R> {
    scope: Scope,
    dir: PathBuf,
    /// The user's home, for user units.
    home: Option<PathBuf>,
    /// Whether this process may change the scope's units.
    privileged: bool,
    runner: R,
}

impl<R: CommandRunner> Systemd<R> {
    /// The user units of the user whose home is `home`, under
    /// `~/.config/systemd/user`, managed with `systemctl --user`.
    pub fn user(home: &Path, runner: R) -> Self {
        Self {
            scope: Scope::User,
            dir: home.join(".config/systemd/user"),
            home: Some(home.to_path_buf()),
            privileged: true,
            runner,
        }
    }

    /// The system units under `root` (`/` on a live machine), in
    /// `/etc/systemd/system`, managed with `systemctl`; `privileged` when this
    /// process runs as root.
    pub fn system(root: &Path, privileged: bool, runner: R) -> Self {
        Self {
            scope: Scope::System,
            dir: root.join("etc/systemd/system"),
            home: None,
            privileged,
            runner,
        }
    }

    fn unit(name: &Name) -> String {
        format!("dolos-{name}.service")
    }

    fn systemctl(&self, args: &[&str]) -> Result<(), Failure> {
        let mut all = Vec::with_capacity(args.len() + 1);

        if self.scope == Scope::User {
            all.push("--user");
        }

        all.extend_from_slice(args);

        self.runner
            .run("systemctl", &all)
            .map_err(|detail| Failure::of("systemctl", &all, detail))
    }
}

#[cfg(target_os = "linux")]
impl Systemd<super::SystemRunner> {
    /// This machine's units in `scope`.
    pub fn live(scope: Scope) -> Result<Self, ServiceError> {
        Ok(match scope {
            Scope::User => Self::user(&super::home()?, super::SystemRunner),
            Scope::System => Self::system(
                Path::new("/"),
                nix::unistd::Uid::effective().is_root(),
                super::SystemRunner,
            ),
        })
    }
}

impl<R: CommandRunner> ServiceBackend for Systemd<R> {
    fn manager(&self) -> Manager {
        Manager::Systemd
    }

    fn scope(&self) -> Scope {
        self.scope
    }

    fn dir(&self) -> &Path {
        &self.dir
    }

    fn identity(&self, name: &Name) -> String {
        Self::unit(name)
    }

    fn file(&self, name: &Name) -> PathBuf {
        self.dir.join(Self::unit(name))
    }

    fn identity_of(&self, file_name: &str) -> Option<String> {
        let name = file_name.strip_prefix("dolos-")?.strip_suffix(".service")?;

        (!name.is_empty()).then(|| file_name.to_string())
    }

    fn render(&self, unit: &Unit) -> Result<String, ServiceError> {
        let exec = unit.arguments()?.map(exec_word).join(" ");

        // The value runs to the end of its line, unquoted: systemd strips a
        // trailing space and reads a trailing `\` as a line continuation.
        let working_dir = text(&unit.working_dir)?;

        if working_dir.ends_with([' ', '\\']) {
            return Err(ServiceError::UnsupportedPath(unit.working_dir.clone()));
        }

        let working_dir = escape_specifiers(working_dir);

        let (network, wanted_by) = match self.scope {
            // A user manager has no network target to wait for.
            Scope::User => ("", "default.target"),
            Scope::System => (
                "After=network-online.target\nWants=network-online.target\n",
                "multi-user.target",
            ),
        };

        Ok(format!(
            "# {MARKER} To change it, uninstall and install again.\n\
             [Unit]\n\
             Description=Dolos ({name})\n\
             {network}\n\
             [Service]\n\
             WorkingDirectory={working_dir}\n\
             ExecStart={exec}\n\
             Restart=on-failure\n\
             RestartSec=5\n\
             \n\
             [Install]\n\
             WantedBy={wanted_by}\n",
            name = unit.name,
        ))
    }

    fn read(&self, contents: &str) -> Option<Recorded> {
        let mut exec = None;
        let mut working_dir = None;

        for line in contents.lines() {
            match line.split_once('=') {
                Some((key, value)) if key.trim() == "ExecStart" => exec = Some(value.trim()),
                Some((key, value)) if key.trim() == "WorkingDirectory" => {
                    working_dir = Some(value.trim())
                }
                _ => {}
            }
        }

        let arguments = split_exec(exec?)?;

        // `-` only makes a missing directory no error.
        let working_dir = match working_dir.map(|dir| dir.trim_start_matches('-')) {
            Some(dir) if dir.starts_with('~') => self
                .home
                .clone()?
                .join(dir.trim_start_matches('~').trim_start_matches('/')),
            Some(dir) => PathBuf::from(unescape_specifiers(dir)),
            None => match &self.home {
                Some(home) => home.clone(),
                None => PathBuf::from("/"),
            },
        };

        Some(Recorded::from_arguments(&arguments, working_dir))
    }

    fn check_access(&self) -> Result<(), ServiceError> {
        if self.privileged {
            Ok(())
        } else {
            Err(ServiceError::NeedsRoot)
        }
    }

    fn active(&self, name: &Name) -> bool {
        self.systemctl(&["is-active", "--quiet", &Self::unit(name)])
            .is_ok()
    }

    fn register(&self, name: &Name, start: bool) -> Result<(), RegisterError> {
        let unit = Self::unit(name);

        self.systemctl(&["daemon-reload"])
            .map_err(|failure| RegisterError {
                failure,
                loaded: false,
                cleanup: Vec::new(),
            })?;

        self.systemctl(&["enable", &unit])
            .map_err(|failure| RegisterError {
                failure,
                loaded: true,
                cleanup: Vec::new(),
            })?;

        if start {
            if let Err(failure) = self.systemctl(&["start", &unit]) {
                let cleanup = self
                    .systemctl(&["disable", "--now", &unit])
                    .err()
                    .map(|failure| failure.to_string())
                    .into_iter()
                    .collect();

                return Err(RegisterError {
                    failure,
                    loaded: true,
                    cleanup,
                });
            }
        }

        Ok(())
    }

    fn unregister(&self, name: &Name) -> Result<(), Failure> {
        self.systemctl(&["disable", "--now", &Self::unit(name)])
    }

    fn forget(&self) -> Result<(), Failure> {
        self.systemctl(&["daemon-reload"])
    }

    fn autostart(&self) -> Autostart {
        match self.scope {
            Scope::User => Autostart::Login,
            Scope::System => Autostart::Boot,
        }
    }

    fn commands(&self, name: &Name) -> Commands {
        let unit = Self::unit(name);
        let (systemctl, journalctl) = match self.scope {
            Scope::User => ("systemctl --user", "journalctl --user"),
            Scope::System => ("systemctl", "journalctl"),
        };

        Commands {
            start: format!("{systemctl} start {unit}"),
            stop: format!("{systemctl} stop {unit}"),
            restart: format!("{systemctl} restart {unit}"),
            status: format!("{systemctl} status {unit}"),
            logs: format!("{journalctl} -u {unit} -f"),
        }
    }

    fn notes(&self, _name: &Name) -> Vec<String> {
        match self.scope {
            Scope::User => vec![
                "to start it at boot rather than at login, run `loginctl enable-linger`".into(),
            ],
            Scope::System => vec![
                "it runs as root, so root owns what it writes under its storage directory, and \
                 `dolos` commands run as another user cannot change that data"
                    .into(),
            ],
        }
    }
}

/// `%` as systemd reads it literally.
fn escape_specifiers(value: &str) -> String {
    value.replace('%', "%%")
}

fn unescape_specifiers(value: &str) -> String {
    value.replace("%%", "%")
}

/// One `ExecStart=` word: specifiers and variables escaped, and quoted
/// unless it is plain.
fn exec_word(word: &str) -> String {
    let escaped = escape_specifiers(word).replace('$', "$$");
    let plain = escaped
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "/._-+,:=@%$".contains(c));

    if plain {
        escaped
    } else {
        format!("\"{}\"", escaped.replace('\\', r"\\").replace('"', "\\\""))
    }
}

/// An `ExecStart=` value's words, as systemd splits and unescapes them,
/// without the executable's prefixes (`-`, `@`, `+`, ...).
fn split_exec(value: &str) -> Option<Vec<String>> {
    let mut words = Vec::new();
    let mut chars = value.chars().peekable();

    loop {
        while chars.next_if(|c| c.is_whitespace()).is_some() {}

        if chars.peek().is_none() {
            break;
        }

        let mut word = String::new();
        let mut quote = None;

        while let Some(c) = chars.next() {
            match (quote, c) {
                (None, c) if c.is_whitespace() => break,
                (None, '"' | '\'') => quote = Some(c),
                (Some(open), c) if c == open => quote = None,
                (_, '\\') => word.push(match chars.next()? {
                    'n' => '\n',
                    't' => '\t',
                    other => other,
                }),
                (_, c) => word.push(c),
            }
        }

        if quote.is_some() {
            return None;
        }

        words.push(unescape_specifiers(&word).replace("$$", "$"));
    }

    if let Some(first) = words.first_mut() {
        *first = first
            .trim_start_matches(['@', '-', ':', '+', '!'])
            .to_string();
    }

    Some(words)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exec_words_round_trip() {
        let words = [
            "/opt/my dolos/dolos",
            "--config",
            r#"/srv/100% "odd"\dir/dolos.toml"#,
            "$HOME",
            "daemon",
        ];
        let line = words.map(exec_word).join(" ");

        assert_eq!(split_exec(&line).unwrap(), words);
    }

    #[test]
    fn plain_words_stay_unquoted() {
        assert_eq!(exec_word("/usr/local/bin/dolos"), "/usr/local/bin/dolos");
        assert_eq!(exec_word("/srv/a b"), "\"/srv/a b\"");
        assert_eq!(exec_word("/srv/50%"), "/srv/50%%");
    }

    #[test]
    fn prefixes_and_unterminated_quotes() {
        assert_eq!(
            split_exec("-/usr/bin/dolos -c x.toml daemon").unwrap(),
            ["/usr/bin/dolos", "-c", "x.toml", "daemon"]
        );
        assert_eq!(split_exec("\"/usr/bin/dolos"), None);
    }
}
