//! launchd agents, `io.txpipe.dolos.<name>`: the user's LaunchAgents, loaded
//! at login. Loading one starts it, so an agent installed without `--start`
//! is only loaded at the next login, or by `launchctl bootstrap`.

use std::path::{Path, PathBuf};

use super::{
    shell_quote, text, Autostart, CommandRunner, Commands, Failure, Manager, Name, Recorded,
    RegisterError, Scope, ServiceBackend, ServiceError, Unit, MARKER,
};

pub struct Launchd<R> {
    agents: PathBuf,
    logs: PathBuf,
    uid: u32,
    runner: R,
}

impl<R: CommandRunner> Launchd<R> {
    /// The LaunchAgents of the user whose home is `home` and whose id is
    /// `uid`.
    pub fn new(home: &Path, uid: u32, runner: R) -> Self {
        Self {
            agents: home.join("Library/LaunchAgents"),
            logs: home.join("Library/Logs"),
            uid,
            runner,
        }
    }

    fn label(name: &Name) -> String {
        format!("io.txpipe.dolos.{name}")
    }

    fn domain(&self) -> String {
        format!("gui/{}", self.uid)
    }

    fn target(&self, name: &Name) -> String {
        format!("gui/{}/{}", self.uid, Self::label(name))
    }

    /// Where the agent's stdout and stderr go.
    fn log(&self, name: &Name) -> PathBuf {
        self.logs.join(format!("dolos-{name}.log"))
    }

    fn launchctl(&self, args: &[&str]) -> Result<String, Failure> {
        self.runner
            .run("launchctl", args)
            .map_err(|detail| Failure::of("launchctl", args, detail))
    }
}

#[cfg(target_os = "macos")]
impl Launchd<super::SystemRunner> {
    /// This user's agents.
    pub fn live() -> Result<Self, ServiceError> {
        Ok(Self::new(
            &super::home()?,
            nix::unistd::Uid::current().as_raw(),
            super::SystemRunner,
        ))
    }
}

impl<R: CommandRunner> ServiceBackend for Launchd<R> {
    fn manager(&self) -> Manager {
        Manager::Launchd
    }

    fn scope(&self) -> Scope {
        Scope::User
    }

    fn dir(&self) -> &Path {
        &self.agents
    }

    fn identity(&self, name: &Name) -> String {
        Self::label(name)
    }

    fn file(&self, name: &Name) -> PathBuf {
        self.agents.join(format!("{}.plist", Self::label(name)))
    }

    fn identity_of(&self, file_name: &str) -> Option<String> {
        let label = file_name.strip_suffix(".plist")?;
        let name = label.strip_prefix("io.txpipe.dolos.")?;

        (!name.is_empty()).then(|| label.to_string())
    }

    fn render(&self, unit: &Unit) -> Result<String, ServiceError> {
        let arguments: String = unit
            .arguments()?
            .iter()
            .map(|argument| format!("\t\t<string>{}</string>\n", escape(argument)))
            .collect();
        let log = self.log(&unit.name);
        let log = escape(text(&log)?);

        Ok(format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<!-- {MARKER} To change it, uninstall and install again. -->
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>{label}</string>
	<key>ProgramArguments</key>
	<array>
{arguments}	</array>
	<key>WorkingDirectory</key>
	<string>{working_dir}</string>
	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<dict>
		<key>SuccessfulExit</key>
		<false/>
	</dict>
	<key>StandardOutPath</key>
	<string>{log}</string>
	<key>StandardErrorPath</key>
	<string>{log}</string>
</dict>
</plist>
"#,
            label = Self::label(&unit.name),
            working_dir = escape(text(&unit.working_dir)?),
        ))
    }

    fn read(&self, contents: &str) -> Option<Recorded> {
        let array = after_key(contents, "ProgramArguments")?
            .trim_start()
            .strip_prefix("<array>")?;
        let array = &array[..array.find("</array>")?];

        let mut arguments = Vec::new();
        let mut rest = array;

        while let Some((argument, after)) = string(rest) {
            arguments.push(argument);
            rest = after;
        }

        // launchd runs an agent without one in `/`.
        let working_dir = after_key(contents, "WorkingDirectory")
            .and_then(string)
            .map_or_else(|| PathBuf::from("/"), |(dir, _)| PathBuf::from(dir));

        Some(Recorded::from_arguments(&arguments, working_dir))
    }

    fn check_access(&self) -> Result<(), ServiceError> {
        Ok(())
    }

    fn active(&self, name: &Name) -> bool {
        self.runner
            .run("launchctl", &["print", &self.target(name)])
            .is_ok()
    }

    fn register(&self, name: &Name, start: bool) -> Result<(), RegisterError> {
        if !start {
            return Ok(());
        }

        let file = self.file(name);

        self.launchctl(&["bootstrap", &self.domain(), &file.to_string_lossy()])
            .map(drop)
            .map_err(|failure| RegisterError {
                failure,
                loaded: false,
                cleanup: Vec::new(),
            })
    }

    fn unregister(&self, name: &Name) -> Result<(), Failure> {
        // Only a loaded agent can be booted out.
        if self.active(name) {
            self.launchctl(&["bootout", &self.target(name)])?;
        }

        Ok(())
    }

    fn forget(&self) -> Result<(), Failure> {
        Ok(())
    }

    fn autostart(&self) -> Autostart {
        Autostart::Login
    }

    fn commands(&self, name: &Name) -> Commands {
        let target = self.target(name);
        let file = self.file(name);
        let log = self.log(name);

        Commands {
            start: format!(
                "launchctl bootstrap {} {}",
                self.domain(),
                shell_quote(&file.to_string_lossy())
            ),
            stop: format!("launchctl bootout {target}"),
            restart: format!("launchctl kickstart -k {target}"),
            status: format!("launchctl print {target}"),
            logs: format!("tail -f {}", shell_quote(&log.to_string_lossy())),
        }
    }

    fn notes(&self, name: &Name) -> Vec<String> {
        vec![
            "`stop` unloads it until it is started again or you log in again".into(),
            format!(
                "its log, {}, is never rotated, and uninstalling keeps it",
                self.log(name).display()
            ),
        ]
    }
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn unescape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut rest = value;

    while let Some(at) = rest.find('&') {
        out.push_str(&rest[..at]);
        rest = &rest[at..];

        let entity = rest.find(';').map(|end| (&rest[1..end], end));
        let decoded = entity.and_then(|(entity, end)| {
            let c = match entity {
                "amp" => '&',
                "lt" => '<',
                "gt" => '>',
                "quot" => '"',
                "apos" => '\'',
                _ => {
                    let code = match entity.strip_prefix("#x") {
                        Some(hex) => u32::from_str_radix(hex, 16).ok()?,
                        None => entity.strip_prefix('#')?.parse().ok()?,
                    };
                    char::from_u32(code)?
                }
            };
            Some((c, end))
        });

        match decoded {
            Some((c, end)) => {
                out.push(c);
                rest = &rest[end + 1..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }

    out.push_str(rest);
    out
}

/// What follows `<key>{key}</key>`.
fn after_key<'a>(contents: &'a str, key: &str) -> Option<&'a str> {
    let marker = format!("<key>{key}</key>");
    let at = contents.find(&marker)?;

    Some(&contents[at + marker.len()..])
}

/// The leading `<string>`'s text, and what follows it.
fn string(xml: &str) -> Option<(String, &str)> {
    let rest = xml.trim_start().strip_prefix("<string>")?;
    let end = rest.find("</string>")?;

    Some((unescape(&rest[..end]), &rest[end + "</string>".len()..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entities_round_trip() {
        let value = "/srv/a & b/<x>/'q'\"";

        assert_eq!(unescape(&escape(value)), value);
        assert_eq!(unescape("&#65;&#x42;&quot;&bogus;"), "AB\"&bogus;");
    }
}
