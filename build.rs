//! Stamps the revision the binary is built from into the compiled artifact.
//!
//! The interesting part is not reading the revision — it is making sure the
//! recorded one cannot outlive the code it names. Cargo caches a build
//! script's output and re-runs the script only when one of the paths it asked
//! to watch has changed. Any git path this script could watch belongs to the
//! worktree that happened to build first: with a `CARGO_TARGET_DIR` shared
//! across worktrees there is a single cached output for all of them, so a
//! second worktree at a different commit silently inherits the first
//! worktree's revision. Watching a `HEAD` that is a symbolic ref makes it
//! worse still — its contents do not change when the branch advances, so even
//! the original worktree keeps the first revision it ever recorded.
//!
//! So the script asks to be re-run unconditionally, by watching a path under
//! `OUT_DIR` that it never creates. The cost is one `git` invocation per
//! build plus a recompile of this package; the gain is that
//! `dolos --version` names the revision it was actually built from, or says
//! `unknown`, and never a confident wrong answer.

use std::path::Path;
use std::process::Command;

/// Set this to record a revision without consulting git — for a build from a
/// source archive, or a pipeline that already knows the commit it checked out.
const REVISION_OVERRIDE: &str = "DOLOS_GIT_SHA";

/// Emitted only when [`REVISION_OVERRIDE`] supplied the revision. The stamp
/// itself cannot carry that fact: cargo puts every `rustc-env` variable into
/// the environment of the executables it runs, so a test asking whether
/// `DOLOS_GIT_SHA` is set sees the stamp and concludes every build was
/// overridden. This marker is absent unless an override really happened.
const OVERRIDE_MARKER: &str = "DOLOS_GIT_SHA_OVERRIDDEN";

/// Runs `git` in the package directory, returning its trimmed stdout on
/// success. Any failure — no git, no repository, no commit — is `None`, which
/// the caller turns into `unknown` rather than into a guess.
fn git(manifest_dir: &str, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("--no-optional-locks")
        .args(args)
        .current_dir(manifest_dir)
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// The revision to stamp, and whether it came from [`REVISION_OVERRIDE`]
/// rather than from git: the override if one is set, else `HEAD` abbreviated
/// to eight characters, suffixed `-dirty` when tracked files differ from it.
fn revision(manifest_dir: &str) -> (String, bool) {
    if let Ok(sha) = std::env::var(REVISION_OVERRIDE) {
        if !sha.trim().is_empty() {
            return (sha.trim().to_owned(), true);
        }
    }

    let Some(sha) =
        git(manifest_dir, &["rev-parse", "--short=8", "HEAD"]).filter(|s| !s.is_empty())
    else {
        return ("unknown".to_owned(), false);
    };

    let sha = match git(
        manifest_dir,
        &["status", "--porcelain", "--untracked-files=no"],
    ) {
        Some(status) if !status.is_empty() => format!("{sha}-dirty"),
        _ => sha,
    };

    (sha, false)
}

fn main() {
    // Cargo reads these from the environment of *this* process, so they
    // describe the build actually running. The `env!` equivalents would be
    // baked into the build script binary, which is itself cached across
    // worktrees — the very staleness this script exists to avoid.
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR");
    let package_version = std::env::var("CARGO_PKG_VERSION").expect("CARGO_PKG_VERSION");

    let never_created = Path::new(&out_dir).join("always-rerun");
    println!("cargo:rerun-if-changed={}", never_created.display());
    println!("cargo:rerun-if-env-changed={REVISION_OVERRIDE}");

    let (revision, overridden) = revision(&manifest_dir);

    println!("cargo:rustc-env={REVISION_OVERRIDE}={revision}");
    println!("cargo:rustc-env=DOLOS_VERSION={package_version} ({revision})");

    if overridden {
        println!("cargo:rustc-env={OVERRIDE_MARKER}=1");
    }
}
