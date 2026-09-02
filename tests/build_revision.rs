//! Guards the revision the binary reports about itself.
//!
//! A build script's output is cached, and every git path it could watch
//! belongs to whichever worktree ran it first — so with a `CARGO_TARGET_DIR`
//! shared across worktrees a binary used to report a commit it was not built
//! from, confidently and with no way to tell. `build.rs` now re-runs on every
//! build; this test asserts the property that failed, so a future change that
//! reintroduces caching is caught here rather than in a report that
//! attributes results to the wrong commit.

use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").ok()?;

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

#[test]
fn stamped_revision_names_the_tree_it_was_built_from() {
    // Not `env::var("DOLOS_GIT_SHA")`: cargo puts the stamp itself into this
    // process's environment, so that guard holds for every build and skips the
    // whole test. The marker exists only when an override really happened.
    if option_env!("DOLOS_GIT_SHA_OVERRIDDEN").is_some() {
        eprintln!("skipped: the revision was overridden via DOLOS_GIT_SHA");
        return;
    }

    let Some(head) = git(&["rev-parse", "--short=8", "HEAD"]).filter(|s| !s.is_empty()) else {
        eprintln!("skipped: no git revision available for this tree");
        return;
    };

    let Some(status) = git(&["status", "--porcelain", "--untracked-files=no"]) else {
        eprintln!("skipped: git could not report the state of the working tree");
        return;
    };

    let expected = if status.is_empty() {
        head
    } else {
        format!("{head}-dirty")
    };

    assert_eq!(
        env!("DOLOS_GIT_SHA"),
        expected,
        "the binary reports a revision it was not built from; the build script's \
         output was reused from an earlier build (a target directory shared across \
         git worktrees is the usual cause)",
    );

    assert_eq!(
        env!("DOLOS_VERSION"),
        format!("{} ({})", env!("CARGO_PKG_VERSION"), expected),
    );
}
