use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

fn main() {
    let git_dir = PathBuf::from(".git");
    emit_git_rerun_hints(&git_dir);

    let sha = git_short_sha().unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=DOLOS_GIT_SHA={sha}");
}

fn git_short_sha() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let sha = String::from_utf8(output.stdout).ok()?;
    let sha = sha.trim();

    (!sha.is_empty()).then_some(sha.to_string())
}

fn emit_git_rerun_hints(git_dir: &Path) {
    let head = git_dir.join("HEAD");

    println!("cargo:rerun-if-changed={}", head.display());

    let Ok(head_contents) = fs::read_to_string(&head) else {
        return;
    };

    let Some(reference) = head_contents.strip_prefix("ref: ") else {
        return;
    };

    let reference = reference.trim();
    let reference_path = git_dir.join(reference);
    println!("cargo:rerun-if-changed={}", reference_path.display());
}
