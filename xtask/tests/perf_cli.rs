use std::process::Command;

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_cargo-xtask")
}

#[test]
fn storage_and_minibf_have_separate_command_trees() {
    for subject in ["storage", "minibf"] {
        let output = Command::new(binary())
            .args(["perf", subject, "--help"])
            .output()
            .unwrap();
        assert!(output.status.success());
        let help = String::from_utf8(output.stdout).unwrap();
        if subject == "storage" {
            assert!(help.contains("train"));
            assert!(!help.contains("minibf"));
        } else {
            assert!(help.contains("compare"));
            assert!(!help.contains("train"));
        }
    }
    assert!(Command::new(binary())
        .args(["perf", "storage", "run", "--help"])
        .output()
        .unwrap()
        .status
        .success());
    assert!(Command::new(binary())
        .args(["archive-bench", "bench", "--help"])
        .status()
        .unwrap()
        .success());
}

#[test]
fn paired_runner_uses_minibf_command_and_shared_report() {
    let temp = tempfile::tempdir().unwrap();
    let records = temp.path().join("paired.jsonl");
    let result = Command::new(binary())
        .args(["perf", "minibf", "compare", "--bin"])
        .arg(format!("baseline={}", binary()))
        .arg("--bin")
        .arg(format!("candidate={}", binary()))
        .args(["--", "--work"])
        .arg(temp.path())
        .arg("--out")
        .arg(&records)
        .args([
            "--run",
            "taxonomy-smoke",
            "--requests",
            "2",
            "--repeat",
            "1",
            "--cases",
            "epoch-stakes-sparse-pool",
        ])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(&records).unwrap().lines().count(),
        2
    );
    let report = Command::new(binary())
        .args(["perf", "report"])
        .arg(&records)
        .output()
        .unwrap();
    assert!(report.status.success());
    assert!(String::from_utf8(report.stdout)
        .unwrap()
        .contains("INSUFFICIENT"));
    let gate = Command::new(binary())
        .args(["perf", "minibf", "check"])
        .arg(&records)
        .output()
        .unwrap();
    assert!(!gate.status.success());
}
