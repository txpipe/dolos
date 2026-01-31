mod harness;

use harness::compare::compare_csvs;
use harness::config::{load_xtask_config, resolve_path};
use harness::dump;

#[test]
fn compare_ground_truth_instances() {
    let repo_root = std::env::current_dir().expect("detecting repo root");
    let xtask_config = load_xtask_config(&repo_root).expect("loading xtask.toml");
    let instances_root = resolve_path(&repo_root, &xtask_config.instances_root);

    if !instances_root.exists() {
        eprintln!(
            "instances root does not exist: {}, skipping",
            instances_root.display()
        );
        return;
    }

    let mut entries: Vec<_> = std::fs::read_dir(&instances_root)
        .expect("reading instances root")
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.starts_with("test-") && e.path().is_dir()
        })
        .collect();

    entries.sort_by_key(|e| e.file_name());

    if entries.is_empty() {
        eprintln!("no test-* instances found, skipping");
        return;
    }

    let mut all_failures: Vec<String> = Vec::new();

    for entry in &entries {
        let dir_name = entry.file_name().to_string_lossy().to_string();
        let instance_dir = entry.path();

        // Parse test-{network}-{epoch}
        let parts: Vec<&str> = dir_name.splitn(3, '-').collect();
        if parts.len() < 3 {
            eprintln!("skipping instance with unexpected name format: {}", dir_name);
            continue;
        }
        let network = parts[1];
        let stop_epoch: u64 = match parts[2].parse() {
            Ok(e) => e,
            Err(_) => {
                eprintln!("skipping instance with non-numeric epoch: {}", dir_name);
                continue;
            }
        };

        let subject_epoch = stop_epoch.saturating_sub(2);
        let config_path = instance_dir.join("dolos.toml");
        if !config_path.exists() {
            eprintln!("skipping {}: no dolos.toml", dir_name);
            continue;
        }

        let ground_truth_dir = instance_dir.join("ground-truth");
        let dumps_dir = instance_dir.join("dumps");
        std::fs::create_dir_all(&dumps_dir).expect("creating dumps dir");

        eprintln!("\n=== Instance: {} (network={}, stop_epoch={}, subject_epoch={}) ===",
            dir_name, network, stop_epoch, subject_epoch);

        let mut instance_failures = Vec::new();

        // Eras
        {
            let dolos_path = dumps_dir.join("eras.csv");
            let gt_path = ground_truth_dir.join("eras.csv");
            eprintln!("\nComparing eras (full state)");
            if let Err(e) = dump::dump_eras(&config_path, &dolos_path) {
                instance_failures.push(format!("{}:eras dump failed: {}", dir_name, e));
            } else if gt_path.exists() {
                match compare_csvs(&dolos_path, &gt_path, &[0], 20) {
                    Ok(n) if n > 0 => instance_failures.push(format!("{}:eras ({} diffs)", dir_name, n)),
                    Err(e) => instance_failures.push(format!("{}:eras compare failed: {}", dir_name, e)),
                    _ => {}
                }
            } else {
                eprintln!("  ground-truth file missing, skipping: {}", gt_path.display());
            }
        }

        // Epochs
        {
            let dolos_path = dumps_dir.join("epochs.csv");
            let gt_path = ground_truth_dir.join("epochs.csv");
            eprintln!("\nComparing epochs (stop epoch {})", stop_epoch);
            if let Err(e) = dump::dump_epochs(&config_path, stop_epoch, &dolos_path) {
                instance_failures.push(format!("{}:epochs dump failed: {}", dir_name, e));
            } else if gt_path.exists() {
                match compare_csvs(&dolos_path, &gt_path, &[0], 20) {
                    Ok(n) if n > 0 => instance_failures.push(format!("{}:epochs ({} diffs)", dir_name, n)),
                    Err(e) => instance_failures.push(format!("{}:epochs compare failed: {}", dir_name, e)),
                    _ => {}
                }
            } else {
                eprintln!("  ground-truth file missing, skipping: {}", gt_path.display());
            }
        }

        // Delegation
        {
            let dolos_path = dump::delegation_csv_path(&dumps_dir, subject_epoch);
            let gt_path = ground_truth_dir.join(format!("delegation-{}.csv", subject_epoch));
            eprintln!("\nComparing delegation (subject epoch {})", subject_epoch);
            if dolos_path.exists() && gt_path.exists() {
                match compare_csvs(&dolos_path, &gt_path, &[0], 20) {
                    Ok(n) if n > 0 => instance_failures.push(format!("{}:delegation ({} diffs)", dir_name, n)),
                    Err(e) => instance_failures.push(format!("{}:delegation compare failed: {}", dir_name, e)),
                    _ => {}
                }
            } else {
                if !dolos_path.exists() {
                    eprintln!("  dolos dump missing, skipping: {}", dolos_path.display());
                }
                if !gt_path.exists() {
                    eprintln!("  ground-truth file missing, skipping: {}", gt_path.display());
                }
            }
        }

        // Stake
        {
            let dolos_path = dump::stake_csv_path(&dumps_dir, subject_epoch);
            let gt_path = ground_truth_dir.join(format!("stake-{}.csv", subject_epoch));
            eprintln!("\nComparing stake (subject epoch {})", subject_epoch);
            if dolos_path.exists() && gt_path.exists() {
                match compare_csvs(&dolos_path, &gt_path, &[0, 1], 20) {
                    Ok(n) if n > 0 => instance_failures.push(format!("{}:stake ({} diffs)", dir_name, n)),
                    Err(e) => instance_failures.push(format!("{}:stake compare failed: {}", dir_name, e)),
                    _ => {}
                }
            } else {
                if !dolos_path.exists() {
                    eprintln!("  dolos dump missing, skipping: {}", dolos_path.display());
                }
                if !gt_path.exists() {
                    eprintln!("  ground-truth file missing, skipping: {}", gt_path.display());
                }
            }
        }

        // Rewards
        {
            let dolos_path = dumps_dir.join("rewards.csv");
            let gt_path = ground_truth_dir.join("rewards.csv");
            eprintln!("\nComparing rewards (subject epoch {})", subject_epoch);
            if let Err(e) = dump::dump_rewards(&config_path, subject_epoch, &dolos_path) {
                instance_failures.push(format!("{}:rewards dump failed: {}", dir_name, e));
            } else if gt_path.exists() {
                match compare_csvs(&dolos_path, &gt_path, &[0, 1, 3, 4], 20) {
                    Ok(n) if n > 0 => instance_failures.push(format!("{}:rewards ({} diffs)", dir_name, n)),
                    Err(e) => instance_failures.push(format!("{}:rewards compare failed: {}", dir_name, e)),
                    _ => {}
                }
            } else {
                eprintln!("  ground-truth file missing, skipping: {}", gt_path.display());
            }
        }

        all_failures.extend(instance_failures);
    }

    if !all_failures.is_empty() {
        panic!(
            "\n{} ground-truth comparison(s) failed:\n  - {}",
            all_failures.len(),
            all_failures.join("\n  - ")
        );
    }
}
