use std::path::Path;

fn check_directory(directory: &Path) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let file = entry.unwrap().path();
        if file.is_dir() {
            check_directory(&file);
            continue;
        }
        if !matches!(
            file.extension().and_then(|extension| extension.to_str()),
            Some("json" | "jsonl" | "md")
        ) {
            continue;
        }
        let contents = std::fs::read_to_string(&file).unwrap();
        for forbidden in [
            "/Users/",
            "/Volumes/",
            "/home/",
            "/private/tmp/",
            "~/",
            "claude.ai/code/",
        ] {
            assert!(
                !contents.contains(forbidden),
                "{} contains a workstation path or session link; sanitize before publishing",
                file.display()
            );
        }
    }
}

#[test]
fn published_artifacts_exclude_workstation_paths_and_sessions() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    check_directory(&root.join("results"));
    check_directory(&root.join("../flatfiles/dictionary"));
}
