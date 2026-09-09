#!/usr/bin/env python3
"""Print an apply_patch patch for identical timing instrumentation on all arms."""

import difflib
import pathlib
import sys

source = pathlib.Path(sys.argv[1]).resolve() / "src/bin/dolos/data/import_archive.rs"
before = source.read_text()
after = before.replace(
    "    let mut reached_end = false;",
    "    let mut reached_end = false;\n    let mut benchmark_commits = Vec::new();",
)
after = after.replace(
    "        writer\n            .commit()",
    "        let benchmark_started = std::time::Instant::now();\n        writer\n            .commit()",
)
after = after.replace(
    '.context("committing archive batch")?;',
    '.context("committing archive batch")?;\n'
    "        benchmark_commits.push(benchmark_started.elapsed().as_nanos() as u64);",
)
buffers = "serde_json::Value::Null"
if ".start_import_writer()" in before:
    buffers = """match &archive {
        dolos::adapters::ArchiveStoreBackend::Fjall(store) => {
            let stats = store.append_stats();
            serde_json::json!({
                "encoders_peak": stats.import_encoders_peak,
                "window_bytes_peak": stats.import_window_bytes_peak,
                "buffer_bytes_peak": stats.import_buffer_bytes_peak,
                "windows": stats.import_windows,
                "import_batches": stats.import_batches,
                "serial_batches": stats.serial_batches,
            })
        }
        _ => serde_json::Value::Null,
    }"""
after = after.replace(
    '    progress.finish_with_message("archive import complete");',
    '    progress.finish_with_message("archive import complete");\n'
    '    if let Some(path) = std::env::var_os("DOLOS_ARCHIVE_BENCH_METRICS") {\n'
    f"        let buffers = {buffers};\n"
    '        let metrics = serde_json::json!({"commit_ns": benchmark_commits, "buffers": buffers});\n'
    "        std::fs::write(path, serde_json::to_vec(&metrics).unwrap()).unwrap();\n"
    "    }",
)
assert after.count("benchmark_commits.push(") == 1
assert after.count("let mut benchmark_commits") == 1
diff = list(difflib.unified_diff(before.splitlines(True), after.splitlines(True)))
assert diff
print("*** Begin Patch")
print(f"*** Update File: {source}")
for line in diff[2:]:
    if line.startswith("@@"):
        print("@@")
    else:
        print(line, end="")
print("*** End Patch")
