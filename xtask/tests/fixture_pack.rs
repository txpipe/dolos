//! Round-trip tests for fixture tar.zst pack/extract.
//!
//! Exercises the pack + extract path with no OCI interaction.

use std::fs;
use std::path::{Path, PathBuf};

use tempfile::tempdir;

// The pack module lives inside the xtask binary crate. Include it directly
// via `#[path]` so tests can drive the functions without exposing a lib.
#[path = "../src/fixture/pack.rs"]
mod pack;

fn write_file(p: &Path, content: &[u8]) {
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(p, content).unwrap();
}

fn collect_files(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn walk(root: &Path, dir: &Path, out: &mut Vec<(PathBuf, Vec<u8>)>) {
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let md = fs::metadata(&path).unwrap();
        if md.is_dir() {
            walk(root, &path, out);
        } else if md.is_file() {
            let rel = path.strip_prefix(root).unwrap().to_path_buf();
            out.push((rel, fs::read(&path).unwrap()));
        }
    }
}

#[test]
fn pack_then_extract_round_trip() {
    let src = tempdir().unwrap();
    write_file(&src.path().join("epochs.csv"), b"epoch_no\n100\n");
    write_file(
        &src.path().join("nested").join("stake-98.csv"),
        b"stake,pool,lovelace\n",
    );

    let archive = tempdir().unwrap();
    let archive_path = archive.path().join("payload.tar.zst");
    pack::pack_tar_zst(src.path(), &archive_path, 3).unwrap();

    assert!(
        archive_path.exists() && archive_path.metadata().unwrap().len() > 0,
        "archive was not written"
    );

    let dst = tempdir().unwrap();
    pack::extract_tar_zst(&archive_path, dst.path()).unwrap();

    assert_eq!(collect_files(src.path()), collect_files(dst.path()));
}

#[test]
fn pack_follows_symlinks() {
    let target = tempdir().unwrap();
    write_file(&target.path().join("hello.txt"), b"hello world");

    let src = tempdir().unwrap();
    std::os::unix::fs::symlink(target.path(), src.path().join("link")).unwrap();

    let archive = tempdir().unwrap();
    let archive_path = archive.path().join("payload.tar.zst");
    pack::pack_tar_zst(src.path(), &archive_path, 3).unwrap();

    let dst = tempdir().unwrap();
    pack::extract_tar_zst(&archive_path, dst.path()).unwrap();

    let extracted = dst.path().join("link").join("hello.txt");
    assert!(extracted.exists(), "symlinked file not extracted");
    assert_eq!(fs::read(&extracted).unwrap(), b"hello world");
}
