//! Checks of a redb file before and after redb opens it, shared by the stores
//! a release may reset rather than refuse.

use std::path::Path;

/// redb's page size, which its header must record.
const REDB_PAGE_SIZE: u32 = 4096;

/// The magic number redb writes at the start of a file it has initialised.
const REDB_MAGIC: &[u8] = b"redb\x1a\x0a\xa9\x0d\x0a";

/// The leading header bytes read here: the magic number and the layout fields.
const HEADER_PREFIX: usize = 32;

/// Fails as corrupt if the file carries redb's magic number but is cut short
/// of its header, is shorter than the layout its header records, is not a
/// whole number of pages, or records a layout redb cannot read: redb panics on
/// these, or on Windows never returns, before it reports any error. The header
/// is read under a shared lock, which conflicts with redb's exclusive one, so
/// a file another process holds fails as open instead.
pub(crate) fn check_layout(path: &Path) -> Result<(), redb::Error> {
    use std::io::Read as _;

    let Some(mut held) = locked_shared(path)? else {
        return Ok(());
    };

    let len = held.file.metadata()?.len();
    let mut header = Vec::with_capacity(HEADER_PREFIX);

    // redb reports a file it cannot read, or without its magic number, itself
    if (&mut held.file)
        .take(HEADER_PREFIX as u64)
        .read_to_end(&mut header)
        .is_err()
        || !header.starts_with(REDB_MAGIC)
    {
        return Ok(());
    }

    if header.len() < HEADER_PREFIX {
        return Err(redb::Error::Corrupted(format!(
            "the file is {len} bytes, too short for its header"
        )));
    }

    let field = |offset: usize| {
        u128::from(u32::from_le_bytes(
            header[offset..offset + 4].try_into().unwrap(),
        ))
    };

    let (page_size, header_pages, max_pages, full_regions, trailing_pages) =
        (field(12), field(16), field(20), field(24), field(28));

    if page_size != u128::from(REDB_PAGE_SIZE)
        || max_pages == 0
        || (full_regions == 0 && trailing_pages == 0)
    {
        return Err(redb::Error::Corrupted(
            "its header records a layout redb cannot read".into(),
        ));
    }

    let trailing = if trailing_pages > 0 {
        (header_pages + trailing_pages) * page_size
    } else {
        0
    };

    let expected = page_size + full_regions * (header_pages + max_pages) * page_size + trailing;

    if u128::from(len) < expected || u128::from(len) % page_size != 0 {
        return Err(redb::Error::Corrupted(format!(
            "the file is {len} bytes and its header lays out {expected} in pages of {page_size}"
        )));
    }

    Ok(())
}

/// Fails as open while another process holds the file at `path`, and reads
/// or changes nothing else. A missing file is not held.
pub(crate) fn check_unheld(path: &Path) -> Result<(), redb::Error> {
    locked_shared(path).map(drop)
}

/// A file under [`locked_shared`]'s lock, released when it drops: a child
/// forked meanwhile holds a copy of the descriptor until it execs, and with
/// it the lock, which would refuse redb's exclusive one.
struct SharedLock {
    file: std::fs::File,
    locked: bool,
}

impl Drop for SharedLock {
    fn drop(&mut self) {
        if self.locked {
            let _ = self.file.unlock();
        }
    }
}

/// The file at `path` under a shared lock, which conflicts with redb's
/// exclusive one, or `None` if there is no file.
fn locked_shared(path: &Path) -> Result<Option<SharedLock>, redb::Error> {
    use std::fs::TryLockError;

    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };

    // a shared lock, since NFS refuses an exclusive one on a read-only handle
    let locked = match file.try_lock_shared() {
        Ok(()) => true,
        Err(TryLockError::WouldBlock) => return Err(redb::Error::DatabaseAlreadyOpen),
        Err(TryLockError::Error(error)) if error.kind() == std::io::ErrorKind::Unsupported => false,
        Err(TryLockError::Error(error)) => return Err(error.into()),
    };

    Ok(Some(SharedLock { file, locked }))
}

/// Whether an open failed on the file's contents rather than on access to it:
/// damage, a cut-short file, or a format or table layout of another release.
/// redb only reads the contents once it holds the file's lock, so no other
/// process is using a file found corrupt.
pub(crate) fn is_corrupt(error: &redb::Error) -> bool {
    match error {
        redb::Error::Corrupted(_)
        | redb::Error::UpgradeRequired(_)
        | redb::Error::TableTypeMismatch { .. }
        | redb::Error::TypeDefinitionChanged { .. }
        | redb::Error::TableIsMultimap(_)
        | redb::Error::TableIsNotMultimap(_) => true,
        redb::Error::Io(error) => matches!(
            error.kind(),
            std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_cut_short_of_its_header_is_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cut.redb");

        for len in [REDB_MAGIC.len(), 16, HEADER_PREFIX - 1] {
            let mut bytes = REDB_MAGIC.to_vec();
            bytes.resize(len, 0);
            std::fs::write(&path, &bytes).unwrap();

            assert!(
                matches!(check_layout(&path), Err(redb::Error::Corrupted(_))),
                "cut to {len} bytes"
            );
        }
    }

    #[test]
    fn a_short_file_without_the_magic_number_is_left_to_redb() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short.redb");

        for bytes in [&b""[..], b"redb", &[0xa5; 16]] {
            std::fs::write(&path, bytes).unwrap();

            assert!(check_layout(&path).is_ok(), "{bytes:?}");
        }
    }

    /// A copy of the probe's descriptor, as a child forked during the check
    /// holds until it execs, must not keep the lock past the probe: redb's
    /// exclusive lock would be refused.
    #[cfg(unix)]
    #[test]
    fn the_probe_releases_its_lock_while_a_copy_of_its_file_lives() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("held.redb");
        std::fs::write(&path, b"").unwrap();

        let held = locked_shared(&path).unwrap().unwrap();
        assert!(held.locked, "the file took no shared lock");

        let copy = held.file.try_clone().unwrap();
        drop(held);

        let other = std::fs::File::open(&path).unwrap();
        assert!(other.try_lock().is_ok(), "the copy kept the probe's lock");
        drop(copy);
    }
}
