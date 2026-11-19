//! Checks of a redb file before and after redb opens it, shared by the stores
//! a release may reset rather than refuse.

use std::path::Path;

/// redb's page size, which its header must record.
const REDB_PAGE_SIZE: u32 = 4096;

/// Fails as corrupt if the file is shorter than the layout its header records,
/// is not a whole number of pages, or records a layout redb cannot read: redb
/// panics on all three before it reports any error. The header is read under a
/// shared lock, which conflicts with redb's exclusive one, so a file another
/// process holds fails as open instead.
pub(crate) fn check_layout(path: &Path) -> Result<(), redb::Error> {
    use std::io::Read as _;

    let Some(mut file) = locked_shared(path)? else {
        return Ok(());
    };

    let len = file.metadata()?.len();
    let mut header = [0; 32];

    // redb reports a file too short for its magic number or its header itself
    if file.read_exact(&mut header).is_err() || !header.starts_with(b"redb\x1a\x0a\xa9\x0d\x0a") {
        return Ok(());
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

/// The file at `path` under a shared lock, which conflicts with redb's
/// exclusive one, or `None` if there is no file.
fn locked_shared(path: &Path) -> Result<Option<std::fs::File>, redb::Error> {
    use std::fs::TryLockError;

    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };

    // a shared lock, since NFS refuses an exclusive one on a read-only handle
    match file.try_lock_shared() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => return Err(redb::Error::DatabaseAlreadyOpen),
        Err(TryLockError::Error(error)) if error.kind() == std::io::ErrorKind::Unsupported => {}
        Err(TryLockError::Error(error)) => return Err(error.into()),
    }

    Ok(Some(file))
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
