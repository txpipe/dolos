//! Whether two instances can run side by side: a service name isolates
//! neither files nor listeners, so each instance needs its own stores, ports
//! and socket.

use std::borrow::Cow;
use std::net::{IpAddr, SocketAddr};
use std::path::{Component, Path, PathBuf};

use dolos_core::config::RootConfig;

/// What an instance writes and listens on, each with the key that sets it.
pub(super) struct Resources {
    paths: Vec<(&'static str, PathBuf)>,
    listeners: Vec<(&'static str, String)>,
    sockets: Vec<(&'static str, PathBuf)>,
}

impl Resources {
    /// `config`'s, with relative paths against `working_dir`.
    pub fn of(config: &RootConfig, working_dir: &Path) -> Self {
        let storage = &config.storage;
        let root = comparable(&storage.path, working_dir);
        let keeps_archive = config.keeps_archive();

        let stores = [
            ("storage.state.path", storage.state_path()),
            ("sync.wal_path", config.wal_path()),
            (
                "storage.archive.path",
                storage.archive_path().filter(|_| keeps_archive),
            ),
            (
                "storage.archive.blocks_path",
                storage.blocks_path().filter(|_| keeps_archive),
            ),
            ("storage.cache.path", storage.cache_path()),
            ("mempool.path", config.mempool_path()),
        ];

        // A store under the storage root overlaps whatever the root does.
        let mut paths = vec![("storage.path", root.clone())];
        paths.extend(stores.into_iter().filter_map(|(key, path)| {
            let path = comparable(&path?, working_dir);
            (!path.starts_with(&root)).then_some((key, path))
        }));

        let serve = &config.serve;
        let listeners = [
            (
                "serve.grpc.listen_address",
                serve.grpc.as_ref().map(|x| x.listen_address.clone()),
            ),
            (
                "serve.minibf.listen_address",
                serve.minibf.as_ref().map(|x| x.listen_address.to_string()),
            ),
            (
                "serve.minikupo.listen_address",
                serve
                    .minikupo
                    .as_ref()
                    .map(|x| x.listen_address.to_string()),
            ),
            (
                "serve.trp.listen_address",
                serve.trp.as_ref().map(|x| x.listen_address.to_string()),
            ),
            (
                "relay.listen_address",
                config.relay.as_ref().map(|x| x.listen_address.clone()),
            ),
        ]
        .into_iter()
        .filter_map(|(key, address)| Some((key, address?)))
        .collect();

        let sockets = serve
            .ouroboros
            .iter()
            .map(|x| {
                (
                    "serve.ouroboros.listen_path",
                    comparable(&x.listen_path, working_dir),
                )
            })
            .collect();

        Self {
            paths,
            listeners,
            sockets,
        }
    }

    /// Why `self` cannot run beside `other`, if it cannot.
    pub fn clash(&self, other: &Self) -> Option<String> {
        for (key, ours) in &self.paths {
            for (their_key, theirs) in &other.paths {
                let (a, b) = (folded(ours), folded(theirs));

                if a.starts_with(&b) || b.starts_with(&a) {
                    return Some(format!(
                        "{key} {} overlaps its {their_key} {}",
                        ours.display(),
                        theirs.display()
                    ));
                }
            }
        }

        for (key, ours) in &self.listeners {
            for (their_key, theirs) in &other.listeners {
                if same_port(ours, theirs) {
                    return Some(format!(
                        "{key} {ours} takes the port of its {their_key} {theirs}"
                    ));
                }
            }
        }

        for (key, ours) in &self.sockets {
            for (their_key, theirs) in &other.sockets {
                if folded(ours) == folded(theirs) {
                    return Some(format!("{key} {} is also its {their_key}", ours.display()));
                }
            }
        }

        None
    }
}

/// `path` against `working_dir`, as the file system resolves it: each
/// existing part through its symlinks before a `..` steps out of it, the
/// missing rest as written, so that two spellings of one place compare equal.
fn comparable(path: &Path, working_dir: &Path) -> PathBuf {
    let mut resolved = PathBuf::new();

    for component in working_dir.join(path).components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            other => {
                resolved.push(other);

                if let Ok(real) = resolved.canonicalize() {
                    resolved = real;
                }
            }
        }
    }

    resolved
}

/// `path` as the file system compares it, including the part that does not
/// exist yet. macOS volumes ignore case unless formatted otherwise; on one
/// that does not, two paths differing only in case are refused all the same.
fn folded(path: &Path) -> Cow<'_, Path> {
    match path.to_str() {
        Some(text) if cfg!(target_os = "macos") => Cow::Owned(text.to_lowercase().into()),
        _ => Cow::Borrowed(path),
    }
}

fn host_and_port(address: &str) -> Option<(Option<IpAddr>, u16)> {
    if let Ok(socket) = address.parse::<SocketAddr>() {
        return Some((Some(socket.ip()), socket.port()));
    }

    let (host, port) = address.rsplit_once(':')?;
    let host = host.trim_start_matches('[').trim_end_matches(']');

    Some((host.parse().ok(), port.parse().ok()?))
}

/// Whether two listen addresses bind one port: the same address, or a
/// wildcard and anything. A host name may resolve to any address.
fn same_port(a: &str, b: &str) -> bool {
    let (Some((host_a, port_a)), Some((host_b, port_b))) = (host_and_port(a), host_and_port(b))
    else {
        return a == b;
    };

    if port_a == 0 || port_a != port_b {
        return false;
    }

    match (host_a, host_b) {
        (Some(a), Some(b)) => a == b || a.is_unspecified() || b.is_unspecified(),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listeners_clash_on_a_shared_port() {
        assert!(same_port("0.0.0.0:3000", "127.0.0.1:3000"));
        assert!(same_port("[::]:50051", "127.0.0.1:50051"));
        assert!(same_port("localhost:3000", "127.0.0.1:3000"));
        assert!(same_port("127.0.0.1:3000", "127.0.0.1:3000"));
        assert!(!same_port("127.0.0.1:3000", "127.0.0.2:3000"));
        assert!(!same_port("0.0.0.0:3000", "0.0.0.0:3001"));
        assert!(!same_port("0.0.0.0:0", "0.0.0.0:0"));
    }

    #[test]
    fn paths_compare_by_where_they_lead() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a");
        std::fs::create_dir(&a).unwrap();

        assert_eq!(
            comparable(Path::new("../a/./data"), &a),
            comparable(Path::new("data"), &a)
        );
        assert!(
            comparable(Path::new("data/state"), &a).starts_with(comparable(Path::new("data"), &a))
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_resolve_before_parent_steps() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let real = root.join("real/x");
        std::fs::create_dir_all(&real).unwrap();
        std::os::unix::fs::symlink(&real, root.join("link")).unwrap();

        // `link/..` is `real`, where the kernel steps out of the link's target.
        assert_eq!(
            comparable(Path::new("../data"), &root.join("link")),
            root.join("real/data")
        );
        assert_eq!(
            comparable(Path::new("data/../../y"), &root.join("link")),
            root.join("real/y")
        );
    }
}
