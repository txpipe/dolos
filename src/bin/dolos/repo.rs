//! How an operator names an OCI repository on the command line.
//!
//! One type, shared by the two commands that take one — `dolos snapshot publish
//! --repo` and `dolos bootstrap stelae --source` — because the grammar a
//! repository name has to satisfy is the registry's and not either command's. A
//! second copy would be a second answer to the same question, and the one that
//! drifted would be the one an operator met.

/// An OCI repository, as `oci://HOST/PATH`.
///
/// Split into the registry host and the repository path the way a registry
/// client wants them, and refused rather than guessed at where the URL says
/// something neither command can honour. A `:tag` or `@digest` is refused in
/// particular: the tag a stele lives under is the profile's — `epoch-{n}` and
/// `latest` — so a URL naming one is an operator expecting something that will
/// not happen. On the restore side there is a flag that *does* name a stele,
/// `--point`, and it takes the profile's spelling.
///
/// The repository *name* is held to the distribution grammar — lowercase
/// components, `.`/`_`/`-` separators, no empty segments — by handing it to the
/// parser `oci-client` already ships rather than by a second copy of that
/// grammar living here. It is a validator only: its own splitter applies
/// registry defaults (a bare name becomes `docker.io/...`) that would quietly
/// rewrite what the operator typed, so the split above stays ours. Without the
/// check, `oci://ghcr.io//txpipe/dolos` or an uppercase component is accepted
/// here and refused by the registry, hours later, in its words rather than
/// ours.
#[derive(Debug, Clone)]
pub struct RepoRef {
    pub host: String,
    pub repository: String,
}

impl std::str::FromStr for RepoRef {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let bad = |why: &str| format!("{raw:?} is not an OCI repository: {why}");

        let rest = raw
            .strip_prefix("oci://")
            .ok_or_else(|| bad("it does not start with `oci://`"))?;

        let (host, repository) = rest
            .split_once('/')
            .ok_or_else(|| bad("it names a registry but no repository path"))?;

        if host.is_empty() {
            return Err(bad("it names no registry host"));
        }

        if repository.is_empty() || repository.ends_with('/') {
            return Err(bad("it names no repository path"));
        }

        // A host may carry a port, so only the path is checked for a reference.
        if repository.contains(':') || repository.contains('@') {
            return Err(bad(
                "it names a tag or a digest, and the tag a stele lives under is the profile's",
            ));
        }

        // Checked here, discarded here: what is wanted is the grammar's verdict
        // on the name, not the reference it would build out of it.
        //
        // Gated because the parser arrives with the registry client. A build
        // without one refuses an `oci://` URL outright, so the check it cannot
        // make is one nothing would reach.
        #[cfg(feature = "registry")]
        rest.parse::<dolos_snapshot::registry::Reference>()
            .map_err(|_| bad("its repository path is not a valid OCI name"))?;

        Ok(Self {
            host: host.to_owned(),
            repository: repository.to_owned(),
        })
    }
}

impl std::fmt::Display for RepoRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "oci://{}/{}", self.host, self.repository)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_repository_splits_into_a_host_and_a_path() {
        let parsed: RepoRef = "oci://ghcr.io/txpipe/dolos-snapshots/mainnet"
            .parse()
            .unwrap();

        assert_eq!(parsed.host, "ghcr.io");
        assert_eq!(parsed.repository, "txpipe/dolos-snapshots/mainnet");

        // A port belongs to the host, which is what makes the tag check below
        // safe to run on the path alone.
        let local: RepoRef = "oci://127.0.0.1:5000/dolos".parse().unwrap();

        assert_eq!(local.host, "127.0.0.1:5000");
        assert_eq!(local.repository, "dolos");

        // And what it prints is what it parsed, so an error message naming a
        // repository names the one the operator typed.
        assert_eq!(local.to_string(), "oci://127.0.0.1:5000/dolos");
    }

    #[test]
    fn a_nonsensical_repository_is_refused() {
        for raw in [
            "ghcr.io/txpipe/dolos",                  // no scheme
            "https://ghcr.io/txpipe/dolos",          // the wrong scheme
            "oci://ghcr.io",                         // no repository path
            "oci://ghcr.io/",                        // still no repository path
            "oci:///txpipe/dolos",                   // no host
            "oci://ghcr.io/txpipe/dolos/",           // a trailing slash
            "oci://ghcr.io/txpipe/dolos:v1",         // a tag is the profile's to render
            "oci://ghcr.io/txpipe/dolos@sha256:abc", // and so is a digest
            "",
        ] {
            assert!(raw.parse::<RepoRef>().is_err(), "{raw:?}");
        }
    }

    /// Names the distribution grammar refuses, which a split on `/` alone
    /// cannot see.
    ///
    /// Each of these reaches the registry as part of the request path, so
    /// accepting them here buys the operator an opaque error from someone
    /// else's server at the end of a publish rather than a sentence from this
    /// command at the start of one.
    #[cfg(feature = "registry")]
    #[test]
    fn a_repository_path_outside_the_grammar_is_refused() {
        for raw in [
            "oci://ghcr.io//txpipe/dolos",      // an empty component
            "oci://ghcr.io/txpipe//dolos",      // an empty component, inside
            "oci://ghcr.io/TxPipe/dolos",       // uppercase; names are lowercase
            "oci://ghcr.io/txpipe/dolos?x=1",   // a query
            "oci://ghcr.io/txpipe/dolos#frag",  // a fragment
            "oci://ghcr.io/txpipe/dolos snaps", // whitespace
            "oci://ghcr.io/txpipe/-dolos",      // a component opening on a separator
        ] {
            assert!(raw.parse::<RepoRef>().is_err(), "{raw:?}");
        }
    }

    /// And the shapes it allows, so the check above is not quietly refusing
    /// everything.
    #[cfg(feature = "registry")]
    #[test]
    fn ordinary_repository_paths_still_parse() {
        for raw in [
            "oci://ghcr.io/txpipe/dolos-snapshots/mainnet",
            "oci://ghcr.io/txpipe/dolos_snapshots",
            "oci://ghcr.io/txpipe/dolos.snapshots",
            "oci://localhost:5000/dolos/mainnet",
            "oci://127.0.0.1:5000/dolos",
        ] {
            assert!(raw.parse::<RepoRef>().is_ok(), "{raw:?}");
        }
    }
}
