pub fn print_init_banner() {
    println!(
        r#"
888888ba           dP
88    `8b          88
88     88 .d8888b. 88 .d8888b. .d8888b.
88     88 88'  `88 88 88'  `88 Y8ooooo.
88    .8P 88.  .88 88 88.  .88       88
8888888P  `88888P' dP `88888P' `88888P'"#
    );

    println!("\x1b[90moooooooooooooooooooooooooooooooooooooooo\x1b[0m");

    println!(
        "\x1b[1;95mv{} ({})\x1b[0m\n",
        env!("CARGO_PKG_VERSION"),
        git_sha()
    );
}

/// What vergen emits in place of a value it cannot compute, such as the
/// commit of a build from a source tarball.
const VERGEN_PLACEHOLDER: &str = "VERGEN_IDEMPOTENT_OUTPUT";

/// The commit of the running build, or `unknown` when it was built without
/// one.
pub fn git_sha() -> &'static str {
    known_sha(option_env!("VERGEN_GIT_SHA"))
}

fn known_sha(emitted: Option<&'static str>) -> &'static str {
    match emitted {
        Some(sha) if !sha.is_empty() && sha != VERGEN_PLACEHOLDER => sha,
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_commit_is_kept() {
        assert_eq!(known_sha(Some("8ce5fbf6")), "8ce5fbf6");
    }

    #[test]
    fn a_missing_or_placeholder_commit_is_unknown() {
        assert_eq!(known_sha(None), "unknown");
        assert_eq!(known_sha(Some("")), "unknown");
        assert_eq!(known_sha(Some(VERGEN_PLACEHOLDER)), "unknown");
    }
}
