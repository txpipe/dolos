use axum::body::Body;
use axum::http::{Request, Uri};
use dolos_core::config::RootConfig;
use http_body_util::BodyExt;
use miette::{Context, IntoDiagnostic};
use std::io::Write;
use tower::ServiceExt;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// Minibf path (supports querystrings)
    #[arg(value_name = "PATH")]
    path: String,
}

#[tokio::main]
pub async fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    let minibf = config
        .serve
        .minibf
        .as_ref()
        .ok_or(miette::miette!("missing minibf config"))?;

    let domain = crate::common::setup_domain(config)?;

    let path = if args.path.starts_with('/') {
        args.path.trim().to_string()
    } else {
        format!("/{}", args.path.trim())
    };

    // When `base_path` has a value, the router serves each route with that prefix.
    // This code adds the same prefix to the CLI path. If the path already has
    // the prefix, this code does not add the prefix again.
    let base_path = minibf.base_path();
    let path = add_base_path(path, base_path.as_deref());

    let uri: Uri = path
        .parse()
        .into_diagnostic()
        .context("invalid minibf path")?;

    let app = dolos_minibf::build_router(minibf.clone(), domain);

    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .into_diagnostic()
        .context("building minibf request")?;

    let response = app
        .oneshot(request)
        .await
        .into_diagnostic()
        .context("executing minibf query")?;

    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .into_diagnostic()
        .context("reading minibf response body")?
        .to_bytes();

    if status.is_success() {
        std::io::stdout()
            .write_all(&body)
            .into_diagnostic()
            .context("writing minibf response")?;
        Ok(())
    } else {
        let message = String::from_utf8_lossy(&body);
        Err(miette::miette!(
            "minibf query failed with status {}: {}",
            status,
            message
        ))
    }
}

fn add_base_path(path: String, base_path: Option<&str>) -> String {
    let Some(base_path) = base_path else {
        return path;
    };

    let has_base_path = path.strip_prefix(base_path).is_some_and(|suffix| {
        suffix.is_empty() || suffix.starts_with('/') || suffix.starts_with('?')
    });

    if has_base_path {
        path
    } else {
        format!("{base_path}{path}")
    }
}

#[cfg(test)]
mod tests {
    use super::add_base_path;

    #[test]
    fn base_path_before_query_is_not_added_again() {
        let path = "/api/v0?foo=bar";

        assert_eq!(add_base_path(path.into(), Some("/api/v0")), path);
    }
}
