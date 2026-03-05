use axum::body::Body;
use axum::http::{Request, Uri};
use dolos_core::config::RootConfig;
use http_body_util::BodyExt;
use miette::{Context, IntoDiagnostic};
use std::io::Write;
use tower::ServiceExt;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// MiniKupo path (supports querystrings)
    #[arg(value_name = "PATH")]
    path: String,
}

#[tokio::main]
pub async fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    let minikupo = config
        .serve
        .minikupo
        .as_ref()
        .ok_or(miette::miette!("missing minikupo config"))?;

    let domain = crate::common::setup_domain(config)?;

    let path = if args.path.starts_with('/') {
        args.path.trim().to_string()
    } else {
        format!("/{}", args.path.trim())
    };

    let uri: Uri = path
        .parse()
        .into_diagnostic()
        .context("invalid minikupo path")?;

    let app = dolos_minikupo::build_router(minikupo.clone(), domain);

    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .into_diagnostic()
        .context("building minikupo request")?;

    let response = app
        .oneshot(request)
        .await
        .into_diagnostic()
        .context("executing minikupo query")?;

    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .into_diagnostic()
        .context("reading minikupo response body")?
        .to_bytes();

    if status.is_success() {
        std::io::stdout()
            .write_all(&body)
            .into_diagnostic()
            .context("writing minikupo response")?;
        Ok(())
    } else {
        let message = String::from_utf8_lossy(&body);
        Err(miette::miette!(
            "minikupo query failed with status {}: {}",
            status,
            message
        ))
    }
}
