<div align="center">
    <h1>Dolos: Cardano Data Node</h1>
    <img alt="GitHub" src="https://img.shields.io/github/license/txpipe/dolos" />
    <img alt="GitHub Workflow Status" src="https://img.shields.io/github/actions/workflow/status/txpipe/dolos/validate.yml" />
    <hr/>
</div>

## Introduction

Dolos is a new type of Cardano node, fine-tuned to solve a very narrow scope: keeping an updated copy of the ledger and replying to queries from trusted clients, while requiring a small fraction of the resources

## Getting started

You can find comprehensive instructions on how to use Dolos in our [end-user documentation site](https://dolos.txpipe.io). The simplest way to get started is following our [quickstart guide](https://dolos.txpipe.io/quickstart).

## For Contributors

PRs are welcome. Start by cloning the repo and using cargo to run a local node. We use `cargo release` for release management and `cliff` for changelog updates.

### Running Tokio Console

Some times is useful to observe the details of all running tokio tasks. Dolos supports [tokio console](https://github.com/tokio-rs/console) as a dev-only feature. Follow these instructions to get it running:

1. Ensure you have enable tokio traces in dolos.toml:

```toml
[logging]
max_level=trace
include_tokio=true
```

2. Start Dolos' process using this command:

```
RUSTFLAGS="--cfg tokio_unstable" cargo --features debug run
```

3. Start tokio console in a different terminal:

```
tokio-console
```