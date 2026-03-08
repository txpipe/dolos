<div align="center">
  <img src="docs/assets/logo2.png" alt="Dolos Logo" width="200">
  <h1>Dolos</h1>
  <p><strong>A Cardano Data Node</strong></p>
  
  <a href="https://github.com/txpipe/dolos/blob/main/LICENSE"><img src="https://img.shields.io/github/license/txpipe/dolos?style=for-the-badge&color=blue" alt="License: Apache-2.0"></a>
  <a href="https://github.com/txpipe/dolos/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/txpipe/dolos/ci.yml?style=for-the-badge&label=CI" alt="CI Status"></a>
  <a href="https://crates.io/crates/dolos"><img src="https://img.shields.io/crates/v/dolos?style=for-the-badge&color=orange" alt="Crates.io"></a>
  <a href="https://dolos.txpipe.io"><img src="https://img.shields.io/badge/docs-dolos.txpipe.io-blue?style=for-the-badge" alt="Documentation"></a>
  
  <br>
  <br>
</div>

## What is Dolos?

Cardano nodes traditionally assume one of two roles: block producer or relay. Dolos introduces a third role: the **data node** — optimized for keeping an updated ledger and responding to queries while requiring a fraction of the resources.

Dolos connects directly to the Cardano network using Ouroboros Node-to-Node (N2N) mini-protocols (via [Pallas](https://github.com/txpipe/pallas)). It relies on honest upstream peers rather than performing full consensus validation, enabling significant resource savings.

- **Low resource footprint** — Runs with a small fraction of the memory and CPU required by a traditional Cardano node
- **Rich API surface** — Everything needed to build dApps, wallets, and explorers: UTxOs, transactions, blocks, accounts, assets, pools, governance data, and scripts
- **Full multi-era support** — Handles all Cardano eras from Byron through Conway, including full governance support (DReps, proposals, voting)

## Operating Modes

Dolos offers three distinct operating profiles, allowing you to choose the right trade-off between storage requirements and query capabilities:

| Mode | Description | Best For |
|------|-------------|----------|
| **Ledger-only** | Current chain state only (UTxO set, pools, stake distribution, protocol params, reference scripts) — minimal disk footprint | Services that only need the current ledger state |
| **Sliding history** | Configurable retention window for historical data (transactions, blocks, rewards, delegations within window) | Most dApps that need recent history |
| **Full archive** | Complete chain history from genesis | Explorers, analytics, and archival services |

## Features

### APIs & Integrations

Dolos provides multiple interfaces for accessing Cardano data, making it compatible with existing tooling:

- **Blockfrost-compatible REST API (MiniBF)** — Drop-in replacement for Blockfrost with endpoints for blocks, transactions, accounts, addresses, assets, pools, governance, and scripts
- **Kupo-compatible HTTP (MiniKupo)** — Pattern-based UTxO matching for chain-indexing workflows
- **gRPC (UTxO RPC)** — High-performance streaming API following the [UTxO RPC](https://utxorpc.org) specification, with gRPC-Web support for browser clients
- **TRP (Transaction Resolver Protocol)** — JSON-RPC endpoint for Tx3 framework integration
- **Ogmios compatibility** — Supports Ogmios-style queries for familiar local state query workflows
- **Cardano CLI compatibility** — Use `cardano-cli` local state queries against Dolos

### Data Capabilities

- **Full historical reward logs** — Complete reward distribution calculations and epoch state management
- **Stake distribution snapshots** — Historical stake snapshots and epoch boundary logic
- **Pool registry & metadata** — Pool registration, retirement handling, and delegator tracking
- **Asset registry** — Token and NFT metadata tracking with CIP-25 support
- **Script indexing** — Support for Native scripts and Plutus V1/V2/V3
- **Governance data** — DRep registration, proposals, and voting state (Conway era)

### Developer Experience

- **Mempool-aware transaction submit** — Tracks pending, inflight, and finalized UTxO states, enabling transaction chaining workflows
- **Local devnet mode** — Ephemeral single-node network via Tx3 tooling for offline development (resets on restart)
- **Fast Mithril bootstrap** — Sync mainnet from Mithril snapshot in under 20 hours
- **Dolos snapshots** — Export and load node state in minutes for rapid deployment
- **Multi-platform binaries** — Native packages for macOS (Apple Silicon), Linux (ARM64/x64), Windows x64, plus Docker images

### Operations & Observability

- **Dual storage backends** — Choose between Redb v3 or Fjall LSM-tree based on your workload
- **OpenTelemetry integration** — Distributed tracing with OTLP export, focused on mempool operations
- **Prometheus metrics** — Health and performance monitoring endpoints
- **Rust implementation** — Memory safety, high performance, and small binary size

## Architecture

Dolos follows a modular, layered architecture:

- **Core abstractions** (`dolos-core`) — Storage traits (State, Archive, WAL, Index), entity-delta system, and batch processing pipeline
- **Cardano logic** (`dolos-cardano`) — Era-specific block processing, validation, reward calculation, and UTxO delta computation
- **Storage backends** — Pluggable implementations: Redb v3 or Fjall LSM-tree
- **Service layer** — gRPC, REST, and Ouroboros protocol servers

Data is organized into four isolated storage layers: State (current ledger), Archive (historical blocks), WAL (crash recovery), and Index (fast lookups). State mutations use an entity-delta pattern enabling efficient rollbacks without full snapshots.

## Quick Start

```bash
# macOS
brew install txpipe/tap/dolos

# Linux
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/txpipe/dolos/releases/latest/download/dolos-installer.sh | sh

# Windows (PowerShell)
powershell -c "irm https://github.com/txpipe/dolos/releases/latest/download/dolos-installer.ps1 | iex"

# Docker
docker run ghcr.io/txpipe/dolos:latest
```

Once installed:

```bash
dolos init       # Interactive configuration
dolos bootstrap  # Sync from Mithril snapshot
dolos daemon     # Start the node
```

📖 **Full documentation**: [https://dolos.txpipe.io](https://dolos.txpipe.io)

## Contributing

PRs are welcome! Please ensure your changes pass CI checks:

```bash
cargo clippy --workspace --all-targets --all-features
cargo test --workspace --all-features
```

See [CONTRIBUTING.md](.github/CONTRIBUTING.md) for guidelines.

## License

Dolos is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
