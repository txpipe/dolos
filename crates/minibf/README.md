# Dolos Minibf

Blockfrost-compatible HTTP API service for the Dolos Cardano data node.

## Overview

`dolos-minibf` provides a REST API that mimics the Blockfrost API, allowing existing Cardano ecosystem tools to work seamlessly with Dolos without requiring code changes. It serves as an API compatibility layer for existing Blockfrost clients.

## Features

### API Compatibility
- **Blockfrost v0**: Full compatibility with Blockfrost API v0 endpoints

### Endpoints
- **Block Information**: Block details, transactions, and metadata
- **Transaction Data**: Transaction details, inputs, outputs, and metadata
- **Account Information**: Address balances, transaction history
- **Asset Information**: Token metadata, policies, and asset details
- **Network Data**: Era summaries, protocol parameters
- **Pool Information**: Stake pool details and delegation data

### Data Transformation
- **Blockfrost Format**: Converts Dolos internal data to Blockfrost response format
- **Pagination**: Blockfrost-style pagination for large result sets
- **Filtering**: Support for Blockfrost query parameters and filters

## Usage


### Configuration
```toml
[minibf]
enabled = true
port = 8080
host = "0.0.0.0"
```

## Architecture

### HTTP Server
- **Framework**: Built on Axum for high performance
- **Error Handling**: Blockfrost-compatible error responses
- **Health Checks**: Service health and readiness endpoints

### Data Access
- **Storage Integration**: Reads from Dolos storage backends
- **Async Processing**: Non-blocking I/O for high concurrency

## Dependencies

- **dolos-core**: Core domain and storage abstractions
- **dolos-cardano**: Cardano-specific data structures
- **axum**: Web framework for HTTP server
- **tokio**: Async runtime
- **serde**: JSON serialization/deserialization

## Testing Strategy

`dolos-minibf` uses a layered test harness so we can validate Blockfrost compatibility
without running a full node:

- **Synthetic data**: Tests use `dolos-testing` to generate small, deterministic
  Conway blocks and transactions. These blocks are imported into a `ToyDomain`
  during test setup, so the endpoints exercise the same storage paths as real data.
- **Mocked domain**: The test harness builds a `ToyDomain` with a synthetic
  `CardanoConfig` and imports blocks via the core import pipeline.
- **Happy path coverage**: Each endpoint has a “200” test that deserializes the
  response into the corresponding `blockfrost-openapi` model to ensure schema
  compatibility.
- **Fault injection**: The harness can wrap the domain with `TestFault` to
  force store errors (State/Archive/WAL/Index). This drives consistent **500**
  responses without modifying production code.
- **Client-side validation**: Tests use a shared `TestApp` helper to call routes
  without starting an HTTP server, ensuring fast and deterministic execution.
- **404/400 behavior**: Where the OpenAPI spec requires “not found” or “bad
  request,” tests assert those codes to keep API behavior aligned with the spec.

Key modules:

- `crates/minibf/src/test_support.rs` – test harness + synthetic vectors
- `crates/testing/src/synthetic.rs` – synthetic block/tx generator
- `crates/testing/src/faults.rs` – fault injection used by tests

## Performance

- **Low Latency**: Sub-millisecond response times for cached data
- **High Throughput**: Thousands of requests per second
- **Memory Efficient**: Minimal memory footprint with streaming responses
- **Concurrent**: Parallel request processing

## Integration

### Existing Tools
- **Blockfrost SDKs**: Full compatibility with official SDKs
- **Explorer Frontends**: Drop-in replacement for Blockfrost API
- **Wallet Software**: Compatible with wallet integrations

## Related Crates

- [`dolos-core`](../dolos-core/README.md) - Core storage abstractions
- [`dolos-cardano`](../dolos-cardano/README.md) - Cardano data provider
