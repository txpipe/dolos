# Dolos Minibf

Blockfrost-compatible HTTP API service for the Dolos Cardano data node.

## Overview

`dolos-minibf` provides a REST API that mimics the Blockfrost API, allowing existing Cardano ecosystem tools to work seamlessly with Dolos without requiring code changes. It serves as an API compatibility layer for existing Blockfrost clients.

### Blockfrost Spec Compliance

The official [Blockfrost OpenAPI specification](https://github.com/blockfrost/openapi) requires all endpoints to be served under the `/api/v0` base path. Dolos supports this via the `base_path` configuration option:

```toml
[serve.minibf]
listen_address = "[::]:3000"
base_path = "/api/v0"  # For full Blockfrost spec compliance
```

When `base_path` is set, **all routes** (including health and metrics) are served under that prefix (e.g., `/api/v0/blocks/latest`, `/api/v0/health`, `/api/v0/metrics`). If omitted, endpoints are served at the root for backward compatibility.

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
