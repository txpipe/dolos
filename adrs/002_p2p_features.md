# ADR 002 - P2P Features

## Status

Proposed

## Context

- The current version of Dolos (v0.19) supports a single upstream node.
- Dolos initiates communication using node-to-node mini-protocols to a well-known peer specified via config.
- Chain-sync, block-fetch and tx-submit operations are performed against this single upstream peer.
- Dolos doesn't execute consensus checks, it trusts the upstream peer data. This is by design, because Dolos aims to be a lightweight data-provider and not a full-node.

## Decision

- Refactor the network layer to support connections to multiple upstream peers.
- Support a fixed topology via config that allows users to specify multiple well-known peers.
- Support for querying the ledger state for well-known relay data for stake pools.
- Implement peer-sharing mini-protocol to discover new peers dynamically.
- Define a churn mechanism for rotation of active peers.
- Refactor chain sync pipeline so that it performs a naive chain-selection algorithm that follows the longest-chain rule.
- Refactor tx submission so that mempool txs fanout to all active peers.
- Refactor block fetching pipeline so that it fetches from the best active peer measuring by a latency score.

## Rationale

- Having multiple upstream peers would improve resilience of the data syncing pipeline by removing the current SPOF.
- Having dynamic peer discovery and a peer churn mechanism mitigates the risk of eclipse attacks.
- Having the ledger as source for relay data while using Mithril snapshots reduces the degree of trust required from the set of bootstrapping upstream peers.
- Having a block fetching mechanism that uses latency scores provides a potential performance improvement.
- Having a fanout mechanism that submits txs to multiple peers simultaneously provides a potential performance improvement.

## Limitations

- Although this feature improves the overall security and resilience of the system, it is not equivalent to implementing the full Ouroboros consensus algorithm. Dolos main goal remains as a lightweight layer with friendly API endpoints for interacting with the Cardano blockchain. Some minimal block-level validations are also in Dolos' roadmap with the goal of increasing the security guarantees without affecting the overall performance (eg: block issuer check, previous-block hash check, etc).
- Dolos still has a trust assumption against the initial, well-known peers that it uses for initialization. The topology used for bootstrapping a node is a crucial aspect of the operators tasks. Using Mithril snapshots and ledger discovery mitigates this concern.

## Performance Impact

- Having multiple upstream peers can have an impact on the resource footprint of a syncing Dolos daemon process. CPU and memory can be increased depending on the number of concurrent active peers.
- Having multiple concurrent chain-sync clients will have a linear increase in bandwidth proportional to the number of concurrent active peers without any improvement on block throughput.
- Performance impact is directly tied to the configuration parameters of the feature. Limiting the number of concurrent active peers or disabling the feature completely is a valid mechanism for users to tailor the tradeoff between performance & resilience.

## Alternatives Considered

1. **Single Peer with Failover**
   - Pros: Simpler implementation, lower resource usage
   - Cons: Still vulnerable to SPOF, no performance benefits

2. **Full Ouroboros Consensus Implementation**
   - Pros: Maximum security and decentralization
   - Cons: Overkill for Dolos's lightweight data-provider goals, significant complexity increase

3. **Static Peer List Only**
   - Pros: Simpler than dynamic discovery, predictable behavior
   - Cons: Less resilient to network changes, no automatic recovery

## Implementation Details

### New Configuration

- the `upstream` config section will support an array of initial upstream peers.

```toml
[upstream]
bootstrap_peers = ["peer1:3001", "peer2:2001"]
```

```toml
[upstream]
max_hot_peers = 5
max_warm_peers = 3
max_cold_peers = 30
```

- the `upstream` config section will support a setting for defining the churn period of an active peer.

```toml
[upstream]
peer_churn_period = 3600
```

- the `upstream` config section will support a flag to enable / disable the different types of dynamic peer discovery strategies.

```toml
[upstream]
ledger_peer_discovery = true
network_peer_discovery = true
```

### Network Manager

- a new network manager component will be created that will serve as a state machine for all of the outbound network connections.
- the manager will be in charge of boostrapping, peer discovery and churn mechanism.
- the manager will also serve as a facade for simplifying the interaction of miniprotocols agents against a set of multiple peers.

### Peer discovery

- If enabled via config, a peer-sharing query will be executed for each new hot peer. The response will be added to the set of cold peers after removing duplicates.
- If enabled via config, a leger query will be executed at the start of the bootstrapping process to access relay data.  The response will be added to the set of cold peers after removing duplicates.
- The peer discovery process will continue running indefinitely until the max number of cold peers has been reached.

### Chain Sync Pipeline

- Chain sync miniprotocol will be started and executed for each hot peer.
- Each new peer will attempt to intersect the chain from a known block from a few slots in the past of the current Dolos tip.
- A sliding window of received headers will be used to compare against forks and retain only a single one by following the longest chain rule.
- Block fetch will be executed only once against the peer with smallest latency score that also provided the corresponding header.

