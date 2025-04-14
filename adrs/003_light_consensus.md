# ADR 003 - Light Consensus

## Status

Proposed

## Context

- Dolos doesn't execute consensus checks nor ledger validations, it trusts the upstream peer data. This setup is by design, the main goal of Dolos is to be a lightweight data provider while a trusted full-node fulfills the role of providing the expected security guarantees.
- Performing the complete set of consensus and ledger logic increases the resource footprint of the process, defeating the purpose of the light-node.
- Depending on the risk-profile the operator is willing to assume, there're particular scenarios where the full set of validations (Consensus + Ledger) aren't required. An operator might prefer a setup where the upstream trust assumption is relaxed as long as the probability of a successful attack remains within a relatively low threshold.
- One of such trade-off is to skip transaction-level validations (aka: ledger validations) as long as the trust on the network consensus remains strong enough.

## Decision

- Implement the minimum set of validations that are required for assuring Ouroboros consensus guarantees as part of the chain-sync process
- Avoid ledger validations during chain-sync, assume that each transaction has been already validated by the upstream peer.

## Rationale

- The ledger validation is expensive, it requires many individual checks and interactions for each of the transactions in the block. In contrast, the consensus logic is relatively "cheap", since it only requires a few checks for each block header.
- The body hash available in the header can be used to assert the consistency and integrity of the block body. This means that although we don't perform any validation, we can guarantee that the block hasn't been tampered with.
- Implementing consensus checks removes part of the trust assumptions of the upstream peer. Checking consensus means that the network itself can be trusted to be providing the agreed version of the chain.

## Limitations

- Enforcing these consensus checks provides strong guarantees that the network is agreeing on a common view of the chain, but it's not enforcing that the network itself is behaving honestly. If an attacker has enough stake to control the whole Cardano network, it could forge blocks with invalid transactions and Dolos wouldn't be able to discriminate between valid and invalid blocks.

## Performance Impact

- Performing the required VRF cryptography checks on block headers will increase the baseline CPU usage. We expect this to be low enough during normal chain-sync.
- VRF checks require knowing the stake distribution of the network, maintaining this state will introduce new disk IO calls and more data being persisted. We expect this to be low enough.

## Alternatives Considered

1. **No changes**
   - Pros: Simpler operation, current resource usage profile.
   - Cons: High trust assumption on upstream peer, susceptible to eclipse attacks and long-range attacks.

2. **Full Consensus & Ledger Validation**
   - Pros: Same level of security as a full-node.
   - Cons: Overkill for Dolos's lightweight data-provider goals, significant complexity increase

## Implementation Details

### Block Header Validation

To support this feature, we need to introduce new operations as part of the existing chain-sync pipeline. For a block header to be accepted, the following checks need to be executed:

- body hash consistency
- previous hash equality
- VRF checks against stake distribution
- chain density (active slots over time)

### Chain Selection

Since Dolos will be receiving multiple headers from multiple peer, a chain-selection process needs to be implemented in order to pick the best candidate chain. This selection will be based on the following properties:

- Longest chain
- Chain with higher density

### Stake Distribution

VRF checks require knowledge of the stake distribution to complete. Keeping this state in Dolos requires the following changes:

- new storage kv table for delegated stake by pool
- new state update logic for:
  - pool registration / de-registration
  - stake registration / de-registration / delegation
  - rewards / withdrawals