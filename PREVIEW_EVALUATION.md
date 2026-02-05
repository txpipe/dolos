# Preview Conformance Failures - Evaluation Report

## Executive Summary

Total Failures: **10**

| Category | Count | Status |
|----------|-------|--------|
| Pools | 6 | Mixed (3 Won't Fix, 3 Pending) |
| Accounts | 2 | Pending |
| Epochs | 1 | Pending |
| Network | 1 | Pending |

**Legend:**
- **Pending**: Requires fix (Mapping or Data Model).
- **Won't Fix**: Documented but out of scope.

---

## Won't Fix

The following failures are documented but will not be addressed in this release:

### pools/:pool_id/updates Endpoint

**Affected Tests:**
- `pools/:pool_id/updates metadata mismatch - status field`

**Error:** `status` field mismatch - expected "registered" vs "active"

**Reason:** Not part of endpoints included in scope of work for this release. Will be addressed in open-source effort after delivery date.

**Priority:** Low

---

### pools/:pool_id/metadata Endpoint

**Affected Tests:**
- `pools/:pool_id/metadata offchain metadata - homepage field`

**Error:** `homepage` field has unexpected value format

**Reason:** Not part of endpoints included in scope of work for this release. Will be addressed in open-source effort after delivery date.

**Priority:** Low

---

### pools/:pool_id/history Endpoint

**Affected Tests:**
- `pools/:pool_id/history active_epoch` (epoch boundary issues)

**Error:** `active_epoch` value mismatch (off by one epoch)

**Reason:** Not part of endpoints included in scope of work for this release. Will be addressed in open-source effort after delivery date.

**Priority:** Low

---

## Outstanding Failures (Preview)

### ACCOUNTS

**Test:** accounts/:stake_address retired drep
- **Endpoint:** `accounts/stake_test1uq70zpxr7jdqxdlj895x9lvnwn9lrcknwpx8cswlld7x76gtzvrjp`
- **Error:** `drep_id` field should be null for retired DRep but shows a value

**Test:** accounts/:stake_address when DRep is retired all delegators to that DRep should have their drep_id cleared
- **Endpoint:** `accounts/stake_test1uq70zpxr7jdqxdlj895x9lvnwn9lrcknwpx8cswlld7x76gtzvrjp`
- **Error:** DRep clearing logic not implemented

---

### EPOCHS

**Test:** epochs/:number/parameters epoch 300 params
- **Endpoint:** `epochs/300/parameters`
- **Error:** `nonce` field is empty (`""`) but expected to have a value

---

### NETWORK

**Test:** network test
- **Endpoint:** `network`
- **Error:** Test timed out in 15000ms

---

### POOLS

**Test:** pools/extended output matches data returned from /pools/:pool_id and /pools/:pool_id/metadata
- **Endpoint:** `pools/extended?count=1&page=1`
- **Error:** Response code 404 (Not Found) or data mismatch

---

## Summary by Category

### Accounts (2 failures)
Both failures relate to DRep handling when DReps are retired. The system does not properly clear the `drep_id` field for accounts that were delegated to a retired DRep.

### Epochs (1 failure)
Single failure related to epoch parameters where the `nonce` field is missing/empty for epoch 300.

### Network (1 failure)
Timeout issue on the network endpoint, likely related to the same stake calculation caching issue seen in mainnet.

### Pools (3 pending failures)
- pools/extended endpoint 404 errors (needs to be fixed - in scope)
- Epoch boundary issues in pool history (won't fix - out of scope)

---

## Recommendations

1. **Priority 1:** Fix network endpoint timeout (cache stake calculations)
2. **Priority 2:** Implement DRep clearing logic for retired DReps
3. **Priority 3:** Fix epoch parameter nonce field
4. **Priority 4:** Pool metadata/status field alignment
