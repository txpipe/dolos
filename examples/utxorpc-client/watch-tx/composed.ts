// Compose several patterns into one predicate.
//
// The lean examples each use a single convenience filter. Underneath, they all
// build a `TxPredicate`, and `watchTxByPredicate` lets you compose those with
// boolean operators:
//
//   { match }  — a single TxPattern
//   { anyOf }  — OR of sub-predicates
//   { allOf }  — AND of sub-predicates
//   { not }    — NONE of the sub-predicates may match
//
// This example builds:  (touches ADDRESS  OR  moves POLICY_ID)  AND NOT (touches NOISE)
//
//   ADDRESS=addr_test1... POLICY_ID=<hex> NOISE=addr_test1... npm run watch:composed
//
// Provide ADDRESS and/or POLICY_ID (at least one). NOISE is optional.

import {
  watchClient,
  addrToBytes,
  hexToBytes,
  runStream,
  startPoint,
  DEFAULT_ADDRESS,
  DEFAULT_POLICY,
  c,
  printTx,
} from "./shared";

const ADDRESS = process.env.ADDRESS ?? DEFAULT_ADDRESS;
const POLICY_ID = process.env.POLICY_ID ?? DEFAULT_POLICY;
const NOISE = process.env.NOISE; // optional address to exclude

async function main() {
  const predicate: any = {
    anyOf: [
      { match: { hasAddress: { exactAddress: addrToBytes(ADDRESS) } } },
      { match: { movesAsset: { policyId: hexToBytes(POLICY_ID) } } },
    ],
  };
  if (NOISE) predicate.not = [{ match: { hasAddress: { exactAddress: addrToBytes(NOISE) } } }];

  console.log("👀 watching with composed predicate:");
  console.log(`   ${c.dim("anyOf:")} touches ${c.address(`${ADDRESS.slice(0, 20)}…`)}  OR  moves policy ${c.asset(`${POLICY_ID.slice(0, 12)}…`)}`);
  if (NOISE) console.log(`   ${c.dim("and NOT")} touching ${c.address(NOISE)}`);
  console.log();

  await runStream(watchClient().watchTxByPredicate(predicate, startPoint()), printTx);
}

main();
