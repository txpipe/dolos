// Watch transactions that move a particular native asset.
//
// `watchTxForAsset` filters server-side on `TxPattern.movesAsset` — a tx matches
// if the given policy (and, optionally, asset name) appears in any input or
// output. Pass just the policy id to follow a whole collection/token family, or
// add an asset name to pin a single asset.
//
//   POLICY_ID=<hex> npm run watch:asset
//   POLICY_ID=<hex> ASSET_NAME=<hex> npm run watch:asset
//
// Defaults to a policy known to move in the replayed range, so it matches out of
// the box.

import {
  watchClient,
  hexToBytes,
  runStream,
  startPoint,
  DEFAULT_POLICY,
  c,
  printTx,
} from "./shared";

const POLICY_ID = process.env.POLICY_ID ?? DEFAULT_POLICY;
const ASSET_NAME = process.env.ASSET_NAME;

async function main() {
  const policyId = hexToBytes(POLICY_ID);
  const assetName = ASSET_NAME ? hexToBytes(ASSET_NAME) : undefined;
  console.log(
    `👀 watching txs moving policy ${c.asset(POLICY_ID)}` +
      (ASSET_NAME ? ` / asset ${c.asset(ASSET_NAME)}` : "") +
      "\n",
  );

  const stream = watchClient().watchTxForAsset(policyId, assetName, startPoint());

  await runStream(stream, printTx);
}

main();
