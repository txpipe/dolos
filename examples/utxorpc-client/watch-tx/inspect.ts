// Inspect the full content of every matched transaction.
//
// The other examples are about *filtering*; this one is about the *payload*. For
// each matched tx it prints a readable breakdown: hash, fee, inputs (with their
// resolved output when the node provides it), outputs with ADA + native assets,
// mint/burn, and a certificate count.
//
//   npm run watch:inspect                       # firehose (every tx)
//   ADDRESS=addr_test1... npm run watch:inspect  # only txs touching an address
//   POLICY_ID=<hex> npm run watch:inspect        # only txs moving a policy
//
// It doubles as a discovery tool: run the firehose, then copy an address or
// policy id out of the output to use with the filtering examples.

import {
  watchClient,
  addrToBytes,
  hexToBytes,
  runStream,
  startPoint,
  c,
  printTx,
} from "./shared";

const ADDRESS = process.env.ADDRESS;
const POLICY_ID = process.env.POLICY_ID;

async function main() {
  const watch = watchClient();
  const from = startPoint();

  let stream: AsyncIterable<any>;
  if (ADDRESS) {
    console.log(`👀 inspecting txs touching ${c.address(ADDRESS)}\n`);
    stream = watch.watchTxForAddress(addrToBytes(ADDRESS), from);
  } else if (POLICY_ID) {
    console.log(`👀 inspecting txs moving policy ${c.asset(POLICY_ID)}\n`);
    stream = watch.watchTxForAsset(hexToBytes(POLICY_ID), undefined, from);
  } else {
    console.log("👀 inspecting ALL txs (firehose)\n");
    stream = watch.watchTx(from);
  }

  await runStream(stream, printTx);
}

main();
