// Watch transactions that touch a specific address.
//
// `watchTxForAddress` filters server-side on `TxPattern.hasAddress.exactAddress`
// — a tx matches if the address appears in any of its inputs or outputs.
//
//   ADDRESS=addr_test1... npm run watch:address
//
// ADDRESS accepts a bech32 address or raw address bytes as hex.

import {
  watchClient,
  addrToBytes,
  runStream,
  startPoint,
  DEFAULT_ADDRESS,
  c,
  printTx,
} from "./shared";

const ADDRESS = process.env.ADDRESS ?? DEFAULT_ADDRESS;

async function main() {
  const address = addrToBytes(ADDRESS);
  console.log(`👀 watching txs that touch ${c.address(ADDRESS)}\n`);

  await runStream(watchClient().watchTxForAddress(address, startPoint()), printTx);
}

main();
