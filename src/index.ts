import { join } from "node:path";

import applicationConfigPath from "application-config-path";
import { restoreSessionOrStartLoginFlow } from "movistar-cloud/login/flow";
import { MovistarCloudSessionStore } from "movistar-cloud/login/sessions";
import PQueue from "p-queue";

import { phoneNumber } from "./env";
import { main } from "./fuse";

const appConfigDir = applicationConfigPath("movistar-cloud-ts-fuse");

const sessions = new MovistarCloudSessionStore(
  join(appConfigDir, "sessions.json"),
);

const pQueue = new PQueue({
  concurrency: 3,
  interval: 5000,
  intervalCap: 25,
  strict: true,
  timeout: 120_000,
});

const mv = await restoreSessionOrStartLoginFlow(sessions, phoneNumber, {
  pQueue,
});

if (!mv) {
  process.exit(1);
}

setInterval(() => {
  console.log(
    "PQueue size:",
    pQueue.size,
    "\tis saturated:",
    pQueue.isSaturated,
  );
}, 1000).unref();

await main(mv);
