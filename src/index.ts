import applicationConfigPath from "application-config-path";
import { restoreSessionOrStartLoginFlow } from "movistar-cloud/login/flow";

import { MovistarCloudSessionStore } from "movistar-cloud/login/sessions";
import { join } from "node:path";
import { phoneNumber } from "./env";
import { main } from "./fuse";

const appConfigDir = applicationConfigPath("movistar-cloud-ts-fuse");

const sessions = new MovistarCloudSessionStore(
  join(appConfigDir, "sessions.json"),
);

const mv = await restoreSessionOrStartLoginFlow(sessions, phoneNumber);

if (!mv) {
  process.exit(1);
}

await main(mv);
