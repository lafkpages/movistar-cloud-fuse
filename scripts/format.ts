import { which } from "bun";

import { o$ } from ".";

await o$`bun run -b prettier --write .`;

const yek = which("yek");
if (yek) {
  await o$`${yek} > repo.txt`;
}
