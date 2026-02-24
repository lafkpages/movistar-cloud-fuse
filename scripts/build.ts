import { $ } from "bun";

// See https://u.luisafk.dev/kFAde
$.throws(true).env({
  FORCE_COLOR: Bun.enableANSIColors ? "1" : undefined,
});

const isDev = process.argv.includes("--dev");

await $`rm -rf dist`;
await $`bun build src/index.ts --target=node --outdir=dist ${isDev ? "--sourcemap=inline" : ""} ${isDev ? "--env=MOVISTAR_CLOUD_*" : "--production"}`;
