import { o$ } from ".";

const isDev = process.argv.includes("--dev");

await o$`rm -rf dist`;
await o$`bun build src/index.ts --target=node --outdir=dist ${isDev ? "--sourcemap=inline" : ""} ${isDev ? "--env=MOVISTAR_CLOUD_*" : "--production"}`;
