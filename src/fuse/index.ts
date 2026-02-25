import type { MovistarCloudClient } from "movistar-cloud";
import type { OpsCache } from "./cache";
import type { LocalFiles, OpenFiles } from "./open-files";

import { mkdir } from "node:fs/promises";

import Fuse from "@cocalc/fuse-native";

import { cacheDir, mountPath, volname } from "../env";
import { getOps } from "./ops";

const opsCache: OpsCache = {
  readdir: new Map(),
  getattr: new Map(),
};

// Track locally-created files by path → cache file path, for reads before remote sync
const localFiles: LocalFiles = new Map();

const openFiles: OpenFiles = new Map();
let fdCounter = 100;

export async function main(mv: MovistarCloudClient) {
  await mkdir(cacheDir, { recursive: true });

  await mv.listRoots();

  if (!mv.rootFolderId) {
    throw new Error(
      `Expected a root folder ID in the MovistarCloudClient instance`,
    );
  }

  const { rootFolderId } = mv;

  const fuse = new Fuse(
    mountPath,
    getOps({ mv, rootFolderId, opsCache, localFiles, openFiles, fdCounter }),
    {
      volname,
      mkdir: true,

      timeout: 120_000,

      // debug: true,
    },
  );

  fuse.mount((err) => {
    if (err) {
      throw err;
    }

    console.log(`\n✅ Drive successfully mounted at ${mountPath}`);
  });

  // Handle Ctrl+C gracefully to unmount
  process.on("SIGINT", function () {
    fuse.unmount((err) => {
      if (err) {
        console.error("Error unmounting drive:", err);
        process.exit(1);
      }

      console.log("\nUnmounted.");
      process.exit();
    });
  });
}
