import Fuse from "@cocalc/fuse-native";
import type { MovistarCloudClient } from "movistar-cloud";
import { mountPath, volname } from "../env";
import { createStat, dirStat } from "./stat";

export async function main(mv: MovistarCloudClient) {
  await mv.listRoots();

  if (!mv.rootFolderId) {
    throw new Error(
      `Expected a root folder ID in the MovistarCloudClient instance`,
    );
  }

  const { rootFolderId } = mv;

  const ops: Fuse.OPERATIONS = {
    async readdir(path, cb) {
      console.log("readdir(%s)", path);

      if (path === "/") {
        const folders = await mv.listFolders(rootFolderId);
        const files = await mv.listFiles(rootFolderId, ["name", "size"]);

        const names: string[] = [];
        const stats: Fuse.Stats[] = [];

        for (const f of folders) {
          names.push(f.name);
          stats.push(dirStat);
        }

        for (const f of files) {
          names.push(f.name!);
          stats.push(createStat({ size: f.size! }));
        }

        console.log(
          "Found %d folders and %d files",
          folders.length,
          files.length,
        );

        return cb(0, names, stats);
      }
      return cb(Fuse.ENOENT);
    },
    getattr(path, cb) {
      console.log("getattr(%s)", path);

      if (path === "/") {
        return cb(0, dirStat);
      }

      return cb(Fuse.ENOENT);
    },
    // open(path, flags, cb) {
    //   console.log("open(%s, %d)", path, flags);

    //   return cb(0, 42); // Return a dummy file descriptor
    // },
    // read(path, fd, buf, len, pos, cb) {
    //   console.log("read(%s, %d, %d, %d)", path, fd, len, pos);

    //   const str = "Hello World".slice(pos, pos + len);

    //   if (!str) {return cb(0);}

    //   buf.write(str);
    //   return cb(str.length);
    // },
  };

  const fuse = new Fuse(mountPath, ops, {
    debug: true,
    mkdir: true,
    volname,
  });

  fuse.mount((err) => {
    if (err) {
      throw err;
    }

    console.log(`\n✅ Drive successfully mounted at ${mountPath}`);
  });

  // Handle Ctrl+C gracefully to unmount
  process.on("SIGINT", function () {
    fuse.unmount((err) => {
      console.log("\nUnmounted.");
      process.exit();
    });
  });
}
