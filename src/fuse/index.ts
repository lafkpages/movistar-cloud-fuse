import Fuse from "@cocalc/fuse-native";
import type { MovistarCloudClient } from "movistar-cloud";
import { mountPath, volname } from "../env";
import { createStat, dirStat } from "./stat";

async function traversePath(
  mv: MovistarCloudClient,
  rootFolderId: number,
  path: string,
  expectsDir: boolean,
) {
  const parts = path.replace(/^\/|\/$/g, "").split("/");
  let currentPartIndex = 0;
  let currentFolderId = rootFolderId;

  if (parts.length > 1) {
    for (; currentPartIndex < parts.length - 1; currentPartIndex++) {
      const folder = await mv.findFolder(
        currentFolderId,
        parts[currentPartIndex]!,
      );

      if (!folder) {
        return { folder: null, file: null, err: Fuse.ENOENT };
      }

      currentFolderId = folder.id;
    }
  }

  const folder = await mv.findFolder(currentFolderId, parts[currentPartIndex]!);

  if (folder) {
    return { folder, file: null, err: 0 };
  }

  if (expectsDir) {
    return { folder: null, file: null, err: Fuse.ENOTDIR };
  }

  const file = await mv.findFile(currentFolderId, parts[currentPartIndex]!, [
    "name",
    "size",
  ]);

  if (file) {
    return { folder: null, file, err: 0 };
  }

  return { folder: null, file: null, err: Fuse.ENOENT };
}

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

      let folderId: number;

      if (path === "/") {
        folderId = rootFolderId;
      } else if (path.startsWith("/")) {
        const { folder, err } = await traversePath(
          mv,
          rootFolderId,
          path,
          true,
        );

        if (err) {
          return cb(err);
        }

        folderId = folder!.id;
      } else {
        return cb(Fuse.ENOENT);
      }

      const folders = await mv.listFolders(folderId);
      const files = await mv.listFiles(folderId, ["name", "size"]);

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

      return cb(0, names, stats);
    },
    async getattr(path, cb) {
      console.log("getattr(%s)", path);

      if (path === "/") {
        return cb(0, dirStat);
      }

      if (!path.startsWith("/")) {
        return cb(Fuse.ENOENT);
      }

      const { folder, file, err } = await traversePath(
        mv,
        rootFolderId,
        path,
        path.endsWith("/"),
      );

      if (err) {
        return cb(err);
      } else if (folder) {
        return cb(0, dirStat);
      } else if (file) {
        return cb(0, createStat({ size: file.size! }));
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
    volname,
    mkdir: true,

    timeout: 120_000,

    debug: true,
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
