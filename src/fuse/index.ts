import Fuse from "@cocalc/fuse-native";
import type { MovistarCloudClient } from "movistar-cloud";
import type { FileHandle } from "node:fs/promises";
import { constants, mkdir, open } from "node:fs/promises";
import { join } from "node:path";
import { Writable } from "node:stream";
import { cacheDir, mountPath, volname } from "../env";
import { ExpectedItemType, traversePath } from "../traverse";
import { isErrno } from "../types";
import { opsCacheLifetime, wrapCbWithCache, type OpsCache } from "./cache";
import { createStat, dirStat } from "./stat";

let fdCounter = 100;
const openFiles = new Map<number, OpenFile>();

interface OpenFile {
  remoteFileId: number;
  fileHandle: Promise<FileHandle>;
}

const opsCache: OpsCache = {
  readdir: new Map(),
  getattr: new Map(),
};

export async function main(mv: MovistarCloudClient) {
  await mkdir(cacheDir, { recursive: true });

  await mv.listRoots();

  if (!mv.rootFolderId) {
    throw new Error(
      `Expected a root folder ID in the MovistarCloudClient instance`,
    );
  }

  const { rootFolderId } = mv;

  const ops: Fuse.OPERATIONS = {
    async readdir(path, cb) {
      const now = Date.now();
      const cached = opsCache.readdir.get(path);
      if (cached) {
        if (now - cached.timestamp < opsCacheLifetime) {
          console.log("Cache hit for readdir(%s)", path);
          return cb(...cached.cb);
        } else {
          opsCache.readdir.delete(path);
        }
      }
      cb = wrapCbWithCache(opsCache, "readdir", path, cb);

      console.log("readdir(%s)", path);

      let folderId: number;

      if (path === "/") {
        folderId = rootFolderId;
      } else if (path.startsWith("/")) {
        const { folder, err } = await traversePath(
          mv,
          rootFolderId,
          path,
          ExpectedItemType.ExpectDirectory,
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
      const now = Date.now();
      const cached = opsCache.getattr.get(path);
      if (cached) {
        if (now - cached.timestamp < opsCacheLifetime) {
          console.log("Cache hit for getattr(%s)", path);
          return cb(...cached.cb);
        } else {
          opsCache.getattr.delete(path);
        }
      }
      cb = wrapCbWithCache(opsCache, "getattr", path, cb);

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
        path.endsWith("/")
          ? ExpectedItemType.ExpectDirectory
          : ExpectedItemType.ExpectEither,
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
    async open(path, flags, cb) {
      console.log("open(%s, %d)", path, flags);

      const { file, err } = await traversePath(
        mv,
        rootFolderId,
        path,
        ExpectedItemType.ExpectFile,
        ["name", "size", "url"],
      );

      if (err) {
        return cb(err);
      }

      const fd = fdCounter++;

      const cacheFilePath = join(cacheDir, `${file!.id}`);
      const cacheFileHandle = await open(
        cacheFilePath,
        constants.O_CREAT | constants.O_RDWR,
      );

      const { resolve, reject, promise } = Promise.withResolvers<FileHandle>();

      const openFile: OpenFile = {
        remoteFileId: file!.id,
        fileHandle: promise,
      };
      openFiles.set(fd, openFile);

      mv.downloadFile(file!.url!)
        .then(async (resp) => {
          if (!resp.body) {
            throw new Error(`Expected a response body when downloading file`);
          }

          await resp.body!.pipeTo(
            Writable.toWeb(cacheFileHandle.createWriteStream()),
          );

          resolve(cacheFileHandle);
        })
        .catch(reject);

      return cb(0, fd);
    },
    async read(path, fd, buf, len, pos, cb) {
      console.log("read(%s, %d, %d, %d)", path, fd, len, pos);

      const openFile = openFiles.get(fd);

      if (!openFile) {
        return cb(Fuse.EBADF);
      }

      const fh = await openFile.fileHandle;

      let bytesRead: number;
      try {
        ({ bytesRead } = await fh.read(buf, 0, len, pos));
      } catch (err) {
        if (isErrno(err)) {
          switch (err.code) {
            case "EBADF":
              return cb(Fuse.EBADF);
          }
        }

        throw err;
      }

      console.log("read: fd=%d bytesRead=%d", fd, bytesRead);

      return cb(bytesRead);
    },
    async release(path, fd, cb) {
      console.log("release(%s, %d)", path, fd);

      const openFile = openFiles.get(fd);

      if (openFile) {
        await (await openFile.fileHandle).close();
        openFiles.delete(fd);
        return cb(0);
      }

      return cb(Fuse.EBADF);
    },
  };

  const fuse = new Fuse(mountPath, ops, {
    volname,
    mkdir: true,

    timeout: 120_000,

    // debug: true,
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
