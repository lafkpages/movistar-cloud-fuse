import type { MovistarCloudClient } from "movistar-cloud";
import type { OpsCache } from "./cache";
import type { OpenFile, OpenFiles } from "./open-files";

import { constants, open } from "node:fs/promises";
import { join, parse as parsePath } from "node:path";

import Fuse from "@cocalc/fuse-native";

import { cacheDir } from "../env";
import { ExpectedItemType, traversePath } from "../traverse";
import { opsCacheLifetime, wrapCbWithCache } from "./cache";
import { notifyWaiters } from "./open-files";
import { createStat, dirStat } from "./stat";

export function getOps(args: {
  mv: MovistarCloudClient;
  rootFolderId: number;
  opsCache: OpsCache;
  openFiles: OpenFiles;
  fdCounter: number;
}): Fuse.OPERATIONS {
  const { mv, rootFolderId, opsCache, openFiles } = args;

  return {
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

      const fd = args.fdCounter++;

      const cacheFilePath = join(cacheDir, `${file!.id}`);
      const cacheFileHandle = await open(
        cacheFilePath,
        constants.O_CREAT | constants.O_RDWR,
      );

      const openFile: OpenFile = {
        remoteFileId: file!.id,
        cacheFileHandle,
        bytesDownloaded: 0,
        done: false,
        closed: false,
        waiters: [],
      };
      openFiles.set(fd, openFile);

      // Download in background, streaming chunks to cache file
      mv.downloadFile(file!.url!).then(async (resp) => {
        if (!resp.body) {
          throw new Error(`Expected a response body when downloading file`);
        }

        const reader = resp.body.getReader();

        while (true) {
          const { done, value } = await reader.read();

          if (done) {
            break;
          }

          await cacheFileHandle.write(
            value,
            0,
            value.length,
            openFile.bytesDownloaded,
          );

          openFile.bytesDownloaded += value.length;
          notifyWaiters(openFile);
        }

        openFile.done = true;
        notifyWaiters(openFile);

        if (openFile.closed) {
          await cacheFileHandle.close();
        }
      });

      return cb(0, fd);
    },
    async read(path, fd, buf, len, pos, cb) {
      console.log("read(%s, %d, %d, %d)", path, fd, len, pos);

      const openFile = openFiles.get(fd);

      if (!openFile) {
        return cb(Fuse.EBADF);
      }

      // Wait until we have enough data or the download finishes
      while (
        !openFile.done &&
        !openFile.closed &&
        openFile.bytesDownloaded < pos + len
      ) {
        await new Promise<void>((resolve) => openFile.waiters.push(resolve));
      }

      if (openFile.closed && openFile.bytesDownloaded <= pos) {
        return cb(Fuse.EIO);
      }

      // Read whatever is available
      const available = Math.max(
        0,
        Math.min(len, openFile.bytesDownloaded - pos),
      );

      if (available <= 0) {
        return cb(0);
      }

      const { bytesRead } = await openFile.cacheFileHandle.read(
        buf,
        0,
        available,
        pos,
      );

      console.log("read: fd=%d bytesRead=%d", fd, bytesRead);

      return cb(bytesRead);
    },
    async release(path, fd, cb) {
      console.log("release(%s, %d)", path, fd);

      const openFile = openFiles.get(fd);

      if (openFile) {
        openFiles.delete(fd);
        openFile.closed = true;

        // Allow download to finish in background if not done yet
        if (openFile.done) {
          await openFile.cacheFileHandle.close();
        }

        return cb(0);
      }

      return cb(Fuse.EBADF);
    },
    async mkdir(path, mode, cb) {
      console.log("mkdir(%s, %o)", path, mode);

      if (!path.startsWith("/")) {
        return cb(Fuse.ENOENT);
      }

      const parsedPath = parsePath(path);

      let parentFolderId: number;

      if (parsedPath.dir === "/") {
        parentFolderId = rootFolderId;
      } else {
        const { folder, err } = await traversePath(
          mv,
          rootFolderId,
          parsedPath.dir,
          ExpectedItemType.ExpectDirectory,
          ["name"],
        );

        if (err) {
          return cb(err);
        }

        parentFolderId = folder!.id;
      }

      await mv.createFolder(parsedPath.base, parentFolderId);

      // Update cache of parent folder
      const cache = opsCache.readdir.get(parsedPath.dir);
      if (cache && !cache.cb[0]) {
        cache.cb[1]!.push(parsedPath.base);
        cache.cb[2]!.push(dirStat);
      }

      // Cache the new folder's contents (empty)
      opsCache.readdir.set(path, {
        timestamp: Date.now(),
        cb: [0, [], []],
      });

      // Cache the new folder's attributes
      opsCache.getattr.set(path, {
        timestamp: Date.now(),
        cb: [0, dirStat],
      });

      return cb(0);
    },
    async rmdir(path, cb) {
      console.log("rmdir(%s)", path);

      if (!path.startsWith("/")) {
        return cb(Fuse.ENOENT);
      }

      if (path === "/") {
        return cb(Fuse.EBUSY);
        // TODO: is this the right error code?
      }

      const { folder, err } = await traversePath(
        mv,
        rootFolderId,
        path,
        ExpectedItemType.ExpectDirectory,
        ["name"],
      );

      if (err) {
        return cb(err);
      }

      await mv.removeFolders([folder!.id]);

      // Update cache of parent folder
      const parsedPath = parsePath(path);
      const cache = opsCache.readdir.get(parsedPath.dir);
      if (cache && !cache.cb[0]) {
        const idx = cache.cb[1]!.indexOf(parsedPath.base);
        if (idx !== -1) {
          cache.cb[1]!.splice(idx, 1);
          cache.cb[2]!.splice(idx, 1);
        }
      }

      // Update cache of removed folder
      const now = Date.now();
      opsCache.readdir.set(path, {
        timestamp: now,
        cb: [Fuse.ENOENT, undefined, undefined],
      });
      opsCache.getattr.set(path, {
        timestamp: now,
        cb: [Fuse.ENOENT, undefined],
      });

      return cb(0);
    },
    async unlink(path, cb) {
      console.log("unlink(%s)", path);

      if (!path.startsWith("/")) {
        return cb(Fuse.ENOENT);
      }

      const { file, err } = await traversePath(
        mv,
        rootFolderId,
        path,
        ExpectedItemType.ExpectFile,
        ["name"],
      );

      if (err) {
        return cb(err);
      }

      await mv.removeFiles([file!.id]);

      // Update cache of parent folder
      const parsedPath = parsePath(path);
      const cache = opsCache.readdir.get(parsedPath.dir);
      if (cache && !cache.cb[0]) {
        const idx = cache.cb[1]!.indexOf(parsedPath.base);
        if (idx !== -1) {
          cache.cb[1]!.splice(idx, 1);
          cache.cb[2]!.splice(idx, 1);
        }
      }

      // Update cache of removed file
      const now = Date.now();
      opsCache.readdir.set(path, {
        timestamp: now,
        cb: [Fuse.ENOENT, undefined, undefined],
      });
      opsCache.getattr.set(path, {
        timestamp: now,
        cb: [Fuse.ENOENT, undefined],
      });

      return cb(0);
    },
  };
}
