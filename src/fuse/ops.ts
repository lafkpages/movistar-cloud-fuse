import type { MovistarCloudClient } from "movistar-cloud";
import type { OpsCache } from "./cache";
import type { LocalFiles, OpenFile, OpenFiles } from "./open-files";

import { constants, open, readFile } from "node:fs/promises";
import { join, parse as parsePath } from "node:path";

import Fuse from "@cocalc/fuse-native";
import mime from "mime";

import { cacheDir } from "../env";
import { ExpectedItemType, traversePath } from "../traverse";
import { opsCacheLifetime, wrapCbWithCache } from "./cache";
import { notifyWaiters } from "./open-files";
import { createStat, dirStat } from "./stat";

async function doUpload(
  mv: MovistarCloudClient,
  path: string,
  openFile: OpenFile,
  localFiles: LocalFiles,
  opsCache: OpsCache,
) {
  // Sync writes to disk before reading
  await openFile.cacheFileHandle.datasync();

  const fileData = await readFile(openFile.cacheFilePath!);

  console.log(
    "doUpload: uploading %s (%d bytes) to folder %d",
    openFile.fileName,
    fileData.length,
    openFile.parentFolderId,
  );

  const mimeType =
    mime.getType(openFile.fileName!) ?? "application/octet-stream";
  const blob = new Blob([fileData], {
    type: mimeType,
  });
  const result = await mv.uploadFile(
    blob,
    openFile.fileName!,
    openFile.parentFolderId!,
    new Date(),
    false, // don't wait for validation — avoids FUSE timeout
  );

  console.log("doUpload: upload complete, id=%s", result.id);
  openFile.uploaded = true;

  // Remove from localFiles so subsequent accesses go through the remote
  localFiles.delete(path);

  // Invalidate caches so next access fetches from remote
  const parsedPath = parsePath(path);
  opsCache.readdir.delete(parsedPath.dir);
  opsCache.getattr.delete(path);
}

export function getOps(args: {
  mv: MovistarCloudClient;
  rootFolderId: number;
  opsCache: OpsCache;
  localFiles: LocalFiles;
  openFiles: OpenFiles;
  fdCounter: number;
}): Fuse.OPERATIONS {
  const { mv, rootFolderId, opsCache, localFiles, openFiles } = args;

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

      // Check if this is a locally-created file not yet on the remote
      if (localFiles.has(path)) {
        // Find the open writable file to get current size
        let size = 0;
        for (const [, of] of openFiles) {
          if (of.writable && of.cacheFilePath === localFiles.get(path)) {
            size = of.bytesWritten ?? 0;
            break;
          }
        }
        return cb(0, createStat({ size }));
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

      // Check if we have a locally-created file that may not yet be on the remote
      const localCachePath = localFiles.get(path);
      if (localCachePath) {
        const fd = args.fdCounter++;
        const cacheFileHandle = await open(localCachePath, constants.O_RDONLY);

        // Get size from getattr cache
        const cachedAttr = opsCache.getattr.get(path);
        const size =
          cachedAttr && !cachedAttr.cb[0] && cachedAttr.cb[1]
            ? cachedAttr.cb[1].size
            : 0;

        const openFile: OpenFile = {
          remoteFileId: 0,
          cacheFileHandle,
          bytesDownloaded: size,
          done: true,
          closed: false,
          waiters: [],
        };
        openFiles.set(fd, openFile);
        return cb(0, fd);
      }

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

    async flush(path, fd, cb) {
      console.log("flush(%s, %d)", path, fd);

      const openFile = openFiles.get(fd);

      if (!openFile?.writable || openFile.uploaded) {
        return cb(0);
      }

      try {
        await doUpload(mv, path, openFile, localFiles, opsCache);
      } catch (e) {
        console.error("flush: upload failed", e);
        return cb(Fuse.EIO);
      }

      return cb(0);
    },

    async release(path, fd, cb) {
      console.log("release(%s, %d)", path, fd);

      const openFile = openFiles.get(fd);

      if (openFile) {
        openFiles.delete(fd);
        openFile.closed = true;

        if (openFile.writable) {
          // Upload if flush didn't already do it
          if (!openFile.uploaded) {
            try {
              await doUpload(mv, path, openFile, localFiles, opsCache);
            } catch (e) {
              console.error("release: upload failed", e);
            }
          }

          await openFile.cacheFileHandle.close();
          return cb(0);
        }

        // Read-only file: allow download to finish in background if not done yet
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

      // If this is a local-only file (not yet on remote), just remove locally
      if (localFiles.has(path)) {
        localFiles.delete(path);

        const parsedPath = parsePath(path);
        const cache = opsCache.readdir.get(parsedPath.dir);
        if (cache && !cache.cb[0]) {
          const idx = cache.cb[1]!.indexOf(parsedPath.base);
          if (idx !== -1) {
            cache.cb[1]!.splice(idx, 1);
            cache.cb[2]!.splice(idx, 1);
          }
        }

        opsCache.getattr.delete(path);
        return cb(0);
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

    async create(path, mode, cb) {
      console.log("create(%s, %o)", path, mode);

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

      const fd = args.fdCounter++;

      const cacheFilePath = join(cacheDir, `create-${fd}`);
      const cacheFileHandle = await open(
        cacheFilePath,
        constants.O_CREAT | constants.O_RDWR | constants.O_TRUNC,
      );

      const openFile: OpenFile = {
        remoteFileId: 0,
        cacheFileHandle,
        bytesDownloaded: 0,
        done: true,
        closed: false,
        waiters: [],
        writable: true,
        parentFolderId,
        fileName: parsedPath.base,
        bytesWritten: 0,
        uploaded: false,
        cacheFilePath,
      };
      openFiles.set(fd, openFile);

      // Track locally so subsequent open() can serve from cache
      localFiles.set(path, cacheFilePath);

      // Update readdir cache of parent folder
      const cache = opsCache.readdir.get(parsedPath.dir);
      if (cache && !cache.cb[0]) {
        cache.cb[1]!.push(parsedPath.base);
        cache.cb[2]!.push(createStat({ size: 0 }));
      }

      // Cache the new file's attributes
      opsCache.getattr.set(path, {
        timestamp: Date.now(),
        cb: [0, createStat({ size: 0 })],
      });

      return cb(0, fd);
    },

    async write(path, fd, buf, len, pos, cb) {
      console.log("write(%s, %d, %d, %d)", path, fd, len, pos);

      const openFile = openFiles.get(fd);

      if (!openFile || !openFile.writable) {
        return cb(Fuse.EBADF);
      }

      const { bytesWritten } = await openFile.cacheFileHandle.write(
        buf,
        0,
        len,
        pos,
      );

      const newEnd = pos + bytesWritten;
      if (newEnd > openFile.bytesWritten!) {
        openFile.bytesWritten = newEnd;
      }

      // Update getattr cache with new size
      opsCache.getattr.set(path, {
        timestamp: Date.now(),
        cb: [0, createStat({ size: openFile.bytesWritten! })],
      });

      console.log(
        "write: fd=%d bytesWritten=%d totalSize=%d",
        fd,
        bytesWritten,
        openFile.bytesWritten,
      );

      return cb(bytesWritten);
    },

    async truncate(path, size, cb) {
      console.log("truncate(%s, %d)", path, size);

      // Check if there's an open writable fd for this path
      for (const [, openFile] of openFiles) {
        if (openFile.writable && !openFile.closed) {
          await openFile.cacheFileHandle.truncate(size);
          openFile.bytesWritten = size;

          opsCache.getattr.set(path, {
            timestamp: Date.now(),
            cb: [0, createStat({ size })],
          });

          return cb(0);
        }
      }

      return cb(Fuse.ENOENT);
    },

    async ftruncate(path, fd, size, cb) {
      console.log("ftruncate(%s, %d, %d)", path, fd, size);

      const openFile = openFiles.get(fd);

      if (!openFile || !openFile.writable) {
        return cb(Fuse.EBADF);
      }

      await openFile.cacheFileHandle.truncate(size);
      openFile.bytesWritten = size;

      opsCache.getattr.set(path, {
        timestamp: Date.now(),
        cb: [0, createStat({ size })],
      });

      return cb(0);
    },

    async rename(src, dest, cb) {
      console.log("rename(%s, %s)", src, dest);

      if (!src.startsWith("/") || !dest.startsWith("/")) {
        return cb(Fuse.ENOENT);
      }

      if (src === "/" || dest === "/") {
        return cb(Fuse.EBUSY);
      }

      const parsedSrc = parsePath(src);
      const parsedDest = parsePath(dest);

      if (parsedSrc.dir !== parsedDest.dir) {
        // TODO: for now, only allow renaming within the same directory
        return cb(Fuse.EXDEV);
      }

      const {
        file: srcFile,
        folder: srcFolder,
        err,
      } = await traversePath(
        mv,
        rootFolderId,
        src,
        ExpectedItemType.ExpectEither,
        ["name"],
      );

      if (err) {
        return cb(err);
      }

      if (srcFolder) {
        // TODO: mv does not support renaming folders yet
        return cb(Fuse.EIO);
      }

      const { file: destFile, err: destErr } = await traversePath(
        mv,
        rootFolderId,
        dest,
        ExpectedItemType.ExpectEither,
        ["name"],
      );

      if (destErr && destErr !== Fuse.ENOENT) {
        return cb(destErr);
      }

      if (destFile) {
        return cb(Fuse.EEXIST);
      }

      await mv.renameFile(srcFile!.id, parsedDest.base);

      // Update local file cache
      const localPath = localFiles.get(src);
      if (localPath) {
        localFiles.delete(src);
        localFiles.set(dest, localPath);
      }

      // Update cache of folder
      const cache = opsCache.readdir.get(parsedSrc.dir);
      if (cache && !cache.cb[0]) {
        const idx = cache.cb[1]!.indexOf(parsedSrc.base);
        if (idx !== -1) {
          cache.cb[1]![idx] = parsedDest.base;
        }
      }

      // Update cache of renamed file
      const now = Date.now();
      opsCache.readdir.set(src, {
        timestamp: now,
        cb: [Fuse.ENOENT, undefined, undefined],
      });
      opsCache.readdir.set(dest, {
        timestamp: now,
        cb: [0, undefined, undefined],
      });
      opsCache.getattr.set(src, {
        timestamp: now,
        cb: [Fuse.ENOENT, undefined],
      });
      opsCache.getattr.set(dest, {
        timestamp: now,
        cb: [0, createStat({ size: srcFile!.size! })],
      });

      return cb(0);
    },

    async utimens(path, atime, mtime, cb) {
      console.log("utimens(%s)", path);
      // No-op: we don't track timestamps on the remote
      return cb(0);
    },
  };
}
