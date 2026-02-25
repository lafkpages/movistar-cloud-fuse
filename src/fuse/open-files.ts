import type { FileHandle } from "node:fs/promises";

export interface OpenFile {
  remoteFileId: number;
  cacheFileHandle: FileHandle;
  bytesDownloaded: number;
  done: boolean;
  closed: boolean;
  waiters: (() => void)[];
}

export type OpenFiles = Map<number, OpenFile>;

export function notifyWaiters(openFile: OpenFile) {
  const waiters = openFile.waiters;
  openFile.waiters = [];
  for (const w of waiters) {
    w();
  }
}
