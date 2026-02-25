import type { FileHandle } from "node:fs/promises";

export interface OpenFile {
  remoteFileId: number;
  cacheFileHandle: FileHandle;
  bytesDownloaded: number;
  done: boolean;
  closed: boolean;
  waiters: (() => void)[];

  /** Set when the file was opened for writing (create). */
  writable?: boolean;
  /** Parent folder ID for uploading on release. */
  parentFolderId?: number;
  /** File name for uploading on release. */
  fileName?: string;
  /** Total bytes written (tracks the file size). */
  bytesWritten?: number;
  /** Whether the file has been uploaded to remote. */
  uploaded?: boolean;
  /** Local cache file path (for writable files). */
  cacheFilePath?: string;
}

export type LocalFiles = Map<string, string>; // path → cache file path
export type OpenFiles = Map<number, OpenFile>;

export function notifyWaiters(openFile: OpenFile) {
  const waiters = openFile.waiters;
  openFile.waiters = [];
  for (const w of waiters) {
    w();
  }
}
