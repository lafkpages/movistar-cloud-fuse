import type Fuse from "@cocalc/fuse-native";

const noDate = new Date(0);

export const dirStat: Fuse.Stats = {
  mtime: noDate,
  atime: noDate,
  ctime: noDate,

  mode: 0o40755,
  nlink: 2,
  uid: process.getuid ? process.getuid() : 0,
  gid: process.getgid ? process.getgid() : 0,

  dev: 0,
  ino: 0,
  rdev: 0,

  size: 0,
  blksize: 4096,
  blocks: 0,
};

/**
 * Helper to generate a full POSIX stat object with sensible defaults
 */
export function createStat(
  isDir: boolean,
  opts: Partial<Fuse.Stats> & Pick<Fuse.Stats, "size">,
): Fuse.Stats {
  return {
    mtime: noDate,
    atime: noDate,
    ctime: noDate,

    mode: isDir ? 0o40755 : 0o100644,
    nlink: isDir ? 2 : 1,
    uid: process.getuid ? process.getuid() : 0,
    gid: process.getgid ? process.getgid() : 0,

    dev: 0,
    ino: 0,
    rdev: 0,

    blksize: 4096,
    blocks: opts.size ? Math.ceil(opts.size / 512) : 0,

    ...opts,
  };
}
