import type Fuse from "@cocalc/fuse-native";
import type { MapValues } from "../types";

export interface OpsCache {
  readdir: Map<
    string,
    {
      timestamp: number;
      cb: [
        err: number,
        names: string[] | undefined,
        stats: Fuse.Stats[] | undefined,
      ];
    }
  >;

  getattr: Map<
    string,
    {
      timestamp: number;
      cb: [err: number, stat: Fuse.Stats | undefined];
    }
  >;
}

export const opsCacheLifetime = 30_000;

type CbArgs<T extends keyof OpsCache> = MapValues<OpsCache[T]>["cb"];

export function wrapCbWithCache<TOpName extends keyof OpsCache>(
  opsCache: OpsCache,
  opName: TOpName,
  path: string,
  cb: (...args: CbArgs<TOpName>) => void,
) {
  return (...args: CbArgs<TOpName>) => {
    opsCache[opName].set(path, {
      timestamp: Date.now(),
      cb: args,
    });
    return cb(...args);
  };
}
