import Fuse from "@cocalc/fuse-native";
import type { MovistarCloudClient } from "movistar-cloud";
import type { MediaField } from "../../movistar-cloud/src/schemas/media";

export const enum ExpectedItemType {
  ExpectFile,
  ExpectDirectory,
  ExpectEither,
}

export async function traversePath(
  mv: MovistarCloudClient,
  rootFolderId: number,
  path: string,
  expects: ExpectedItemType,
  fileFields: MediaField[] = ["name", "size"],
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

  switch (expects) {
    case ExpectedItemType.ExpectDirectory: {
      const folder = await mv.findFolder(
        currentFolderId,
        parts[currentPartIndex]!,
      );

      if (folder) {
        return { folder, file: null, err: 0 };
      }

      const file = await mv.findFile(
        currentFolderId,
        parts[currentPartIndex]!,
        fileFields,
      );

      if (file) {
        return { folder: null, file, err: Fuse.ENOTDIR };
      }

      break;
    }

    case ExpectedItemType.ExpectFile: {
      const file = await mv.findFile(
        currentFolderId,
        parts[currentPartIndex]!,
        fileFields,
      );

      if (file) {
        return { folder: null, file, err: 0 };
      }

      const folder = await mv.findFolder(
        currentFolderId,
        parts[currentPartIndex]!,
      );

      if (folder) {
        return { folder, file: null, err: Fuse.EISDIR };
      }

      break;
    }

    case ExpectedItemType.ExpectEither: {
      const folder = await mv.findFolder(
        currentFolderId,
        parts[currentPartIndex]!,
      );

      if (folder) {
        return { folder, file: null, err: 0 };
      }

      const file = await mv.findFile(
        currentFolderId,
        parts[currentPartIndex]!,
        fileFields,
      );

      if (file) {
        return { folder: null, file, err: 0 };
      }
    }
  }

  return { folder: null, file: null, err: Fuse.ENOENT };
}
