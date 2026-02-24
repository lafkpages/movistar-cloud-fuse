import { homedir } from "node:os";
import { basename } from "node:path";

process.chdir(homedir());

export const phoneNumber = parseInt(
  process.env.MOVISTAR_CLOUD_LOGIN_PHONE_NUMBER || "0",
  10,
);

if (isNaN(phoneNumber) || phoneNumber < 1) {
  throw new Error(
    "Invalid phone number. Please set MOVISTAR_CLOUD_LOGIN_PHONE_NUMBER environment variable to a valid phone number.",
  );
}

export const mountPath =
  process.env.MOVISTAR_CLOUD_FUSE_MOUNT_PATH || "Movistar Cloud";
export const volname = basename(mountPath);
