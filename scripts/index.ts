import { $ } from "bun";

// See https://u.luisafk.dev/kFAde
export const o$ = new $.Shell().throws(true).env({
  FORCE_COLOR: Bun.enableANSIColors ? "1" : undefined,
});
