export type MapValues<T extends Map<unknown, unknown>> =
  T extends Map<unknown, infer V> ? V : never;

export function isErrno(err: unknown): err is NodeJS.ErrnoException {
  return (
    typeof err === "object" &&
    err !== null &&
    "code" in err &&
    typeof err.code === "string"
  );
}
