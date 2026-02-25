export type MapValues<T extends Map<unknown, unknown>> =
  T extends Map<unknown, infer V> ? V : never;
