// Minimal typing for the Workers runtime's node:async_hooks module
// (enabled by the nodejs_als compatibility flag in wrangler.toml).
declare module "node:async_hooks" {
  export class AsyncLocalStorage<T> {
    getStore(): T | undefined;
    run<R>(store: T, callback: () => R): R;
  }
}
