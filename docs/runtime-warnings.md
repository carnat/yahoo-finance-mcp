# Runtime/CI Warning Notes

## GitHub Actions Node runtime warning

If GitHub shows a warning that Node.js 20-based actions are being forced onto Node.js 24, this warning is about action runtime metadata, not the application runtime itself.

- Project/runtime target remains Node 24.
- Workflows are pinned to latest major actions where available.

## `punycode` / `url.parse()` deprecation warnings

- Wrangler has been upgraded to latest available release in `worker/package.json`.
- Project code should use WHATWG `URL` APIs; no intentional `url.parse()` usage is expected in repository code.
- If warnings persist, they are treated as transitive dependency warnings and tracked upstream.

