## Summary

- 

## Verification

- [ ] Smallest relevant local check was run, or the reason it was skipped is stated.
- [ ] Deploy/live smoke expectations were reviewed.

## Tool And Smoke Checklist

Check any that apply:

- [ ] Public tool name, schema, response envelope, alias, or diagnostics changed.
- [ ] Provider, fallback, entitlement, rate-limit, or quarantine behavior changed.
- [ ] Relevant smoke tests were updated in the same PR.
- [ ] Live smoke inputs avoid hardcoded volatile dates/values, or use recorded fixtures.
- [ ] Expected provider failures return explicit JSON status/error codes, not opaque text or fake success.
- [ ] External/provider changes cite current official docs or state why no provider docs apply.
