Last Edit: Claude Opus 5.5 - 2026-11-07 - Motive: List, search and sync client records through an index set, so a login without SCAN runs every path.
[previous header] Last Edit: Codex - 2026-10-25 - Motive: Record verified dependency security and compatibility constraints.

# FAQ

## Does the plugin need SCAN?

No, outside legacy cluster mode. Listing (`RedisDB.__iter__`), `sync()`, the
schema migration and brute-force search read the `<prefix>:client_ids` set
(`RedisDB._iter_client_keys`). SCAN is used once, by `ensure_client_index`, to
index a namespace written by an older release, and only when the login may run
it. See [operations: logins without SCAN](docs/operations.md#logins-without-scan).

## The start-up log says the client index is not built. What now?

The namespace holds records from an older release and the login may not SCAN.
Lookups by name and api key still work; listing and `sync()` see only indexed
clients. Build the index once with a login that may SCAN (the recipe is in
[operations](docs/operations.md#logins-without-scan)) and restart.

## Why does `sync()` leave some old `name:` or `api_key:` sets behind?

It no longer walks the keyspace, so it cannot find a set keyed by a value no
record carries. Such a set is harmless: indexed searches return only records
that still carry the value (`RedisDB._search_with_index`).

## Who must write `client_ids`?

Anything that stores client records directly rather than through `RedisDB`
(an operator, an import script) adds the id beside the record and removes it
when it deletes the record. `update_client` and `remove_client` re-add it, which
mends an index that missed a record.

## Why is filelock pinned exactly?

3.20.3 contains the earlier fixes and predates the newer affected features.
A broad 3.x range can select an affected version. See the primary ranges and
verified limits in [dependency security](docs/dependency-security.md).

## Does this alter runtime behavior?

[RedisDB](hivemind_redis_database/__init__.py) is unchanged. The regression tests exercise classic
lock contention and real dependency metadata. Fresh installs require bitstring>=4.1.1,<5 because the supported bus client
imports BitStream. This legacy Client representation supports
plugin-manager>=0.6.0a1,<0.9. Test extras match the existing policy-E2E stack:
hivescope0.4.0a2, core4.3.0a2, agent0.2.0a1 and manager0.6.0a1.
No Redis implementation or schema is changed. This fork is not compatible with
modern quota's manager>=0.10 requirement and does not replace the maintained
listener's Redis0.1.1a8.

