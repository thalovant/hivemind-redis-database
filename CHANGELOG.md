Last Edit: Claude Opus 5.5 - 2026-11-07 - Motive: List, search and sync client records through an index set, so a login without SCAN runs every path.

# Changelog

## 0.1.1a8+thalovant.2 — client records listed without SCAN (2026-11-07)

- New set `<prefix>:client_ids` holds the id of every client record. `add_item`,
  `update_client` and `remove_client` add to it in the same MULTI/EXEC as the
  record (update and revoke re-add, which mends an index that missed one).
- `__iter__`, `sync()`, the v1 -> v2 schema migration and `_search_brute_force`
  read that set instead of `SCAN MATCH <prefix>:client:*`, in id order.
- `ensure_client_index()` runs at start-up: a namespace marked
  `<prefix>:client_ids_built`, or one that never held a record, needs no walk;
  an older namespace is walked once with SCAN when the login may, then marked.
  Refused (NOPERM), it warns once and leaves the namespace for an
  administrator to index.
- `sync()` no longer walks the keyspace for index keys. It drops and rebuilds
  every index key a record names, invalidates each live key's admission
  record (moving its epoch), and removes ids whose record is gone. It returns
  False without writing while the index is not marked built.
- Indexed `name`/`api_key` searches drop records that no longer carry the
  value, so an orphaned index entry cannot answer for another record.
- `hivemind-redis-migrate-cluster` adds copied records to the target's index,
  and refuses a source whose index is not built.
- The v1 -> v2 schema migration waits for a complete index: at start-up it
  leaves an unindexed namespace at v1 (reads still fold the legacy fields)
  instead of stamping v2 over a partial walk; a direct `migrate()` raises.
- Legacy cluster mode (no `cluster_hash_tag`) is unchanged and still scans.

## 0.1.1a8+thalovant.1 — admission cache write is compare-and-set against revocation (2026-11-03)

- A lookup that misses the admission record reads the authoritative record and
  then writes `api_key_record:<key>`. An update or revocation committing
  between those two steps was undone by that write until the record expired.
  Every invalidation now also moves `api_key_epoch:<key>` (INCR + EXPIRE) in
  the same transaction. The lookup reads that epoch before its authoritative
  read, and writes the record only if it is unchanged, under a WATCH taken
  after the read and held for one GET and one MULTI/EXEC, so such a write
  aborts the refill instead and no pooled connection is held across the
  read. A lookup slower than half an epoch's lifetime does not write. When
  the guard cannot be had, one warning says so and the lookup answers from
  the authoritative record without writing. Legacy cluster mode never fills this cache and
  is unchanged; under a hash tag the epoch shares the record's slot. An
  operator that drops admission records must move the epoch too.
- The lookup logger falls back to the standard library logger when the
  logging facade lacks the attributes it reads, so the admission path cannot
  fail on a log line.
- An indexed lookup's debug lines name the field, never the value: on the
  admission path that value is the client's API key.
- The build carries the PEP 440 local label `+thalovant.1`.

## 0.1.1a8 — this fork brought up to the published release (2026-11-03)

The fork's `dev` was on the 0.1.0a7 line while every hub runs the published
0.1.1a8. The package (`hivemind_redis_database/__init__.py`, `migration.py`,
`version.py`), its tests, `pyproject.toml` and `README.md` are now that
release's files, byte for byte, except that `bitstring` is capped below 5
in the runtime dependencies (the bus client still imports `BitStream`) and
the README notes the cross-process delay of the opt-in in-process cache. The 0.1.0a7-line constraints are retired with the code
they described: the exact `filelock==3.20.3` and `hivemind-plugin-manager<0.9`
pins and their tests, and the hivemind-core 4.x end-to-end suite and its
workflow, which called a `crypto_key` client API that 5.x no longer has. The
runtime image that installs this package carries its own `filelock` and
`bitstring` constraints.

## [0.1.0a6](https://github.com/JarbasHiveMind/hivemind-redis-database/tree/0.1.0a6) (2026-07-16)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-redis-database/compare/0.1.0a5...0.1.0a6)

**Merged pull requests:**

- Revert unauthorized automated merges \(\#31, \#32, \#33\) [\#35](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/35) ([JarbasAl](https://github.com/JarbasAl))

## [0.1.0a5](https://github.com/JarbasHiveMind/hivemind-redis-database/tree/0.1.0a5) (2026-07-16)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-redis-database/compare/0.1.0a4...0.1.0a5)

**Merged pull requests:**

- ci: release safely after fork merges [\#33](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/33) ([goldyfruit](https://github.com/goldyfruit))
- fix: advance last\_seen atomically [\#32](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/32) ([goldyfruit](https://github.com/goldyfruit))
- Use indexed API-key lookup and larger Redis pool [\#31](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/31) ([goldyfruit](https://github.com/goldyfruit))

## [0.1.0a4](https://github.com/JarbasHiveMind/hivemind-redis-database/tree/0.1.0a4) (2026-07-04)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-redis-database/compare/0.1.0a3...0.1.0a4)

**Merged pull requests:**

- Update actions/checkout action to v7 [\#27](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/27) ([renovate[bot]](https://github.com/apps/renovate))

## [0.1.0a3](https://github.com/JarbasHiveMind/hivemind-redis-database/tree/0.1.0a3) (2026-07-04)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-redis-database/compare/0.1.0a2...0.1.0a3)

**Merged pull requests:**

- test: hivescope e2e + CI [\#28](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/28) ([JarbasAl](https://github.com/JarbasAl))

## [0.1.0a2](https://github.com/JarbasHiveMind/hivemind-redis-database/tree/0.1.0a2) (2026-06-05)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-redis-database/compare/0.1.0a1...0.1.0a2)

**Merged pull requests:**

- docs: zero-to-hero README and /docs coverage [\#25](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/25) ([JarbasAl](https://github.com/JarbasAl))

## [0.1.0a1](https://github.com/JarbasHiveMind/hivemind-redis-database/tree/0.1.0a1) (2026-06-05)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-redis-database/compare/0.0.4a2...0.1.0a1)

**Merged pull requests:**

- feat\(db\): schema v2 migration — fold legacy blacklist keys into metadata [\#20](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/20) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.4a2](https://github.com/JarbasHiveMind/hivemind-redis-database/tree/0.0.4a2) (2026-05-20)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-redis-database/compare/0.0.4a1...0.0.4a2)

**Merged pull requests:**

- ci: pass PYPI\_TOKEN explicitly, drop secrets:inherit elsewhere [\#21](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/21) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.4a1](https://github.com/JarbasHiveMind/hivemind-redis-database/tree/0.0.4a1) (2026-05-18)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-redis-database/compare/0.0.3...0.0.4a1)

**Merged pull requests:**

- Preserve client metadata \(supersedes \#17\) [\#18](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/18) ([JarbasAl](https://github.com/JarbasAl))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
