Last Edit: Claude Opus 5.5 - 2026-11-07 - Motive: List, search and sync client records through an index set, so a login without SCAN runs every path.
[previous header] Last Edit: Claude Opus 5.5 - 2026-11-03 - Motive: Admission cache write is compare-and-set against revocation.

# Audit

## 2026-11-07 — SCAN on the listing, sync and migration paths (fixed)

Finding: every path that enumerated client records walked the keyspace with
SCAN (on `dev` @ 43c0215, `hivemind_redis_database/__init__.py`):
`_iter_client_records_safely` l.95 (v1 -> v2 migration, which a namespace with
no `schema_version` runs at start-up), `_search_brute_force` l.1212, `sync` l.1391
and l.1415 (records, then every `name:`, `api_key:`, `api_key_record:`, `idx:`
key), `__iter__` l.1528, and `migration.py` l.32/l.64. SCAN is keyless: Redis
answers it with every key name in the database whatever the login's ACL key
patterns are, so a login confined to one namespace could list other
namespaces' key names, which here include `api_key:<key>`. A login without
SCAN broke listing, `sync()` (called by the WebSocket and HTTP transports on an
unknown key) and the start-up migration of a namespace with no
`schema_version`, which raised from `__post_init__`.

Fix: the `<prefix>:client_ids` set, written in the same MULTI/EXEC as each
record (`add_item` l.976, `remove_client` l.1081, `update_client` l.1179), read
by `_iter_client_keys` l.505; `ensure_client_index` l.537; `sync` l.1512 rebuilds
from it. Legacy cluster mode keeps the walk (`_sync_by_key_walk` l.1608).
Evidence: `tests/test_client_index.py` (13 tests on a fake that refuses SCAN
with NOPERM or counts every SCAN; 2 more on a real Redis 8.2.9 under a login
with no SCAN and no scripting, ACL LOG showing only the tolerated start-up
probes). `pytest tests`: 126 passed, 8 skipped; with the real Redis, the 2
ACL tests pass.

Behaviour changes, deliberate:
- `sync()` cannot find an index set keyed by a value no record carries; it is
  left in place. `_search_with_index` l.1301 now drops records that no longer
  carry the searched value (l.1329), so such a set answers nothing. Before
  this, a stale `api_key:<old>` entry made `get_client_by_api_key(<old>)`
  return the record that had moved to a new key.
- `sync()` no longer deletes a stale create marker that never reached the
  index; it expires after `CREATE_MARKER_TTL` (30 s).
- `sync()` returns False without writing while the index is not marked built.

Still open:
- `ensure_client_index` decides that a namespace never held a record from
  `count`, `id_seq`, `client_ids` and `client:1` (`_namespace_may_hold_records`
  l.519). A namespace with records but none of those would be marked built and
  listed empty. Every writer in this repository and the operator counts.
- `hivemind-core`'s `ClientDatabase.add_client` gives a new client the id
  `total_clients() + 1`, and `add_item` turns an existing id into an update.
  When `count` is below the highest id (records deleted outside this plugin),
  adding a client through the CLI overwrites another client's record. Not
  changed here; see SUGGESTIONS.md.

## 2026-11-03 — admission cache write vs. concurrent invalidation (fixed)

Finding: `get_client_by_api_key` refilled `api_key_record:<key>` with a plain
`SET ... EX` after its authoritative read. A revocation or update committing
in between deleted a record that did not exist yet, and the refill then wrote
the pre-change record, which was served until `admission_record_ttl` expired.
Fixed by a per-key epoch that every invalidation moves; the refill writes
only if the epoch it read before the lookup is unchanged, under a WATCH taken
after the lookup (`hivemind_redis_database/__init__.py`: `_api_key_epoch_key`,
`_invalidate_api_key_records`, `_read_epoch`, `_refill_admission_record`).
Evidence: `tests/test_admission_revocation_race.py`, which fails 8 of 9 on
0.1.1a8, and a real-Redis run of the same interleaving.
The operator's own invalidations need the same epoch move
(thalovant/hivemind-k8s-operator, `_drop_admission_records`).

Also fixed: `_search_with_index` wrote the searched value into its DEBUG
lines, and on the admission path that value is the client's API key
(CodeQL `py/clear-text-logging-sensitive-data`). It now logs the field only;
`tests/test_lookup_logging.py` asserts the key is absent.

Remaining, by design: the opt-in in-process cache (`api_key_cache_ttl`,
default off) is per process, so a revocation through another process takes
up to that TTL to reach it (documented in README).


## 2026-11-03 — superseded by the 0.1.1a8 sync

The fork now carries the published 0.1.1a8 package. The dependency findings
below described the 0.1.0a7 line (exact `filelock` pin, `hivemind-plugin-manager<0.9`,
the 4.x end-to-end stack) and no longer apply: the package declares the
release's own dependencies, and the runtime image that installs it pins
`filelock` and `bitstring` itself. `pytest tests`: 101 passed, 6 skipped.

## Earlier (0.1.0a7 line)

The security constraint is carried by the real installer, not solely by a
scanner input. [Dependency tests](tests/test_dependency_security.py) verify
exact filelock selection and lock behavior. The full declared test extra
passes 94 tests including topology tests, with 6 existing real-Redis skips.
No assertion is weakened and no new skip is added. Hosted real-Redis
integration remains required before merge.

Fresh installs require bitstring>=4.1.1,<5 because the supported bus client
imports BitStream. This legacy Client representation supports
plugin-manager>=0.6.0a1,<0.9. Test extras match the existing policy-E2E stack:
hivescope0.4.0a2, core4.3.0a2, agent0.2.0a1 and manager0.6.0a1.
No Redis implementation or schema is changed. This fork is not compatible with
modern quota's manager>=0.10 requirement and does not replace the maintained
listener's Redis0.1.1a8.

RedisDB in [the runtime source](hivemind_redis_database/__init__.py) is not modified.
This is not a complete audit of that implementation. Future advisory checks
and consumer artifact tests remain required; [evidence](docs/dependency-security.md).

