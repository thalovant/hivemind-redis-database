Last Edit: Claude Opus 5.5 - 2026-11-07 - Motive: List, search and sync client records through an index set, so a login without SCAN runs every path.
[previous header] Last Edit: Claude Opus 5.5 - 2026-11-03 - Motive: Admission cache write is compare-and-set against revocation.

# Maintenance Report

## 2026-11-07 — client records listed without SCAN

- **What:** new set `<prefix>:client_ids`, written by `add_item`,
  `update_client` and `remove_client` in the record's transaction; `__iter__`,
  `sync()`, the v1 -> v2 migration and `_search_brute_force` read it
  (`_iter_client_keys`). `ensure_client_index()` runs at start-up: marked or
  never-written namespaces need no walk; an older namespace is walked once with
  SCAN if the login may, then marked `<prefix>:client_ids_built`; refused, it
  warns once. `sync()` rebuilds every index key a record names, invalidates
  live admission records (epoch moved), drops ids whose record is gone, and
  declines while the index is unbuilt. Indexed searches drop records that no
  longer carry the value. The migration CLI indexes what it copies. Legacy
  cluster mode unchanged. Version `0.1.1a8+thalovant.2`.
- **Why:** SCAN is keyless, so a login confined to one namespace by its ACL key
  patterns can still list every key name in the database with it. Without SCAN
  the plugin's listing and repair paths failed.
- **Verification:**
  - `pytest tests`: 126 passed, 8 skipped (was 112 passed, 6 skipped). Four
    existing tests changed: the sync test now asserts the orphaned set is left
    and answers nothing (the old assertion moved to a new legacy-cluster test,
    which still removes it); the migration fakes learned `get`/`expire` and
    return pipeline results; the v1 fixtures carry the `count` every real
    namespace has.
  - `tests/test_client_index.py`: 13 unit tests, including a fake that raises
    NOPERM on SCAN and one that fails the test on any SCAN once built.
  - Real Redis 8.2.9 (the `redis@sha256:66b7f4c2…` image, RediSearch and
    RedisJSON loaded, throwaway container): `REDIS_ACL_TEST_ADMIN_URL=…
    pytest tests/test_client_index.py -k RealRedis`: 2 passed, under a user
    with `-@all +@connection +@transaction -@admin -@dangerous` and named data
    commands, no SCAN, no scripting. MONITOR during the run: SADD, SET, GET,
    MULTI, EXEC, INCRBY, SMEMBERS, MGET, HSET, EXPIRE, EXISTS, SREM, WATCH,
    SETNX, PING, HELLO, CLIENT; no SCAN, KEYS or EVAL from the plugin.

### Transparency Report

- **AI Model:** Claude Opus 5.5.
- **Actions Taken:** inventoried every SCAN in the package, designed the index
  set and its start-up backfill, rewrote `sync()` on it, updated the affected
  tests and wrote the new ones, ran them on fakes and on a throwaway Redis 8.2.9.
- **Oversight:** requested by the platform lead; not merged or released. The
  listener image that would carry it, and its rollout, are the owner's decision.


## 2026-11-03 — admission cache write is compare-and-set against revocation

- **What:** `_invalidate_api_key_records` moves `api_key_epoch:<key>` (INCR,
  then EXPIRE `admission_record_ttl`) before deleting the record, inside the
  caller's MULTI/EXEC. `get_client_by_api_key` reads that epoch before its
  authoritative read (`_read_epoch`). After it, `_refill_admission_record`
  WATCHes the epoch, re-reads it, and writes the record in MULTI/EXEC only
  if it is unchanged; a `WatchError` drops the write. The WATCH is not held
  across the authoritative read, so concurrent misses cannot exhaust the pool
  that read needs (CodeRabbit, #13). A lookup slower than half of
  `ADMISSION_EPOCH_TTL` (600 s) does not write. No guard, no write, and a
  one-time warning. `_lookup_logger` falls back to stdlib logging.
  `_search_with_index` logs the field it searched, never the value (on the
  admission path, the API key).
  Version `0.1.1a8+thalovant.1`.
- **Verification:**
  - `tests/test_admission_revocation_race.py` runs the revocation (or a
    demotion) inside the authoritative read, against the suite's `FakeRedis` (its pipelines honour WATCH). On 0.1.1a8,
    5 of its 6 tests fail: the revoked key is admitted from the restored
    record, and the demoted client keeps `is_admin`. With this change, all pass.
    Three more pin the ordering (the guard comes after the authoritative read;
    a revocation between the guard and the write is caught by WATCH; a slow
    lookup writes nothing). On 0.1.1a8 the file fails 8 of 9; with the guard
    held across the read (the first version of #13), it fails 3.
  - Against a real Redis 7.4.12, published 0.1.1a8 restores the revoked
    record (TTL 60 s) and admits the key; this build refuses it.
  - The suite's `FakeRedis`/`FakePipeline` learn WATCH, MULTI, EXPIRE and
    reset, so the existing admission tests go through the guarded write.
  - `TestLookupLoggerFallback`: 2 tests, both failing without the fallback.
  - `test_debug_output_is_still_produced_with_level_restored` now asserts the
    API key is absent from the debug lines, and fails on 0.1.1a8, which logged it.
  - `pytest tests`: 112 passed, 6 skipped.

### Transparency Report

- **AI Model:** Claude Opus 5.5.
- **Actions Taken:** wrote the interleaving tests first and ran them against
  the unchanged code, implemented the epoch and the guarded refill, extended
  the test fakes, and added the logger fallback and docstring fixes from the
  review of the sync PR.
- **Oversight:** requested by the platform lead. The listener image that
  carries this build, and any hub rollout, are the owner's decision.


## 2026-11-03 — fork brought up to the published 0.1.1a8

- **Why:** hubs run the published `hivemind-redis-database==0.1.1a8`; this fork's
  `dev` was 0.1.0a7, so no change made here could be reviewed against, or reach,
  what runs.
- **What:** `hivemind_redis_database/{__init__,migration,version}.py` are now
  byte-identical to the 0.1.1a8 wheel and sdist from PyPI (sha256
  `bf31cd76…6d3c` wheel, `b6553928…4fda` sdist, both matching PyPI's published
  digests). `tests/*.py`, `pyproject.toml` and `README.md` are the sdist's, except a
  `bitstring>=4.1.1,<5` runtime dependency (the bus client imports `BitStream`)
  and a README note on the opt-in in-process cache's cross-process delay. Removed with the
  0.1.0a7 line: `tests/test_dependency_security.py` (asserted that line's exact
  pins), `tests/e2e/` and `.github/workflows/policy-e2e.yml` (a hivemind-core 4.x
  stack whose `crypto_key` client API 5.x removed).
- **Verification:** `pytest tests` — 101 passed, 6 skipped (real-Redis
  integration tests skip without a server), on Python 3.13 with the `test`
  extra.

### Transparency Report

- **AI Model:** Claude Opus 5.5.
- **Actions Taken:** downloaded the published 0.1.1a8 wheel and sdist, checked
  their digests against PyPI, copied the release's files over the fork, removed
  the retired 0.1.0a7-line tests and workflow, ran the suite.
- **Oversight:** requested by the platform lead; reviewed through this PR's CI
  and CodeRabbit before merge.

## Dependency compatibility (historical: the 0.1.0a7 line, retired by the 0.1.1a8 sync above)

On that line, install requirements constrained filelock to 3.20.3. Fresh installs require bitstring>=4.1.1,<5 because the supported bus client
imports BitStream. This legacy Client representation supports
plugin-manager>=0.6.0a1,<0.9. Test extras match the existing policy-E2E stack:
hivescope0.4.0a2, core4.3.0a2, agent0.2.0a1 and manager0.6.0a1.
No Redis implementation or schema is changed. This fork is not compatible with
modern quota's manager>=0.10 requirement and does not replace the maintained
listener's Redis0.1.1a8.

Verification: full declared test extras pass 94 tests, including topology
tests, with 6 existing local real-Redis skips. The supported manager0.8.0a3
endpoint passes 91 unit tests with 6 skips before the final test-stack
regression was added. No runtime migration or skipped failure is used.
Wheel/sdist builds and exact built-metadata checks pass.
Those checks lived in `tests/test_dependency_security.py`, removed with that line.
No deployment, package publication, or vulnerability exception is performed.

## Transparency Report

- AI model: Codex, GPT-5-based coding agent; a more specific runtime ID is not exposed.
- Actions: inspected source/primary advisories, edited dependency constraints,
  added tests/documentation, ran tests and package checks.
- Oversight: owner-authorized remediation; changes remain subject to PR review
  and the coordinating agent's merge/release checks.

