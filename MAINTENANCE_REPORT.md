Last Edit: Claude Opus 5.5 - 2026-11-03 - Motive: Admission cache write is compare-and-set against revocation.

# Maintenance Report

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

