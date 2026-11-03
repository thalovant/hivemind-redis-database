Last Edit: Claude Opus 5.5 - 2026-11-03 - Motive: Admission cache write is compare-and-set against revocation.

# Audit

## 2026-11-03 — admission cache write vs. concurrent invalidation (fixed)

Finding: `get_client_by_api_key` refilled `api_key_record:<key>` with a plain
`SET ... EX` after its authoritative read. A revocation or update committing
in between deleted a record that did not exist yet, and the refill then wrote
the pre-change record, which was served until `admission_record_ttl` expired.
Fixed by a per-key epoch that every invalidation moves and the refill WATCHes
(`hivemind_redis_database/__init__.py`: `_api_key_epoch_key`,
`_invalidate_api_key_records`, `_admission_refill_guard`,
`_refill_admission_record`). Evidence:
`tests/test_admission_revocation_race.py`, which fails 5 of 6 on 0.1.1a8.
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

