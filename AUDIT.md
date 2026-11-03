Last Edit: Claude Opus 5.5 - 2026-11-03 - Motive: Record that the 0.1.0a7-line constraints below are retired by the 0.1.1a8 sync.

# Audit

## 2026-11-03 — superseded by the 0.1.1a8 sync

The fork now carries the published 0.1.1a8 package. The dependency findings
below described the 0.1.0a7 line (exact `filelock` pin, `hivemind-plugin-manager<0.9`,
the 4.x end-to-end stack) and no longer apply: the package declares the
release's own dependencies, and the runtime image that installs it pins
`filelock` and `bitstring` itself. `pytest tests`: 103 passed, 6 skipped.

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

