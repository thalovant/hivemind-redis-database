Last Edit: Codex - 2026-10-25 - Motive: Record verified dependency security and compatibility constraints.

# Audit

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

