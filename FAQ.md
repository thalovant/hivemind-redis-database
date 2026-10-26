Last Edit: Codex - 2026-10-25 - Motive: Record verified dependency security and compatibility constraints.

# FAQ

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

