Last Edit: Codex - 2026-10-25 - Motive: Record verified dependency security and compatibility constraints.

# FAQ

## Why is filelock pinned exactly?

3.20.3 contains the earlier fixes and predates the newer affected features.
A broad 3.x range can select an affected version. See the primary ranges and
verified limits in [dependency security](docs/dependency-security.md).

## Does this alter runtime behavior?

[RedisDB](hivemind_redis_database/__init__.py) is unchanged. The regression tests exercise classic
lock contention and real dependency metadata. Fresh installs also require bitstring>=4.1.1,<5 because the supported bus client imports BitStream. The existing plugin-manager range and CI pins are unchanged. With the coverage workflow's plugin-manager 0.6.0a1, the unit suite passes; the declared test extra selects newer Client behavior and retains one pre-existing metadata assertion failure. No API cap or runtime migration is included.

