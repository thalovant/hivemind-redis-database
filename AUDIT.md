Last Edit: Codex - 2026-10-25 - Motive: Record verified dependency security and compatibility constraints.

# Audit

The security constraint is carried by the real installer, not solely by a
scanner input. [Dependency tests](tests/test_dependency_security.py) verify
exact filelock selection and lock behavior. 90 passed, 6 skipped with the existing CI coverage plugin-manager 0.6.0a1 pin; normal declared test extras produce 89 passed, 6 skipped and 1 pre-existing Client metadata failure. The three dependency regressions pass.

Fresh installs also require bitstring>=4.1.1,<5 because the supported bus client imports BitStream. The existing plugin-manager range and CI pins are unchanged. With the coverage workflow's plugin-manager 0.6.0a1, the unit suite passes; the declared test extra selects newer Client behavior and retains one pre-existing metadata assertion failure. No API cap or runtime migration is included.

RedisDB in [the runtime source](hivemind_redis_database/__init__.py) is not modified.
This is not a complete audit of that implementation. Future advisory checks
and consumer artifact tests remain required; [evidence](docs/dependency-security.md).

