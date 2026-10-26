Last Edit: Codex - 2026-10-25 - Motive: Record verified dependency security and compatibility constraints.

# Dependency Security

The real installation source, `pyproject.toml`, pins `filelock==3.20.3`.
This maintains the classic FileLock interface accepted by upstream combo-lock's
`filelock~=3.0` requirement and Python 3.10. No direct URL or scanner exception
is used, and no claim is made that future advisories cannot affect this version.

## Evidence

The GitHub Advisory Database lists older affected ranges `<3.20.1`
([GHSA-w853-jp5j-5j7f](https://github.com/advisories/GHSA-w853-jp5j-5j7f)) and
`<3.20.3` ([GHSA-qmgc-5h2g-mvrw](https://github.com/advisories/GHSA-qmgc-5h2g-mvrw)).
The newer HIGH [GHSA-v4x8-3wr5-3wq8](https://github.com/advisories/GHSA-v4x8-3wr5-3wq8)
affects `>=3.20.4,<=4.2.0`; the other five published ranges begin at 3.20.4,
3.22.0, or 4.1.0. Exact 3.20.3 lies outside all eight ranges.

Fresh installs also require bitstring>=4.1.1,<5 because the supported bus client imports BitStream. The existing plugin-manager range and CI pins are unchanged. With the coverage workflow's plugin-manager 0.6.0a1, the unit suite passes; the declared test extra selects newer Client behavior and retains one pre-existing metadata assertion failure. No API cap or runtime migration is included.

## Verification

- 90 passed, 6 skipped with the existing CI coverage plugin-manager 0.6.0a1 pin; normal declared test extras produce 89 passed, 6 skipped and 1 pre-existing Client metadata failure. The three dependency regressions pass.
- [Regression tests](../tests/test_dependency_security.py) check the installed
  dependency and real classic-lock contention/release.
- Wheel and sdist builds pass; built wheel metadata contains exact filelock==3.20.3 and no direct-URL dependency.
- [RedisDB](../hivemind_redis_database/__init__.py) remains unchanged.

A passing library test does not approve a production rollout. The consuming
application's normal build and vulnerability gates remain required.

