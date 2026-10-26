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
affects `>=3.20.4,<=4.2.0`; other reviewed ranges begin at 3.20.4,
3.22.0, or 4.1.0. [GHSA-2g95-wg33-c5x2](https://github.com/advisories/GHSA-2g95-wg33-c5x2)
affects `>=4.1.0,<=4.2.0`. Exact3.20.3 is outside these reviewed ranges;
this inventory does not replace fresh vulnerability scans.

Fresh installs require bitstring>=4.1.1,<5 because the supported bus client
imports BitStream. This legacy Client representation supports
plugin-manager>=0.6.0a1,<0.9. Test extras match the existing policy-E2E stack:
hivescope0.4.0a2, core4.3.0a2, agent0.2.0a1 and manager0.6.0a1.
No Redis implementation or schema is changed. This fork is not compatible with
modern quota's manager>=0.10 requirement and does not replace the maintained
listener's Redis0.1.1a8.

## Verification

- Full declared test extras pass 94 tests, including topology tests, with 6
  existing real-Redis integration skips because no local Redis server is
  available. No failing test is skipped or weakened by this repair.
- The supported manager0.8.0a3 endpoint passes 91 unit tests with 6 local
  real-Redis skips before the final test-stack metadata regression was added.
- [Regression tests](../tests/test_dependency_security.py) check the installed
  dependency and real classic-lock contention/release.
- Wheel and sdist builds pass; built wheel metadata contains exact filelock==3.20.3 and no direct-URL dependency.
- [RedisDB](../hivemind_redis_database/__init__.py) remains unchanged.

A passing library test does not approve a production rollout. The consuming
application's normal build and vulnerability gates remain required.

