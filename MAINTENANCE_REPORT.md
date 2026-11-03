Last Edit: Claude Opus 5.5 - 2026-11-03 - Motive: Bring the fork up to the published 0.1.1a8 source.

# Maintenance Report

## 2026-11-03 — fork brought up to the published 0.1.1a8

- **Why:** hubs run the published `hivemind-redis-database==0.1.1a8`; this fork's
  `dev` was 0.1.0a7, so no change made here could be reviewed against, or reach,
  what runs.
- **What:** `hivemind_redis_database/{__init__,migration,version}.py` are now
  byte-identical to the 0.1.1a8 wheel and sdist from PyPI (sha256
  `bf31cd76…6d3c` wheel, `b6553928…4fda` sdist, both matching PyPI's published
  digests). `tests/*.py`, `pyproject.toml` and `README.md` are the sdist's; the
  `test` extra adds a test-only `bitstring>=4.1.1,<5` cap. Removed with the
  0.1.0a7 line: `tests/test_dependency_security.py` (asserted that line's exact
  pins), `tests/e2e/` and `.github/workflows/policy-e2e.yml` (a hivemind-core 4.x
  stack whose `crypto_key` client API 5.x removed).
- **Verification:** `pytest tests` — 103 passed, 6 skipped (real-Redis
  integration tests skip without a server), on Python 3.13 with the `test`
  extra.

### Transparency Report

- **AI Model:** Claude Opus 5.5.
- **Actions Taken:** downloaded the published 0.1.1a8 wheel and sdist, checked
  their digests against PyPI, copied the release's files over the fork, removed
  the retired 0.1.0a7-line tests and workflow, ran the suite.
- **Oversight:** requested by the platform lead; reviewed through this PR's CI
  and CodeRabbit before merge.

## Dependency compatibility

Actual install requirements constrain filelock to 3.20.3. Fresh installs require bitstring>=4.1.1,<5 because the supported bus client
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
See [tests/test_dependency_security.py](tests/test_dependency_security.py).
No deployment, package publication, or vulnerability exception is performed.

## Transparency Report

- AI model: Codex, GPT-5-based coding agent; a more specific runtime ID is not exposed.
- Actions: inspected source/primary advisories, edited dependency constraints,
  added tests/documentation, ran tests and package checks.
- Oversight: owner-authorized remediation; changes remain subject to PR review
  and the coordinating agent's merge/release checks.

