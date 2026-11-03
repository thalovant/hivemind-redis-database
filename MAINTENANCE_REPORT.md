Last Edit: Claude Opus 5.5 - 2026-11-03 - Motive: Re-pin filelock outside the advisory ranges (3.20.3 on Python 3.10, >=4.2.1,<5 on 3.11+) after the 0.1.1a8 sync dropped it.

# Maintenance Report

## 2026-11-03: filelock re-pinned for the dependency scan

- **Change:** `pyproject.toml` pins `filelock==3.20.3; python_version < '3.11'` and `filelock>=4.2.1,<5; python_version >= '3.11'`; new `tests/test_dependency_pins.py` keeps both pins and the bitstring cap.
- **Why:** after #12 the platform scan resolved filelock 3.32.7 for this package (HIGH advisory, fixed in 4.2.1) and failed for every infrastructure change.
- **Verification:** `pytest tests/test_dependency_pins.py`: 2 passed. `uv pip compile pyproject.toml` on 3.13 with the reviewed combo-lock override: filelock 4.2.7; on 3.10 without it: filelock 3.20.3 with combo-lock 0.3.1.
- **Transparency Report:** AI model Claude Opus 5.5 (`claude-opus-5-5`) in Claude Code; wrote the pin, the guard test and these notes, verified the resolutions locally; no human review yet.

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

