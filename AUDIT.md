Last Edit: Claude Opus 5.5 - 2026-11-03 - Motive: Re-pin filelock outside the advisory ranges (3.20.3 on Python 3.10, >=4.2.1,<5 on 3.11+) after the 0.1.1a8 sync dropped it.

# Audit

## 2026-11-03: filelock re-pinned

The 0.1.1a8 sync (#12) retired the exact `filelock` pin on the grounds that the runtime image constrains it. The platform's dependency scan resolves this repository's graph on its own, though, and then selected filelock 3.32.7 (inside the published HIGH advisory range, fixed in 4.2.1), which failed the scan for every infrastructure change. `pyproject.toml` now pins `filelock==3.20.3` on Python 3.10 (no fixed release exists there; the least-affected one) and `filelock>=4.2.1,<5` on 3.11+, which resolves with the reviewed combo-lock build the runtime images install (4.2.7 at the time of writing). Guard: `tests/test_dependency_pins.py`. A plain PyPI install on 3.11+ now refuses to resolve rather than silently taking an affected filelock, the same trade the skills made.

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

