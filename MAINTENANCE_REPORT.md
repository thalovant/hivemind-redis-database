Last Edit: Codex - 2026-10-25 - Motive: Record verified dependency security and compatibility constraints.

# Maintenance Report

## Dependency compatibility

Actual install requirements constrain filelock to 3.20.3. Fresh installs also require bitstring>=4.1.1,<5 because the supported bus client imports BitStream. The existing plugin-manager range and CI pins are unchanged. With the coverage workflow's plugin-manager 0.6.0a1, the unit suite passes; the declared test extra selects newer Client behavior and retains one pre-existing metadata assertion failure. No API cap or runtime migration is included.

Verification: 90 passed, 6 skipped with the existing CI coverage plugin-manager 0.6.0a1 pin; normal declared test extras produce 89 passed, 6 skipped and 1 pre-existing Client metadata failure. The three dependency regressions pass. Wheel/sdist builds and exact built-metadata checks pass.
See [tests/test_dependency_security.py](tests/test_dependency_security.py).
No deployment, package publication, or vulnerability exception is performed.

## Transparency Report

- AI model: Codex, GPT-5-based coding agent; a more specific runtime ID is not exposed.
- Actions: inspected source/primary advisories, edited dependency constraints,
  added tests/documentation, ran tests and package checks.
- Oversight: owner-authorized remediation; changes remain subject to PR review
  and the coordinating agent's merge/release checks.

