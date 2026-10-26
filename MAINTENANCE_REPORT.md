Last Edit: Codex - 2026-10-25 - Motive: Record verified dependency security and compatibility constraints.

# Maintenance Report

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

