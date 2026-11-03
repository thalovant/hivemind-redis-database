Last Edit: Claude Opus 5.5 - 2026-11-03 - Motive: Re-pin filelock outside the advisory ranges (3.20.3 on Python 3.10, >=4.2.1,<5 on 3.11+) after the 0.1.1a8 sync dropped it.

# FAQ

## Why is filelock pinned?

So that this package's own dependency graph never selects an affected
filelock: 4.2.1 is the first release outside the published advisory ranges, so
Python 3.11+ requires `>=4.2.1,<5` (it resolves with the reviewed combo-lock
build the runtime images install). Python 3.10 has no fixed release and keeps
3.20.3, the least-affected one. `tests/test_dependency_pins.py` guards both.

## Does this alter runtime behavior?

[RedisDB](hivemind_redis_database/__init__.py) is unchanged. The regression tests exercise classic
lock contention and real dependency metadata. Fresh installs require bitstring>=4.1.1,<5 because the supported bus client
imports BitStream. This legacy Client representation supports
plugin-manager>=0.6.0a1,<0.9. Test extras match the existing policy-E2E stack:
hivescope0.4.0a2, core4.3.0a2, agent0.2.0a1 and manager0.6.0a1.
No Redis implementation or schema is changed. This fork is not compatible with
modern quota's manager>=0.10 requirement and does not replace the maintained
listener's Redis0.1.1a8.

