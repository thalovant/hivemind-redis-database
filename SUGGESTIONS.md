Last Edit: Claude Opus 5.5 - 2026-11-07 - Motive: List, search and sync client records through an index set, so a login without SCAN runs every path.
[previous header] Last Edit: Codex - 2026-10-25 - Motive: Record verified dependency security and compatibility constraints.

# Suggestions

| Problem | Proposal | Impact |
| --- | --- | --- |
| An exact compatibility pin can become stale | Recheck all filelock advisory ranges during consumer release scans | Prevent stale security assumptions |
| Dependency APIs can change independently of this package | Retain [installed metadata and behavior tests](tests/test_dependency_security.py); review bounds before widening | Catch fresh-install incompatibilities |
| Legacy Client metadata differs from modern manager releases | Require an explicit schema/API migration before widening manager<0.9; retain the maintained listener package | Avoid a silent platform downgrade |
| Topology helpers and connection constructors evolve together | Keep test extras and policy-E2E on the same verified stack | Preserve complete topology coverage |
| `ClientDatabase.add_client` (hivemind-core) picks `total_clients() + 1` as the new id, and `add_item` updates whatever record holds it | Let `add_item` allocate from `id_seq` when the provided id is taken by a different api key, or have core pass no id | Stops a CLI add from overwriting another client when `count` drifted below the highest id |
| Start-up probes (`INFO`, `CLUSTER INFO`, `MODULE LIST`, `FT._LIST`) are refused under a confined login and fill its ACL LOG | Config switches `detect_cluster: false` and `redisearch: false` that skip the probes | A clean ACL LOG is then a usable signal for a missing grant |
| Legacy cluster mode still walks the keyspace | Keep `client_ids` there too (one non-transactional SADD per write) and read it once indexed | Same no-SCAN guarantee for untagged cluster namespaces |

Source boundary: [RedisDB](hivemind_redis_database/__init__.py).

