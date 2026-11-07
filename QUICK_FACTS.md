Last Edit: Claude Opus 5.5 - 2026-11-07 - Motive: List, search and sync client records through an index set, so a login without SCAN runs every path.
[previous header] Last Edit: Claude Opus 5.5 - 2026-11-03 - Motive: Admission cache write is compare-and-set against revocation.

# Quick Facts

| Field | Value |
| --- | --- |
| Package | hivemind-redis-database |
| Version | 0.1.1a8+thalovant.2 (the 0.1.1a8 release, the compare-and-set admission refill, and the client index) |
| Entry points | hivemind.database: hivemind-redis-db-plugin; hivemind-redis-migrate-cluster |
| Dependencies | The 0.1.1a8 release's own, plus bitstring>=4.1.1,<5: redis>=4.2.0, hiredis>=2.0.0, hivemind-plugin-manager>=0.5.0,<1.0.0, hivemind-bus-client>=0.4.4 |
| Test graph | The release's `test` extra; 126 passed, 8 real-Redis skips (2 more pass against a real Redis with `REDIS_ACL_TEST_ADMIN_URL`) |
| Adoption | Built into the hub listener image from a pinned commit of this repository |
| Admission refill | Compare-and-set: epoch read before the lookup; after it, WATCH `api_key_epoch:<key>`, re-check, MULTI/EXEC `SET api_key_record:<key> EX` |
| Client index | `<prefix>:client_ids` (set of record ids, written with each record); `<prefix>:client_ids_built` marks it complete |
| Keyless commands needed | None after start-up (non-legacy modes). Start-up probes INFO, CLUSTER INFO, MODULE LIST, FT._LIST may be refused |
| SCAN left | Legacy cluster mode (`_iter_client_keys`, `_sync_by_key_walk`, `_count_client_records`); `ensure_client_index` once on an unindexed namespace; `hivemind-redis-migrate-cluster --clear-target` |

Source: [RedisDB](hivemind_redis_database/__init__.py); [dependency evidence](docs/dependency-security.md).

