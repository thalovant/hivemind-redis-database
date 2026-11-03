Last Edit: Claude Opus 5.5 - 2026-11-03 - Motive: Admission cache write is compare-and-set against revocation.

# Quick Facts

| Field | Value |
| --- | --- |
| Package | hivemind-redis-database |
| Version | 0.1.1a8+thalovant.1 (the 0.1.1a8 release plus the compare-and-set admission refill) |
| Entry points | hivemind.database: hivemind-redis-db-plugin; hivemind-redis-migrate-cluster |
| Dependencies | The 0.1.1a8 release's own, plus bitstring>=4.1.1,<5: redis>=4.2.0, hiredis>=2.0.0, hivemind-plugin-manager>=0.5.0,<1.0.0, hivemind-bus-client>=0.4.4 |
| Test graph | The release's `test` extra; 112 passed, 6 real-Redis skips |
| Adoption | Built into the hub listener image from a pinned commit of this repository |
| Admission refill | Compare-and-set: epoch read before the lookup; after it, WATCH `api_key_epoch:<key>`, re-check, MULTI/EXEC `SET api_key_record:<key> EX` |

Source: [RedisDB](hivemind_redis_database/__init__.py); [dependency evidence](docs/dependency-security.md).

