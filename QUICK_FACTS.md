Last Edit: Codex - 2026-10-25 - Motive: Record verified dependency security and compatibility constraints.

# Quick Facts

| Field | Value |
| --- | --- |
| Package | hivemind-redis-database |
| Version | 0.1.0a7 |
| Entry points | hivemind.database: hivemind-redis-db-plugin; hivemind-redis-migrate-cluster |
| filelock | Exact 3.20.3 in real installation requirements |
| Legacy Client API | manager>=0.6.0a1,<0.9; not compatible with modern quota>=0.10 graph |
| Test graph | Existing policy-E2E stack; 94 passed, 6 local real-Redis skips |
| Adoption | Source-only repair; not a replacement for maintained listener Redis0.1.1a8 |

Source: [RedisDB](hivemind_redis_database/__init__.py); [dependency evidence](docs/dependency-security.md).

