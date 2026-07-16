# Architecture

## Class hierarchy

```
hivemind_plugin_manager.database.AbstractDB   (abstract)
        │
        └─ hivemind_redis_database.RedisDB
                │
                ├─ redis.Redis (single-node mode)
                └─ redis.cluster.RedisCluster (cluster mode)
```

`RedisDB` auto-detects single-node vs. cluster by trying a `CLUSTER INFO`
command after connecting. If the node reports `cluster_enabled:1`, it switches
to `RedisCluster`.

## Key schema

Each client record is stored as a set of keys under a configurable prefix
(`index_prefix`, default `"client"`). In single-node mode:

| Key pattern | Type | Content |
|---|---|---|
| `<prefix>:client:<id>` | Hash | Full client record (all fields). |
| `<prefix>:name:<name>` | String | `client_id` of the matching record. |
| `<prefix>:api_key:<api_key>` | String | `client_id` of the matching record. |
| `<prefix>:idx:<id>` | String | `"1"` (membership sentinel). |
| `<prefix>:count` | String | Integer count of non-revoked clients. |
| `<prefix>:id_seq` | String | Monotone ID sequence counter. |

### Cluster mode with `cluster_hash_tag`

When `cluster_hash_tag` is set (recommended for new cluster deployments),
all keys embed the tag in braces:

```
client:{clients}:client:1
client:{clients}:name:alpha
client:{clients}:api_key:alpha-key
client:{clients}:idx:1
client:{clients}:count
client:{clients}:id_seq
```

The hash tag forces all keys into the same hash slot, enabling
`RedisCluster.pipeline(transaction=True)` for atomic multi-key writes.

## RediSearch acceleration

When the RediSearch module is loaded (`MODULE LIST` returns a module named
`search`), `RedisDB` creates a secondary FT index over the hash records and
uses `FT.SEARCH` for `search_by_value("name", ...)` and
`search_by_value("api_key", ...)`.

Without RediSearch the backend falls back to Redis set-index lookups. Either
way, search remains **exact-match** — RediSearch is used as an accelerator,
not for full-text or fuzzy queries.

## sync()

`sync()` rebuilds the derived keys (counters, set indexes, RediSearch hash
documents) from the authoritative `<prefix>:client:<id>` hash records. It is
a recovery tool, not a transaction boundary — use it after interrupted writes
or manual Redis changes.

## Schema migration

`hivemind-plugin-manager`'s `AbstractDB.migrate()` contract is implemented but
the Redis backend does not track a persistent schema version on disk (there is
no SQLite `PRAGMA user_version` equivalent). Migrations are handled at the
application level.

For Redis Cluster migrations (moving from the legacy untagged key layout to
the `cluster_hash_tag` layout), use the provided CLI tool:

```bash
hivemind-redis-migrate-cluster \
  --config ~/.config/hivemind-core/server.json \
  --target-cluster-hash-tag clients
```

See [cluster_consistency.md](cluster_consistency.md) for the full migration
plan and rollback procedure.

## Authoring a database backend plugin

See [hivemind-sqlite-database: authoring a plugin](https://github.com/JarbasHiveMind/hivemind-sqlite-database/blob/dev/docs/operations.md#authoring-a-database-backend-plugin)
for the `AbstractDB` contract and `pyproject.toml` entry-point registration pattern.
The contract is the same regardless of which storage technology you use underneath.
