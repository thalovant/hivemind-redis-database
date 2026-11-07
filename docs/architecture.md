Last Edit: Claude Opus 5.5 - 2026-11-07 - Motive: List, search and sync client records through an index set, so a login without SCAN runs every path.

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
| `<prefix>:api_key_records` | Hash | Materialized `api_key` to serialized client record used by admission. |
| `<prefix>:idx:<id>` | String | `"1"` (membership sentinel). |
| `<prefix>:count` | String | Integer count of non-revoked clients. |
| `<prefix>:id_seq` | String | Monotone ID sequence counter. |
| `<prefix>:client_ids` | Set | Id of every client record (revoked ones included). Written in the same transaction as the record. |
| `<prefix>:client_ids_built` | String | Present once `client_ids` lists every record written before it existed; the value names who built it (`plugin`, or an administrator's tool). |

### Listing records without SCAN

`__iter__`, `sync()`, the schema migration and searches by a field with no
index (`search_by_value("client_id", ...)`) find the records through
`<prefix>:client_ids` (`RedisDB._iter_client_keys`), not by walking the
keyspace. SCAN is keyless: Redis answers it with every key name in the
database whatever the login's key patterns are, so a login confined to one
namespace by its ACL should not hold it. With this layout the plugin needs
no keyless command after start-up.

`RedisDB.ensure_client_index()` runs at start-up, before the schema
migration. A namespace marked built, or one that never held a record (no
`count`, `id_seq`, `client_ids` or `client:1`), needs no walk. A namespace
written by an older release is walked once with `SCAN MATCH <prefix>:client:*`
when the login may SCAN, and marked. When it may not, one warning says so,
the namespace stays unmarked, lookups by name and api key work as before,
and listing, `sync()` and brute-force search see only indexed clients until
an administrator builds the index: add every `<prefix>:client:<id>` id to
`<prefix>:client_ids` with a login that may SCAN, then set
`<prefix>:client_ids_built`. A writer that stores records directly (an
operator) must add the id to `client_ids` beside the record, and remove it
when it deletes the record.

Legacy cluster mode (no `cluster_hash_tag`) keeps its key walk.

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

`sync()` rebuilds the derived keys (counters, set indexes, materialized API-key
records, and RediSearch hash documents) from the authoritative
`<prefix>:client:<id>` records. It is a recovery tool, not a transaction
boundary — use it after interrupted writes or manual Redis changes.

It finds the records through `<prefix>:client_ids`. Every index key a record
names (its `name:` and `api_key:` sets, its `idx:` document, its admission
record and epoch) is dropped and rebuilt in one transaction, and ids whose
record is gone leave `client_ids`. An index set keyed by a value no record
carries any more cannot be found without walking the keyspace, so it is left
in place; it is harmless because indexed searches only return records that
still carry the value searched for. A stale create marker that never reached
the index expires on its own (30 s). `sync()` returns False without writing
while the index is not marked built, because a rebuild from part of the
records would undercount `count`. Legacy cluster mode keeps the key walk.

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
