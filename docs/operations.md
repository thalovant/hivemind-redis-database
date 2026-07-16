# Operations

## Backup

The `hivemind-redis-migrate-cluster` CLI tool copies records between Redis
namespaces and can be used for manual exports. For a full snapshot, use
standard Redis persistence tools:

```bash
# BGSAVE triggers an async RDB snapshot on the Redis server
redis-cli -h 127.0.0.1 -p 6379 BGSAVE
# Find the dump.rdb file location
redis-cli CONFIG GET dir
```

For RDB + AOF replication or Redis Enterprise snapshots, follow your Redis
deployment's own backup procedure.

## Health check

`RedisDB` exposes a `health_check()` method that pings the connection:

```python
from hivemind_redis_database import RedisDB
db = RedisDB(host="127.0.0.1", port=6379)
assert db.health_check()
```

## Repairing inconsistency

If Redis indexes drift from the authoritative hash records (e.g. after an
interrupted write or manual key deletion), run:

```python
db.sync()
```

`sync()` scans all `<prefix>:client:<id>` hashes and rebuilds counters,
set indexes, and RediSearch documents.

## Migrating from another backend

```python
from hivemind_json_database import JsonDB  # or SQLiteDB
from hivemind_redis_database import RedisDB

src = JsonDB()
dst = RedisDB(host="127.0.0.1", port=6379)

for client in src:
    dst.add_item(client)
dst.commit()
```

Then update `server.json` to set `database.module` to `hivemind-redis-db-plugin`.

## Authoring a database backend plugin

See [hivemind-sqlite-database: authoring a plugin](https://github.com/JarbasHiveMind/hivemind-sqlite-database/blob/dev/docs/operations.md#authoring-a-database-backend-plugin)
for the `AbstractDB` contract and entry-point registration pattern.
