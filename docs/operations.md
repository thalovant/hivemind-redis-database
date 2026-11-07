Last Edit: Claude Opus 5.5 - 2026-11-07 - Motive: List, search and sync client records through an index set, so a login without SCAN runs every path.

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

`sync()` reads every record listed in `<prefix>:client_ids` and rebuilds
counters, set indexes, admission records and RediSearch documents, without
SCAN (see [architecture: sync()](architecture.md#sync)).

## Logins without SCAN

A Redis login confined to one namespace (ACL key patterns `~<prefix>:*`)
should not hold SCAN, which lists every key name in the database. This
plugin needs none once the namespace's client index is built:
`GET SET SETNX MGET INCR(BY) EXPIRE DEL EXISTS SADD SREM SMEMBERS HSET`,
`WATCH MULTI EXEC UNWATCH`, and `PING`/`HELLO`/`CLIENT SETINFO`. Its start-up
probes (`INFO`, `CLUSTER INFO`, `MODULE LIST`, `FT._LIST`) may be refused; it
then runs single-node without RediSearch.

Upgrading a namespace written by an older release under such a login: build
the index once with a login that may SCAN, before or after the upgrade.

```bash
redis-cli --scan --pattern '<prefix>:client:*' | sed 's/.*://' \
  | xargs -r redis-cli SADD '<prefix>:client_ids'
redis-cli SET '<prefix>:client_ids_built' admin
```

Until then the plugin warns once at start-up, and `sync()` declines to run.

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
