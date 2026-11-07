"""The client index: listing, sync and searches without SCAN.

A login confined to one namespace by its ACL key patterns should not hold SCAN,
because Redis answers SCAN with every key name in the database whatever those
patterns are. These tests run the plugin on a fake Redis that refuses SCAN the
way such a login is refused (NOPERM), and on one that fails the test if SCAN is
called at all once the index is built.
"""
import json
import os
import unittest
from unittest.mock import patch
from uuid import uuid4

import redis

from hivemind_plugin_manager.database import Client
from hivemind_redis_database import CLIENT_INDEX_BUILT_BY_PLUGIN, RedisDB
from test_redisdb import FakeRedis


class NoScanRedis(FakeRedis):
    """A login without SCAN: Redis refuses it with NOPERM."""

    def __init__(self):
        super().__init__()
        self.scan_calls = 0

    def scan_iter(self, pattern, count=None):
        self.scan_calls += 1
        raise redis.exceptions.NoPermissionError(
            "NOPERM User hub has no permissions to run the 'scan' command")


class CountingScanRedis(FakeRedis):
    """SCAN works, and every call is counted."""

    def __init__(self):
        super().__init__()
        self.scan_calls = 0

    def scan_iter(self, pattern, count=None):
        self.scan_calls += 1
        return super().scan_iter(pattern, count=count)


def build_db(redis_client, prefix="client"):
    db = object.__new__(RedisDB)
    db.redis = redis_client
    db.redis_pool = redis_client.connection_pool
    db.index_prefix = prefix
    db.redisearch_available = False
    db.is_cluster = False
    db.cluster_hash_tag = None
    db.api_key_cache_ttl = 0.0
    db.admission_record_ttl = 60
    return db


def operator_record(redis_client, client_id, api_key, name, prefix="client"):
    """A record written the way an operator writes one directly: record, name
    and api key sets, search document, counter, and the client index."""
    redis_client.storage[f"{prefix}:client:{client_id}"] = json.dumps(
        {"client_id": client_id, "api_key": api_key, "name": name, "allowed_types": [],
         "metadata": {}})
    redis_client.sadd(f"{prefix}:name:{name}", client_id)
    redis_client.sadd(f"{prefix}:api_key:{api_key}", client_id)
    redis_client.sadd(f"{prefix}:client_ids", client_id)
    redis_client.hset(f"{prefix}:idx:{client_id}", mapping={"name": name, "api_key": api_key})
    redis_client.incr(f"{prefix}:count")


def legacy_record(redis_client, client_id, api_key, name, prefix="client"):
    """A record written before the index existed: everything but client_ids."""
    redis_client.storage[f"{prefix}:client:{client_id}"] = Client(
        client_id=client_id, api_key=api_key, name=name).serialize()
    redis_client.sadd(f"{prefix}:name:{name}", client_id)
    redis_client.sadd(f"{prefix}:api_key:{api_key}", client_id)
    redis_client.incr(f"{prefix}:count")
    redis_client.storage[f"{prefix}:id_seq"] = str(client_id)


class FreshNamespaceTests(unittest.TestCase):
    def test_a_namespace_that_never_held_a_record_is_marked_without_scan(self):
        fake = NoScanRedis()
        db = build_db(fake)

        self.assertTrue(db.ensure_client_index())

        self.assertEqual(fake.scan_calls, 0)
        self.assertEqual(fake.get("client:client_ids_built"), CLIENT_INDEX_BUILT_BY_PLUGIN)

    def test_init_on_a_fresh_namespace_never_scans(self):
        fake = NoScanRedis()
        with patch.object(RedisDB, "_detect_cluster", return_value=False), \
                patch.object(RedisDB, "_create_single_connection", return_value=fake), \
                patch.object(RedisDB, "_check_redisearch_availability", return_value=False), \
                patch.object(RedisDB, "health_check", return_value=True):
            db = RedisDB(index_prefix="hub-new")

        self.assertEqual(fake.scan_calls, 0)
        self.assertTrue(fake.exists("hub-new:client_ids_built"))
        # The v1 -> v2 schema migration ran over the (empty) index.
        self.assertEqual(int(fake.get("hub-new:schema_version")), 2)
        self.assertTrue(db.add_item(Client(client_id=1, api_key="k1", name="one")))
        self.assertEqual([c.name for c in db], ["one"])
        self.assertEqual(fake.scan_calls, 0)


class WritePathTests(unittest.TestCase):
    def setUp(self):
        self.fake = NoScanRedis()
        self.fake.storage["client:client_ids_built"] = "operator"
        self.db = build_db(self.fake)

    def test_add_update_and_revoke_keep_the_record_in_the_index(self):
        self.assertTrue(self.db.add_item(Client(client_id=1, api_key="k1", name="one")))
        self.assertTrue(self.db.add_item(Client(client_id=2, api_key="k2", name="two")))
        ids = self.fake.smembers("client:client_ids")
        self.assertEqual(len(ids), 2)

        first = self.db.get_client_by_api_key("k1")
        first.name = "uno"
        self.assertTrue(self.db.update_client(first))
        self.assertTrue(self.db.remove_client(str(first.client_id)))

        self.assertEqual(self.fake.smembers("client:client_ids"), ids)
        # Revoked records stay records and stay out of listings, as before.
        self.assertEqual([c.name for c in self.db], ["two"])
        self.assertEqual(self.fake.scan_calls, 0)

    def test_the_index_rides_the_records_transaction(self):
        self.db.add_item(Client(client_id=1, api_key="k1", name="one"))
        flags = self.fake.pipeline_transaction_flags
        self.assertTrue(flags and all(flags))

    def test_update_mends_an_index_that_missed_the_record(self):
        legacy_record(self.fake, 4, "k4", "four")
        client = self.db.get_client_by_api_key("k4")
        client.name = "vier"

        self.assertTrue(self.db.update_client(client))

        self.assertIn("4", self.fake.smembers("client:client_ids"))


class ReadPathTests(unittest.TestCase):
    """Once the index is built nothing may scan."""

    def setUp(self):
        self.fake = CountingScanRedis()
        self.fake.storage["client:client_ids_built"] = "operator"
        self.db = build_db(self.fake)
        operator_record(self.fake, 1, "key-1", "kitchen")
        operator_record(self.fake, 2, "key-2", "office")
        operator_record(self.fake, 10, "key-10", "garage")

    def tearDown(self):
        self.assertEqual(self.fake.scan_calls, 0)

    def test_iteration_lists_every_indexed_record_in_id_order(self):
        self.assertEqual([c.client_id for c in self.db], [1, 2, 10])

    def test_brute_force_search_walks_the_index(self):
        found = self.db.search_by_value("client_id", 10)
        self.assertEqual([c.name for c in found], ["garage"])

    def test_indexed_search_drops_records_that_moved_on(self):
        # Rotated outside the plugin and the operator: the old index entry
        # remains and must not answer for the record.
        record = json.loads(self.fake.storage["client:client:2"])
        record["api_key"] = "key-2b"
        self.fake.storage["client:client:2"] = json.dumps(record)

        self.assertIsNone(self.db.get_client_by_api_key("key-2"))
        self.assertEqual(self.db.search_by_value("api_key", "key-2"), [])

    def test_sync_rebuilds_from_the_index_and_drops_ids_that_are_gone(self):
        del self.fake.storage["client:client:10"]
        self.fake.storage["client:count"] = "42"
        self.fake.storage["client:name:kitchen"] = {"1", "77"}

        self.assertTrue(self.db.sync())

        self.assertEqual(self.fake.smembers("client:client_ids"), {"1", "2"})
        self.assertEqual(self.fake.get("client:count"), 2)
        self.assertEqual(self.fake.get("client:id_seq"), 2)
        self.assertEqual(self.fake.smembers("client:name:kitchen"), {"1"})
        self.assertNotIn("client:idx:10", self.fake.hashes)
        # Each live key's admission record is dropped and its epoch moved.
        self.assertEqual(self.fake.get("client:api_key_epoch:key-1"), "1")

    def test_migrate_walks_the_index(self):
        record = json.loads(self.fake.storage["client:client:1"])
        record["intent_blacklist"] = ["i:1"]
        self.fake.storage["client:client:1"] = json.dumps(record)

        self.db.migrate(from_version=1)

        stored = json.loads(self.fake.storage["client:client:1"])
        self.assertNotIn("intent_blacklist", stored)
        self.assertEqual(stored["metadata"]["intent_blacklist"], ["i:1"])


class BackfillTests(unittest.TestCase):
    def test_a_login_that_may_scan_builds_the_index_once(self):
        fake = CountingScanRedis()
        legacy_record(fake, 1, "k1", "one")
        legacy_record(fake, 2, "k2", "two")
        db = build_db(fake)

        self.assertTrue(db.ensure_client_index())
        self.assertTrue(db.ensure_client_index())

        self.assertEqual(fake.scan_calls, 1)
        self.assertEqual(fake.smembers("client:client_ids"), {"1", "2"})
        self.assertEqual(fake.get("client:client_ids_built"), CLIENT_INDEX_BUILT_BY_PLUGIN)
        self.assertEqual(sorted(c.name for c in db), ["one", "two"])

    def test_a_login_without_scan_says_so_once_and_leaves_the_namespace_unmarked(self):
        fake = NoScanRedis()
        legacy_record(fake, 1, "k1", "one")
        db = build_db(fake)

        with patch("hivemind_redis_database.LOG") as log:
            self.assertFalse(db.ensure_client_index())
            self.assertFalse(db.ensure_client_index())

        warnings = [call.args[0] for call in log.warning.call_args_list]
        self.assertEqual(len([line for line in warnings if "not built" in line]), 1)
        # Refused once, not asked again by this process.
        self.assertEqual(fake.scan_calls, 1)
        self.assertFalse(fake.exists("client:client_ids_built"))
        # Lookups by api key and name do not need the index.
        self.assertEqual(db.get_client_by_api_key("k1").name, "one")
        self.assertEqual([c.name for c in db.search_by_value("name", "one")], ["one"])
        # Listing sees only what is indexed, and sync refuses to rebuild from it.
        self.assertEqual(list(db), [])
        self.assertFalse(db.sync())
        self.assertEqual(fake.get("client:count"), "1")
        self.assertEqual(fake.smembers("client:name:one"), {"1"})
        self.assertEqual(fake.scan_calls, 1)
        # An administrator's backfill is seen without a restart.
        fake.sadd("client:client_ids", 1)
        fake.set("client:client_ids_built", "operator")
        self.assertTrue(db.sync())
        self.assertEqual([c.name for c in db], ["one"])

    def test_an_administrators_backfill_is_all_the_plugin_needs(self):
        fake = NoScanRedis()
        legacy_record(fake, 1, "k1", "one")
        legacy_record(fake, 2, "k2", "two")
        # What the operator does with its own login: the walk, then the marker.
        for client_id in (1, 2):
            fake.sadd("client:client_ids", client_id)
        fake.set("client:client_ids_built", "operator")
        db = build_db(fake)

        self.assertTrue(db.ensure_client_index())
        self.assertEqual(sorted(c.name for c in db), ["one", "two"])
        self.assertTrue(db.sync())
        self.assertEqual(fake.scan_calls, 0)


class SchemaMigrationWaitsForTheIndexTests(unittest.TestCase):
    """v1 -> v2 walks the records through the index: never stamp v2 over a partial walk."""

    def seed_v1(self, fake):
        legacy_record(fake, 1, "k1", "one")
        record = json.loads(fake.storage["client:client:1"])
        record["intent_blacklist"] = ["i:1"]
        fake.storage["client:client:1"] = json.dumps(record)

    def test_start_up_leaves_an_unindexed_namespace_at_v1(self):
        fake = NoScanRedis()
        self.seed_v1(fake)
        db = build_db(fake)

        db._maybe_migrate()

        self.assertNotIn("client:schema_version", fake.storage)
        self.assertIn("intent_blacklist", json.loads(fake.storage["client:client:1"]))
        # Reads still fold the legacy field.
        self.assertEqual(db.get_client_by_api_key("k1").name, "one")

    def test_a_direct_migrate_refuses_a_partial_index(self):
        fake = NoScanRedis()
        self.seed_v1(fake)
        with self.assertRaises(RuntimeError):
            build_db(fake).migrate(from_version=1)

    def test_once_indexed_the_namespace_migrates_and_is_stamped(self):
        fake = NoScanRedis()
        self.seed_v1(fake)
        fake.sadd("client:client_ids", 1)
        fake.set("client:client_ids_built", "operator")

        build_db(fake)._maybe_migrate()

        self.assertEqual(int(fake.storage["client:schema_version"]), 2)
        stored = json.loads(fake.storage["client:client:1"])
        self.assertNotIn("intent_blacklist", stored)
        self.assertEqual(stored["metadata"]["intent_blacklist"], ["i:1"])

    def test_the_cluster_migration_refuses_an_unindexed_source(self):
        from hivemind_redis_database.migration import migrate_namespace

        source = NoScanRedis()
        legacy_record(source, 1, "k1", "one")
        target = NoScanRedis()
        target_db = build_db(target, prefix="target")

        with self.assertRaises(RuntimeError):
            migrate_namespace(build_db(source), target_db)
        self.assertNotIn("target:client:1", target.storage)


@unittest.skipUnless(os.getenv("REDIS_ACL_TEST_ADMIN_URL"),
                     "set REDIS_ACL_TEST_ADMIN_URL to a throwaway Redis's admin URL")
class RealRedisWithoutScanTests(unittest.TestCase):
    """The plugin on a real Redis, under a login that holds no SCAN and no
    scripting: the command list an ACL-restricted hub gets, and nothing else.

    Point REDIS_ACL_TEST_ADMIN_URL at a THROWAWAY Redis: the test creates and
    deletes an ACL user and keys under a random prefix there.
    """

    COMMANDS = (
        "-@all +@connection +@transaction -@admin -@dangerous -client|tracking "
        "+get +set +setnx +setex +mget +incr +incrby +hget +hgetall +hset +hsetnx +hdel "
        "+hlen +hexists +sadd +srem +smembers +lpush +rpush +lpop +ltrim +lrange +del "
        "+exists +expire"
    ).split()

    def setUp(self):
        self.admin = redis.Redis.from_url(os.environ["REDIS_ACL_TEST_ADMIN_URL"],
                                          decode_responses=True)
        self.prefix = f"acl-test-{uuid4().hex[:8]}"
        self.user = f"{self.prefix}-user"
        self.password = uuid4().hex
        self.admin.execute_command(
            "ACL", "SETUSER", self.user, "reset", "on", f">{self.password}", "resetkeys",
            f"~{self.prefix}:*", "resetchannels", *self.COMMANDS)
        self.admin.execute_command("ACL", "LOG", "RESET")
        self.addCleanup(self.cleanup)

    def cleanup(self):
        self.admin.execute_command("ACL", "DELUSER", self.user)
        for key in list(self.admin.scan_iter(match=f"{self.prefix}:*", count=500)):
            self.admin.delete(key)
        self.admin.close()

    def hub_db(self):
        kwargs = self.admin.connection_pool.connection_kwargs
        return RedisDB(host=kwargs.get("host", "127.0.0.1"), port=kwargs.get("port", 6379),
                       username=self.user, password=self.password, index_prefix=self.prefix)

    def refused(self):
        return {(entry["reason"], entry["object"])
                for entry in self.admin.execute_command("ACL", "LOG")
                if entry.get("username") == self.user}

    def test_every_path_runs_without_scan_or_scripting(self):
        db = self.hub_db()
        self.assertTrue(db.add_item(Client(client_id=1, api_key="k1", name="kitchen")))
        self.assertTrue(db.add_item(Client(client_id=2, api_key="k2", name="office")))
        self.assertEqual(sorted(c.name for c in db), ["kitchen", "office"])
        self.assertEqual(db.get_client_by_api_key("k1").name, "kitchen")
        client = db.get_client_by_api_key("k2")
        client.name = "study"
        self.assertTrue(db.update_client(client))
        self.assertEqual([c.client_id for c in db.search_by_value("client_id", client.client_id)],
                         [client.client_id])
        self.assertTrue(db.remove_client(str(client.client_id)))
        self.assertTrue(db.sync())
        self.assertEqual([c.name for c in db], ["kitchen"])
        self.assertEqual(len(db), 2)

        # Only the start-up probes this plugin already tolerates are refused.
        objects = {obj for _, obj in self.refused()}
        self.assertLessEqual(objects, {"info", "module|list", "cluster|info", "FT._LIST"})

    def test_an_unindexed_namespace_needs_an_administrator_once(self):
        for client_id, name in ((1, "kitchen"), (2, "office")):
            self.admin.set(f"{self.prefix}:client:{client_id}",
                           Client(client_id=client_id, api_key=f"k{client_id}", name=name).serialize())
            self.admin.sadd(f"{self.prefix}:api_key:k{client_id}", client_id)
            self.admin.incr(f"{self.prefix}:count")
        self.admin.set(f"{self.prefix}:id_seq", 2)
        self.admin.set(f"{self.prefix}:schema_version", 2)

        db = self.hub_db()
        self.assertEqual(list(db), [])
        self.assertEqual(db.get_client_by_api_key("k2").name, "office")
        self.assertIn(("command", "scan"), self.refused())
        self.assertFalse(self.admin.exists(f"{self.prefix}:client_ids_built"))

        # The administrator's walk, as the operator runs it.
        for key in self.admin.scan_iter(match=f"{self.prefix}:client:*", count=500):
            self.admin.sadd(f"{self.prefix}:client_ids", key.rsplit(":", 1)[-1])
        self.admin.set(f"{self.prefix}:client_ids_built", "operator")
        self.admin.execute_command("ACL", "LOG", "RESET")

        db = self.hub_db()
        self.assertEqual(sorted(c.name for c in db), ["kitchen", "office"])
        self.assertTrue(db.sync())
        self.assertNotIn(("command", "scan"), self.refused())


if __name__ == "__main__":
    unittest.main()
