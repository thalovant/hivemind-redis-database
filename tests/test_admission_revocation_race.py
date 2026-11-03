"""The admission-record refill is compare-and-set against invalidation.

A lookup that misses the admission record reads the authoritative record and
then writes the cache. A revocation or update that commits between those two
steps must not be undone by that write. These tests put the mutation exactly
there -- inside the authoritative read, after it has returned its answer --
against the suite's FakeRedis, whose pipelines honour WATCH as Redis does
(EXEC raises WatchError when a watched key was written since WATCH).
"""
import unittest
from unittest.mock import patch

from redis.cluster import key_slot

from hivemind_plugin_manager.database import Client
from hivemind_redis_database import RedisDB
from test_redisdb import FakePipeline, FakeRedis


def _db(redis_client: FakeRedis, *, hash_tag=None) -> RedisDB:
    """A RedisDB on ``redis_client`` without connecting anywhere."""
    db = object.__new__(RedisDB)
    db.redis = redis_client
    db.redis_pool = redis_client.connection_pool
    db.index_prefix = "client"
    db.redisearch_available = False
    db.is_cluster = False
    db.cluster_hash_tag = hash_tag
    return db


def _interleave(db: RedisDB, mutation) -> None:
    """Run ``mutation`` after the authoritative read and before the refill."""
    original = db._authoritative_client_by_api_key

    def read_then_mutate(api_key: str):
        answer = original(api_key)
        mutation()
        return answer

    db._authoritative_client_by_api_key = read_then_mutate


class AdmissionRefillIsCompareAndSet(unittest.TestCase):
    def setUp(self) -> None:
        self.redis = FakeRedis()
        self.db = _db(self.redis)
        self.client = Client(client_id=1, name="satellite", api_key="key-1", is_admin=True)
        self.assertTrue(self.db.add_item(self.client))
        self.record = self.db._api_key_record_key("key-1")

    def _plain_lookups(self) -> None:
        """Undo the interleaving hook."""
        self.db.__dict__.pop("_authoritative_client_by_api_key", None)

    def test_a_revocation_during_the_lookup_is_not_undone(self) -> None:
        _interleave(self.db, lambda: self.db.remove_client("1"))

        self.db.get_client_by_api_key("key-1")

        self.assertIsNone(self.redis.get(self.record),
                          "the refill wrote back a record the revocation had dropped")
        self._plain_lookups()
        self.assertIsNone(self.db.get_client_by_api_key("key-1"),
                          "a revoked key was admitted from the admission record")

    def test_an_update_during_the_lookup_is_not_undone(self) -> None:
        demoted = Client(client_id=1, name="satellite", api_key="key-1", is_admin=False)
        _interleave(self.db, lambda: self.db.update_client(demoted))

        self.db.get_client_by_api_key("key-1")

        self._plain_lookups()
        found = self.db.get_client_by_api_key("key-1")
        self.assertIsNotNone(found)
        self.assertFalse(found.is_admin, "a demoted client was admitted with its old rights")

    def test_an_undisturbed_lookup_still_fills_the_record(self) -> None:
        found = self.db.get_client_by_api_key("key-1")

        self.assertEqual(found.client_id, 1)
        self.assertIsNotNone(self.redis.get(self.record))
        self.assertEqual(self.redis.expiries[self.record], self.db.admission_record_ttl)
        # And the next lookup is served from it.
        with patch.object(self.db, "_authoritative_client_by_api_key",
                          side_effect=AssertionError("authoritative read on a warm record")):
            self.assertEqual(self.db.get_client_by_api_key("key-1").client_id, 1)

    def test_no_guard_means_no_write(self) -> None:
        def refuse_watch(*_args, **_kwargs):
            raise RuntimeError("WATCH unavailable")

        with patch.object(FakePipeline, "watch", refuse_watch):
            found = self.db.get_client_by_api_key("key-1")

        self.assertEqual(found.client_id, 1, "a guard failure must not refuse a valid client")
        self.assertIsNone(self.redis.get(self.record), "an unguarded refill was written")

    def test_invalidation_moves_the_epoch(self) -> None:
        epoch = self.db._api_key_epoch_key("key-1")
        before = self.redis.get(epoch)

        self.assertTrue(self.db.remove_client("1"))

        self.assertNotEqual(self.redis.get(epoch), before)
        self.assertEqual(self.redis.expiries[epoch],
                         max(self.db.admission_record_ttl, RedisDB.ADMISSION_EPOCH_TTL),
                         "epoch keys must expire, and outlive any lookup that may still write")

    def test_the_guard_is_taken_only_after_the_authoritative_read(self) -> None:
        """No pooled connection may be held across the read, which needs its own."""
        events = []
        real_watch = FakePipeline.watch

        def watch(pipeline, *keys):
            events.append("watch")
            return real_watch(pipeline, *keys)

        _interleave(self.db, lambda: events.append("authoritative read"))
        with patch.object(FakePipeline, "watch", watch):
            self.db.get_client_by_api_key("key-1")

        self.assertEqual(events, ["authoritative read", "watch"])
        self.assertIsNotNone(self.redis.get(self.record))

    def test_a_revocation_between_the_guard_and_the_write_is_caught(self) -> None:
        """The epoch check passed; WATCH must still refuse a write after it."""
        real_multi = FakePipeline.multi
        revoked = []

        def multi(pipeline):
            if not revoked:
                revoked.append(self.db.remove_client("1"))
            return real_multi(pipeline)

        with patch.object(FakePipeline, "multi", multi):
            self.db.get_client_by_api_key("key-1")

        self.assertEqual(revoked, [True])
        self.assertIsNone(self.redis.get(self.record))
        self.assertIsNone(self.db.get_client_by_api_key("key-1"))

    def test_a_lookup_slower_than_half_an_epoch_writes_nothing(self) -> None:
        """Its epoch could have expired back to the value read at the start."""
        clock = iter([0.0, RedisDB.ADMISSION_EPOCH_TTL / 2 + 1])
        with patch("hivemind_redis_database.time.monotonic", lambda: next(clock)):
            found = self.db.get_client_by_api_key("key-1")

        self.assertEqual(found.client_id, 1)
        self.assertIsNone(self.redis.get(self.record))


class EpochSharesTheRecordsSlot(unittest.TestCase):
    def test_same_cluster_slot_under_a_hash_tag(self) -> None:
        db = _db(FakeRedis(), hash_tag="fleet")
        self.assertEqual(key_slot(db._api_key_epoch_key("k").encode()),
                         key_slot(db._api_key_record_key("k").encode()))


if __name__ == "__main__":
    unittest.main()
