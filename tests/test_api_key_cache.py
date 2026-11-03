"""Opt-in in-process TTL cache in front of the Redis-side admission records.

Even a single-GET admission costs a client-observed round trip under storm
(~60ms measured at 400 concurrent clients against a shared Redis whose
server-side GET cost was 6.2us). Identity records change only through
explicit operator actions, each of which invalidates this cache in-process
AFTER its Redis write commits; other processes converge within the TTL (keep
it small -- seconds). A generation counter guards against an in-flight lookup
re-caching a value it read before a concurrent mutation committed.
"""
import json
import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


def _db(ttl=10.0):
    import threading
    from hivemind_redis_database import RedisDB
    with patch.object(RedisDB, "__post_init__", lambda self: None):
        kwargs = {} if ttl is None else {"api_key_cache_ttl": ttl}
        db = RedisDB(**kwargs)
    # replicate the cache state __post_init__ would create
    db._api_key_cache = {}
    db._api_key_cache_lock = threading.Lock()
    db._api_key_cache_gen = 0
    db.redis = MagicMock()
    db.is_cluster = False
    return db


def _store(db, key, value):
    db._api_key_cache_store(key, value, db._api_key_cache_gen_snapshot())


def _fake_client(client_id=1, api_key="KEY", name="n"):
    client = SimpleNamespace(client_id=client_id, api_key=api_key, name=name)
    client.serialize = lambda: json.dumps(
        {"client_id": client_id, "api_key": api_key, "name": name, "metadata": {}}
    )
    return client


def _mutation_harness(db):
    """Route _pipeline() to a recorder and log in-process invalidations."""
    events = []
    pipe = MagicMock()
    pipe.execute.side_effect = lambda: events.append("execute")
    db._pipeline = lambda: pipe
    real_invalidate = db._api_key_cache_invalidate
    db._api_key_cache_invalidate = lambda api_key=None: (
        events.append(("invalidate", api_key)),
        real_invalidate(api_key),
    )[1]
    db._ensure_client_attributes = lambda c: None
    return events


class TestApiKeyCache(unittest.TestCase):
    def test_disabled_by_default(self):
        """The dataclass default itself must be off -- constructed without an
        explicit ttl, nothing may be cached."""
        db = _db(ttl=None)
        self.assertEqual(db.api_key_cache_ttl, 0.0)
        _store(db, "k", "client")
        self.assertFalse(db._api_key_cache_get("k")[0])

    def test_disabled_with_explicit_zero(self):
        db = _db(ttl=0)
        _store(db, "k", "client")
        self.assertFalse(db._api_key_cache_get("k")[0])

    def test_hit_within_ttl(self):
        db = _db(ttl=10)
        _store(db, "k", "client-object")
        hit, client = db._api_key_cache_get("k")
        self.assertTrue(hit)
        self.assertEqual(client, "client-object")

    def test_expiry(self):
        db = _db(ttl=0.05)
        _store(db, "k", "c")
        time.sleep(0.06)
        self.assertFalse(db._api_key_cache_get("k")[0])

    def test_negative_result_cached(self):
        db = _db(ttl=10)
        _store(db, "nope", None)
        hit, client = db._api_key_cache_get("nope")
        self.assertTrue(hit)
        self.assertIsNone(client)

    def test_targeted_and_full_invalidation(self):
        db = _db(ttl=10)
        _store(db, "a", 1)
        _store(db, "b", 2)
        db._api_key_cache_invalidate("a")
        self.assertFalse(db._api_key_cache_get("a")[0])
        self.assertTrue(db._api_key_cache_get("b")[0])
        db._api_key_cache_invalidate()
        self.assertFalse(db._api_key_cache_get("b")[0])

    def test_bounded_at_exact_configured_size(self):
        db = _db(ttl=10)
        db.api_key_cache_size = 2
        for i in range(9):
            _store(db, f"k{i}", i)
            self.assertLessEqual(len(db._api_key_cache), 2)

    def test_overwrite_at_capacity_keeps_entries(self):
        db = _db(ttl=10)
        db.api_key_cache_size = 2
        _store(db, "a", 1)
        _store(db, "b", 2)
        _store(db, "a", 3)
        self.assertEqual(len(db._api_key_cache), 2)
        self.assertEqual(db._api_key_cache_get("a")[1], 3)

    def test_cache_parameters_validated(self):
        db = _db(ttl=10)
        db._normalize_parameters()
        db._validate_parameters()  # defaults pass
        db.api_key_cache_size = 0
        with self.assertRaises(ValueError):
            db._validate_parameters()
        db.api_key_cache_size = 2048
        for bad_ttl in (-1, float("nan"), float("inf"), True):
            db.api_key_cache_ttl = bad_ttl
            with self.assertRaises(ValueError):
                db._validate_parameters()
        db.api_key_cache_ttl = 10
        db.api_key_cache_size = True
        with self.assertRaises(ValueError):
            db._validate_parameters()

    def test_inflight_store_dropped_after_invalidation(self):
        """A lookup that snapshotted its generation before a mutation's
        invalidation must not be able to store its (stale) result."""
        db = _db(ttl=10)
        gen = db._api_key_cache_gen_snapshot()
        db._api_key_cache_invalidate("k")
        db._api_key_cache_store("k", "stale-pre-mutation-value", gen)
        self.assertFalse(db._api_key_cache_get("k")[0])

    def test_hot_path_serves_hit_without_redis(self):
        db = _db(ttl=10)
        _store(db, "key1", "cached-client")
        client = db.get_client_by_api_key("key1")
        self.assertEqual(client, "cached-client")
        # A hit must issue NO Redis operation at all (mock_calls covers
        # method calls; assert_not_called would only cover the mock itself).
        self.assertEqual(db.redis.mock_calls, [])


class TestMutationInvalidation(unittest.TestCase):
    """The Redis-side record rides each mutation's transaction via
    _invalidate_api_key_records(writer=p); the in-process layer cannot, so
    every pipeline mutator must invalidate it AFTER p.execute() commits."""

    def test_standard_add_evicts_negative_after_commit(self):
        db = _db(ttl=10)
        events = _mutation_harness(db)
        _store(db, "NEW", None)  # storm cached a denial before the key existed
        db._claim_item_key = lambda key: True
        db._get_next_client_id = lambda: 7
        self.assertTrue(db.add_item(_fake_client(client_id=None, api_key="NEW")))
        self.assertEqual(events, ["execute", ("invalidate", "NEW")])
        self.assertFalse(db._api_key_cache_get("NEW")[0])

    def test_revoke_invalidates_after_commit(self):
        db = _db(ttl=10)
        events = _mutation_harness(db)
        db.redis.get.return_value = json.dumps(
            {"name": "n", "api_key": "OLD", "metadata": {}}
        )
        self.assertTrue(db.remove_client("1"))
        self.assertEqual(events, ["execute", ("invalidate", "OLD")])

    def test_same_key_update_invalidates_after_commit(self):
        db = _db(ttl=10)
        events = _mutation_harness(db)
        db.redis.get.return_value = "old-serialized"
        db._deserialize_client = lambda raw: _fake_client(api_key="KEY")
        self.assertTrue(db.update_client(_fake_client(api_key="KEY")))
        self.assertEqual(
            events, ["execute", ("invalidate", "KEY"), ("invalidate", "KEY")]
        )

    def test_key_change_update_invalidates_both_after_commit(self):
        db = _db(ttl=10)
        events = _mutation_harness(db)
        db.redis.get.return_value = "old-serialized"
        db._deserialize_client = lambda raw: _fake_client(api_key="OLD")
        self.assertTrue(db.update_client(_fake_client(api_key="NEW")))
        self.assertEqual(
            events, ["execute", ("invalidate", "OLD"), ("invalidate", "NEW")]
        )

    def test_sync_clears_cache_after_commit(self):
        db = _db(ttl=10)
        events = _mutation_harness(db)
        db.redisearch_available = False
        db.redis.scan_iter.return_value = []
        self.assertTrue(db.sync())
        self.assertEqual(events, ["execute", ("invalidate", None)])

    def test_immediate_mode_invalidates_in_process(self):
        """writer=None commits per-delete, so the helper itself drops the
        in-process entries afterwards."""
        db = _db(ttl=10)
        _store(db, "K", "cached")
        db._invalidate_api_key_records("K")
        db.redis.delete.assert_called_once()
        self.assertFalse(db._api_key_cache_get("K")[0])


if __name__ == "__main__":
    unittest.main()
