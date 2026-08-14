"""Opt-in TTL cache for the API-key admission lookup.

Under a 400-client reconnect storm the single materialized-hash HGET measured
~60ms against a busy shared Redis. Identity records change only through
explicit operator actions, each of which invalidates this cache in-process;
other processes converge within the TTL (keep it small -- seconds).
"""
import time
import unittest
from unittest.mock import MagicMock, patch


def _db(ttl=10.0):
    import threading
    from hivemind_redis_database import RedisDB
    with patch.object(RedisDB, "__post_init__", lambda self: None):
        db = RedisDB(api_key_cache_ttl=ttl)
    # replicate the cache state __post_init__ would create
    db._api_key_cache = {}
    db._api_key_cache_lock = threading.Lock()
    db.redis = MagicMock()
    return db


class TestApiKeyCache(unittest.TestCase):
    def test_disabled_by_default_ttl_zero(self):
        db = _db(ttl=0)
        db._api_key_cache_store("k", "client")
        hit, _ = db._api_key_cache_get("k")
        self.assertFalse(hit)

    def test_hit_within_ttl(self):
        db = _db(ttl=10)
        db._api_key_cache_store("k", "client-object")
        hit, client = db._api_key_cache_get("k")
        self.assertTrue(hit)
        self.assertEqual(client, "client-object")

    def test_expiry(self):
        db = _db(ttl=0.05)
        db._api_key_cache_store("k", "c")
        time.sleep(0.06)
        hit, _ = db._api_key_cache_get("k")
        self.assertFalse(hit)

    def test_negative_result_cached(self):
        """Unknown/revoked keys hammered by a storm must also be absorbed --
        caching None is deny-side, so it is safe."""
        db = _db(ttl=10)
        db._api_key_cache_store("nope", None)
        hit, client = db._api_key_cache_get("nope")
        self.assertTrue(hit)
        self.assertIsNone(client)

    def test_targeted_invalidation(self):
        db = _db(ttl=10)
        db._api_key_cache_store("a", 1)
        db._api_key_cache_store("b", 2)
        db._api_key_cache_invalidate("a")
        self.assertFalse(db._api_key_cache_get("a")[0])
        self.assertTrue(db._api_key_cache_get("b")[0])

    def test_full_invalidation(self):
        db = _db(ttl=10)
        db._api_key_cache_store("a", 1)
        db._api_key_cache_store("b", 2)
        db._api_key_cache_invalidate()
        self.assertFalse(db._api_key_cache_get("a")[0])
        self.assertFalse(db._api_key_cache_get("b")[0])

    def test_bounded(self):
        db = _db(ttl=10)
        db.api_key_cache_size = 16
        for i in range(40):
            db._api_key_cache_store(f"k{i}", i)
        self.assertLessEqual(len(db._api_key_cache), 17)

    def test_hot_path_uses_cache(self):
        db = _db(ttl=10)
        db._api_key_cache_store("key1", "cached-client")
        client, timings = db.get_client_by_api_key_with_metrics("key1")
        self.assertEqual(client, "cached-client")
        self.assertEqual(timings.get("cache_hit"), 1.0)
        db.redis.hget.assert_not_called()


if __name__ == "__main__":
    unittest.main()
