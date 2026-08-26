"""Connection tuning for the bursty hub admission workload.

A non-zero ``health_check_interval`` makes redis-py issue a blocking PING
before the first command on any connection idle longer than the interval --
one extra serial round-trip at the front of every admission burst. Liveness
is covered reactively by the retry policy and proactively by kernel TCP
keepalive instead.
"""
import unittest
from unittest.mock import MagicMock, patch


def _db():
    from hivemind_redis_database import RedisDB
    with patch.object(RedisDB, "__post_init__", lambda self: None):
        db = RedisDB()
    db._normalize_parameters()
    return db


class TestConnectionTuning(unittest.TestCase):
    def test_single_connection_no_idle_health_check(self):
        db = _db()
        with patch("hivemind_redis_database.redis.StrictRedis") as strict:
            db._create_single_connection()
        kwargs = strict.call_args.kwargs
        self.assertEqual(kwargs["health_check_interval"], 0)

    def test_single_connection_keepalive(self):
        db = _db()
        with patch("hivemind_redis_database.redis.StrictRedis") as strict:
            db._create_single_connection()
        kwargs = strict.call_args.kwargs
        self.assertTrue(kwargs["socket_keepalive"])
        self.assertIsInstance(kwargs["socket_keepalive_options"], dict)

    def test_cluster_connection_keepalive(self):
        db = _db()
        db.cluster_nodes = [{"host": "h", "port": 7000}]
        db._get_startup_nodes = MagicMock(return_value=[])
        with patch("hivemind_redis_database.redis.RedisCluster") as cluster:
            db._create_cluster_connection()
        kwargs = cluster.call_args.kwargs
        self.assertTrue(kwargs["socket_keepalive"])
        self.assertIsInstance(kwargs["socket_keepalive_options"], dict)

    def test_deserialize_str_roundtrip(self):
        """_json_loads (orjson when available, stdlib otherwise) must parse
        the stored record format identically."""
        db = _db()
        client = db._deserialize_client(
            '{"client_id": 3, "api_key": "k", "name": "n", '
            '"metadata": {"owner_id": "o", "unicode": "caf\\u00e9"}}'
        )
        self.assertEqual(client.client_id, 3)
        self.assertEqual(client.api_key, "k")
        self.assertEqual(client.metadata["unicode"], "café")

    def test_deserialize_nonfinite_floats_stay_readable(self):
        """stdlib json emits NaN/Infinity by default and orjson strictly
        rejects them -- records written by stdlib must stay readable
        whichever backend is active."""
        db = _db()
        client = db._deserialize_client(
            '{"client_id": 3, "api_key": "k", "name": "n", '
            '"metadata": {"score": NaN, "bound": Infinity}}'
        )
        self.assertEqual(client.client_id, 3)
        self.assertNotEqual(client.metadata["score"], client.metadata["score"])

    def test_deserialize_coerces_bad_metadata(self):
        db = _db()
        client = db._deserialize_client(
            '{"client_id": 3, "api_key": "k", "name": "n", "metadata": "bogus"}'
        )
        self.assertEqual(client.metadata, {})


if __name__ == "__main__":
    unittest.main()
