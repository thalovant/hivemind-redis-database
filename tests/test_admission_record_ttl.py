"""admission_record_ttl: configurable lifetime of the Redis-side per-key
admission records. The expiry is the backstop against a LOST invalidation
(every write path still invalidates transactionally); deployments whose
clients reconnect after long idle gaps can raise it to avoid re-running the
full authoritative resolution per client on the first storm after idle."""
import unittest
from unittest.mock import MagicMock, patch


def _db(**kwargs):
    from hivemind_redis_database import RedisDB
    with patch.object(RedisDB, "__post_init__", lambda self: None):
        db = RedisDB(**kwargs)
    db._normalize_parameters()
    db.redis = MagicMock()
    db.is_cluster = False
    return db


class TestAdmissionRecordTTL(unittest.TestCase):
    def test_default_keeps_historical_value(self):
        db = _db()
        self.assertEqual(db.admission_record_ttl, db.ADMISSION_CACHE_TTL)
        self.assertEqual(db.admission_record_ttl, 60)

    def test_refill_uses_configured_ttl(self):
        db = _db(admission_record_ttl=3600)
        db.redis.get.return_value = None
        client = MagicMock()
        client.api_key = "k1"
        client.serialize.return_value = "{}"
        db._authoritative_client_by_api_key = lambda api_key: client
        db.get_client_by_api_key("k1")
        _, kwargs = db.redis.set.call_args
        self.assertEqual(kwargs.get("ex"), 3600)

    def test_validated_as_positive_integer(self):
        db = _db()
        db._validate_parameters()  # default passes
        for bad in (0, -5, 1.5, "60", True):
            db.admission_record_ttl = bad
            with self.assertRaises(ValueError):
                db._validate_parameters()


if __name__ == "__main__":
    unittest.main()
