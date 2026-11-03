"""The admission lookup must not pay for logging a node will not emit.

``LOG.debug`` resolves the calling module, function and line with
``inspect.stack()`` before it consults the level, so a discarded record costs
as much as an emitted one -- and the walk gets more expensive the deeper the
stack. hivemind-core calls this path on every connection admission.
"""
import logging
import unittest
from unittest.mock import patch

import hivemind_redis_database as hrd
from hivemind_redis_database import RedisDB

from test_redisdb import FakeRedis, make_client


class TestLookupLogging(unittest.TestCase):
    def setUp(self):
        hrd._LOOKUP_LOGGER = None
        hrd._LOOKUP_LOGGER_KEY = None
        self.redis = FakeRedis()
        db = object.__new__(RedisDB)
        db.redis = self.redis
        db.redis_pool = self.redis.connection_pool
        db.index_prefix = "client"
        db.redisearch_available = False
        db.is_cluster = False
        db.cluster_hash_tag = None
        self.db = db
        self.db.add_item(make_client(client_id=1, name="sat", api_key="key-1"))

    def tearDown(self):
        name = f"{hrd.LOG.name} - {hrd.__name__}"
        stale = logging.getLogger(name)
        for handler in list(stale.handlers):
            stale.removeHandler(handler)
            handler.close()
        hrd.LOG._loggers.pop(name, None)
        hrd._LOOKUP_LOGGER = None
        hrd._LOOKUP_LOGGER_KEY = None

    def test_lookup_does_not_use_the_stack_walking_logger(self):
        """LOG.debug is the expensive one; the lookup must not call it."""
        with patch.object(hrd.LOG, "debug") as expensive:
            self.db.search_by_value("api_key", "key-1")

        self.assertEqual(
            expensive.call_count, 0,
            "the admission lookup still logs through LOG.debug, which walks "
            "inspect.stack() on every call")

    def test_lookup_logger_is_resolved_once(self):
        self.assertIs(hrd._lookup_logger(), hrd._lookup_logger())

    def test_lookup_logger_follows_log_set_level(self):
        """Caching must not pin the level."""
        log = hrd._lookup_logger()
        self.assertIn(log.name, hrd.LOG._loggers)
        previous = hrd.LOG.level
        try:
            hrd.LOG.set_level("DEBUG")
            self.assertEqual(log.level, logging.DEBUG)
            hrd.LOG.set_level("WARNING")
            self.assertEqual(log.level, logging.WARNING)
        finally:
            hrd.LOG.set_level(previous)

    def test_debug_output_is_still_produced_with_level_restored(self):
        log = hrd._lookup_logger()
        previous = log.level
        try:
            log.setLevel(logging.DEBUG)
            with self.assertLogs(log, level="DEBUG") as captured:
                self.db.search_by_value("api_key", "key-1")
        finally:
            log.setLevel(previous)

        joined = "\n".join(captured.output)
        self.assertIn("api_key", joined)
        self.assertIn("key-1", joined)


if __name__ == "__main__":
    unittest.main()


class TestLookupLoggerLifecycle(unittest.TestCase):
    def tearDown(self):
        name = f"{hrd.LOG.name} - {hrd.__name__}"
        stale = logging.getLogger(name)
        for handler in list(stale.handlers):
            stale.removeHandler(handler)
            handler.close()
        hrd.LOG._loggers.pop(name, None)
        hrd._LOOKUP_LOGGER = None
        hrd._LOOKUP_LOGGER_KEY = None

    def test_logger_rewires_after_log_init_changes_base_path(self):
        """init after first use must not strand this path on stdout-only."""
        import tempfile
        hrd._lookup_logger()
        previous = hrd.LOG.base_path
        try:
            with tempfile.TemporaryDirectory() as base:
                hrd.LOG.base_path = base
                kinds = {type(h).__name__
                         for h in hrd._lookup_logger().handlers}
                self.assertIn("RotatingFileHandler", kinds)
        finally:
            hrd.LOG.base_path = previous

    def test_concurrent_first_use_attaches_handlers_once(self):
        import threading
        gate = threading.Event()

        def resolve():
            gate.wait()
            hrd._lookup_logger()

        threads = [threading.Thread(target=resolve) for _ in range(8)]
        for thread in threads:
            thread.start()
        gate.set()
        for thread in threads:
            thread.join(timeout=10)

        handlers = hrd._lookup_logger().handlers
        streams = [h for h in handlers
                   if type(h).__name__ == "StreamHandler"]
        self.assertLessEqual(len(streams), 1)
