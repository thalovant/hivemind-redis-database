"""End-to-end: a real RedisDB backend (fakeredis-backed) used as the
client-credential database of a live hivescope MasterNode.

Exercises the full client-database seam through a real HiveMind topology:

    satellite admission --> RedisDB.add_client
    handshake / message routing --> RedisDB api_key lookup
    master restart (new connection to the same Redis server)
        --> credentials persist and still admit the satellite

No live Redis server is required: ``fakeredis.FakeServer`` provides a
shared in-process store, so persistence across "reconnects" means a brand
new RedisDB connection against the same server data.
"""
import time
from unittest import mock

import fakeredis
import pytest

from hivemind_bus_client.message import HiveMessage, HiveMessageType
from ovos_bus_client.message import Message
from hivemind_core.database import ClientDatabase
from hivescope.topology import TopologyBuilder
from hivescope.assertions import assert_handshake_complete, assert_bus_message_routed

from hivemind_redis_database import RedisDB

_REDIS_PLUGIN = "hivemind-redis-db-plugin"
_CONN_KWARGS = (
    "host", "port", "username", "db", "max_connections", "retry_attempts",
    "retry_delay", "use_ssl", "ssl", "ssl_certfile", "ssl_keyfile",
    "ssl_ca_certs", "ssl_cert_reqs", "ssl_check_hostname",
)


class _HivescopeDBAdapter:
    """Bridges hivescope's ``register_satellite`` (which passes
    ``can_escalate``/``can_propagate``/``can_broadcast`` to ``add_client``)
    to the real ``ClientDatabase`` whose ``add_client`` does not take them.
    Everything else delegates straight to the backing ClientDatabase, so the
    master resolves real clients from the real Redis backend.
    """

    def __init__(self, cdb):
        object.__setattr__(self, "_cdb", cdb)

    def add_client(self, name, key, password=None, admin=False,
                   crypto_key=None, allowed_types=None, metadata=None,
                   intent_blacklist=None, skill_blacklist=None,
                   message_blacklist=None, can_escalate=True,
                   can_propagate=True, can_broadcast=True):
        return self._cdb.add_client(
            name=name, key=key, admin=admin, allowed_types=allowed_types,
            crypto_key=crypto_key, password=password, metadata=metadata,
            intent_blacklist=intent_blacklist, skill_blacklist=skill_blacklist,
            message_blacklist=message_blacklist,
        )

    def __getattr__(self, item):
        return getattr(object.__getattribute__(self, "_cdb"), item)

    def __iter__(self):
        return iter(self._cdb)

    def __len__(self):
        return len(self._cdb)

    def __enter__(self):
        return self._cdb.__enter__()

    def __exit__(self, *a):
        return self._cdb.__exit__(*a)


@pytest.fixture()
def redis_server():
    """One in-process Redis data store shared by every connection the test
    opens, so a second RedisDB instance sees the first one's writes exactly
    like a reconnect to a live server would."""
    server = fakeredis.FakeServer()

    def _fake(*args, **kwargs):
        for c in _CONN_KWARGS:
            kwargs.pop(c, None)
        kwargs.pop("connection_pool", None)
        return fakeredis.FakeStrictRedis(server=server, **kwargs)

    with mock.patch("hivemind_redis_database.redis.StrictRedis", _fake):
        yield server


def _open_client_db() -> ClientDatabase:
    cdb = ClientDatabase(config={
        "module": _REDIS_PLUGIN,
        _REDIS_PLUGIN: {"name": "clients"},
    })
    # Pin this repo's RedisDB explicitly so an editable install always
    # exercises the local code even if another distribution claims the
    # same entry-point name.
    cdb.db = RedisDB(name="clients")
    return cdb


def test_client_db_backend_registration_lookup_and_persistence(redis_server):
    cdb = _open_client_db()

    b = TopologyBuilder()
    m = b.add_master("M0", db=_HivescopeDBAdapter(cdb), require_crypto=False)
    s = b.add_satellite(
        "S0", upstream=m, is_admin=False,
        allowed_types=["recognizer_loop:utterance"],
    )
    b.start_all()
    try:
        assert_handshake_complete(m, s)
        key = s.identity.access_key

        # --- registration: the admission wrote a real record into Redis ---
        client = cdb.get_client_by_api_key(key)
        assert client is not None, "admitted satellite not found in RedisDB"
        assert client.api_key == key
        assert "recognizer_loop:utterance" in (client.allowed_types or [])

        # --- lookup: the connected client is resolved from the backend ---
        seen = []
        m.agent_protocol.bus.on("recognizer_loop:utterance", seen.append)
        s.send(HiveMessage(
            HiveMessageType.BUS,
            payload=Message("recognizer_loop:utterance",
                            {"utterances": ["hello hive"]}),
        ))
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and not seen:
            time.sleep(0.02)
        assert seen, "utterance never reached the agent bus"
        assert_bus_message_routed(m, count=1)
    finally:
        b.stop_all()

    # --- persistence: a fresh connection to the same Redis server (the
    # master "reconnecting" after a restart) still resolves the client ---
    cdb2 = _open_client_db()
    persisted = cdb2.get_client_by_api_key(key)
    assert persisted is not None, "client credentials lost across reconnect"
    assert persisted.api_key == key
    assert persisted.name == client.name
    assert persisted.client_id == client.client_id
    assert "recognizer_loop:utterance" in (persisted.allowed_types or [])

    # And a restarted master wired to the reconnected backend admits a
    # satellite presenting the persisted credentials without re-registering.
    b2 = TopologyBuilder()
    m2 = b2.add_master("M1", db=_HivescopeDBAdapter(cdb2), require_crypto=False)
    s2 = b2.add_satellite("S1", upstream=m2, is_admin=False)
    b2.start_all()
    try:
        assert_handshake_complete(m2, s2)
        # both the pre-restart and post-restart clients live in the same DB
        assert cdb2.get_client_by_api_key(key) is not None
        assert cdb2.get_client_by_api_key(s2.identity.access_key) is not None
        assert cdb2.total_clients() >= 2
    finally:
        b2.stop_all()
