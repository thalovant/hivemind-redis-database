"""End-to-end: a real RedisDB backend (fakeredis-backed), driven through a
live hivemind-core policy chain in a hivescope topology, proving that a
blacklist folded into ``Client.metadata`` by the v1→v2 schema migration is
injected into the OVOS session by ``OVOSAgentPolicy``.

    legacy skill_blacklist key  --(plugin migrate)-->  Client.metadata
        --(OVOSAgentPolicy)-->  session.blacklisted_skills

Requires the policy stack + hivescope (with MasterNode db= injection) +
fakeredis; skipped when any is absent.
"""
import importlib.util
import inspect
import json
import time
from unittest import mock

import pytest

pytest.importorskip("hivemind_core.policy")
pytest.importorskip("hivemind_ovos_agent_plugin")
pytest.importorskip("hivescope")
fakeredis = pytest.importorskip("fakeredis")


def _hivescope_supports_db_injection() -> bool:
    from hivescope.node import MasterNode
    return "db" in inspect.signature(MasterNode.create).parameters


_HAS_POLICY = (
    importlib.util.find_spec("hivemind_core.policy") is not None
    and importlib.util.find_spec("hivemind_ovos_agent_plugin") is not None
)
pytestmark = [
    pytest.mark.skipif(not _HAS_POLICY,
                       reason="needs hivemind-core policy chain + OVOSAgentPolicy"),
    pytest.mark.skipif(not _hivescope_supports_db_injection(),
                       reason="needs hivescope MasterNode.create(db=...) (> 0.3.0a1)"),
]

from hivemind_bus_client.message import HiveMessage, HiveMessageType  # noqa: E402
from ovos_bus_client.message import Message  # noqa: E402
from hivemind_core.database import ClientDatabase  # noqa: E402
from hivescope.topology import TopologyBuilder  # noqa: E402
from hivescope.assertions import assert_session_blacklists_injected  # noqa: E402

from hivemind_redis_database import _iter_client_records_safely, CREATE_MARKER  # noqa: E402

_REDIS_PLUGIN = "hivemind-redis-db-plugin"
_CONN_KWARGS = (
    "host", "port", "username", "db", "max_connections", "retry_attempts",
    "retry_delay", "use_ssl", "ssl", "ssl_certfile", "ssl_keyfile",
    "ssl_ca_certs", "ssl_cert_reqs", "ssl_check_hostname",
)


class _HivescopeDBAdapter:
    """Drops the can_escalate/can_propagate/can_broadcast kwargs hivescope
    passes to add_client; the real ClientDatabase does not accept them."""

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


def _reset_resolution_cache(hm_protocol):
    for attr in ("clients", "connections", "_clients", "_connections"):
        reg = getattr(hm_protocol, attr, None)
        if isinstance(reg, dict):
            for conn in reg.values():
                if hasattr(conn, "reset_user"):
                    conn.reset_user()


def _seed_legacy_blacklist(db, api_key: str, skills):
    """Rewrite a connected client's record into the legacy v1 shape: the
    blacklist lives in a top-level ``skill_blacklist`` key and the schema
    sentinel is rolled back to v1, as an operator's DB looks pre-upgrade."""
    for key, raw in _iter_client_records_safely(db):
        if raw == CREATE_MARKER:
            continue
        try:
            rec = json.loads(raw)
        except (TypeError, ValueError):
            continue
        if isinstance(rec, dict) and rec.get("api_key") == api_key:
            rec["skill_blacklist"] = skills
            db.redis.set(key, json.dumps(rec))
    db.redis.set(db._schema_version_key(), 1)


def test_migrated_skill_blacklist_reaches_session():
    server = fakeredis.FakeServer()

    def _fake(*a, **k):
        for c in _CONN_KWARGS:
            k.pop(c, None)
        k.pop("connection_pool", None)
        return fakeredis.FakeStrictRedis(server=server, **k)

    patcher = mock.patch("hivemind_redis_database.redis.StrictRedis", _fake)
    patcher.start()
    cdb = ClientDatabase(config={
        "module": _REDIS_PLUGIN,
        _REDIS_PLUGIN: {"name": "clients"},
    })

    b = TopologyBuilder()
    m = b.add_master("M0", db=_HivescopeDBAdapter(cdb), require_crypto=False)
    s = b.add_satellite(
        "S0", upstream=m, is_admin=False,
        allowed_types=["recognizer_loop:utterance"],
    )
    b.start_all()
    try:
        key = s.identity.access_key

        _seed_legacy_blacklist(cdb.db, key, ["skill-weather"])
        cdb.db.migrate(from_version=1)
        _reset_resolution_cache(m.hm_protocol)

        seen = []
        m.agent_protocol.bus.on("recognizer_loop:utterance", seen.append)
        s.send(HiveMessage(
            HiveMessageType.BUS,
            payload=Message("recognizer_loop:utterance",
                            {"utterances": ["what is the weather"]}),
        ))

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and not seen:
            time.sleep(0.02)
        assert seen, "utterance never reached the agent bus"

        assert_session_blacklists_injected(
            m, s,
            msg_type="recognizer_loop:utterance",
            expected_skills=["skill-weather"],
        )
    finally:
        b.stop_all()
        patcher.stop()
