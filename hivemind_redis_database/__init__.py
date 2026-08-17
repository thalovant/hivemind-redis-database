from dataclasses import dataclass
from typing import List, Optional, Iterable, Union
import json
import threading
import time

import redis
from redis.cluster import ClusterNode
from ovos_utils.log import LOG

from hivemind_plugin_manager.database import (Client, AbstractDB,
                                                AbstractRemoteDB, cast2client)


CREATE_MARKER = "__hivemind_creating__"
CREATE_MARKER_TTL = 30

_ADVANCE_LAST_SEEN_LUA = """
local raw = redis.call('GET', KEYS[1])
if not raw or raw == ARGV[3] then
    return 0
end
local client = cjson.decode(raw)
if client.api_key ~= ARGV[1] then
    return 0
end
local current = tonumber(client.last_seen) or -1
local incoming = tonumber(ARGV[2])
if incoming > current then
    client.last_seen = incoming
    redis.call('SET', KEYS[1], cjson.encode(client))
end
return 1
"""


def _iter_client_records_safely(db) -> "Iterable[tuple[str, str]]":
    """Yield (key, raw_value) pairs for every stored client record in the
    active namespace. Used by ``migrate()`` to rewrite records in place.
    Skips empties; callers must handle ``CREATE_MARKER`` themselves.
    """
    for key in db.redis.scan_iter(db._scan_pattern("client"), count=100):
        raw = db.redis.get(key)
        if not raw:
            continue
        yield key, raw


@dataclass
class RedisDB(AbstractRemoteDB):
    """
    Redis database implementation for HiveMind with advanced features.

    This class provides a high-performance Redis-based database solution for HiveMind,
    supporting both single Redis instances and Redis Cluster configurations. It includes
    automatic RediSearch integration, connection pooling, and comprehensive error handling.

    Features:
        - Automatic Redis vs Redis Cluster detection and configuration
        - RediSearch integration with fallback to basic indexing
        - Connection pooling with health checks and retry logic
        - Sequential client ID generation
        - Multi-hub support via configurable key prefixes
        - Production-ready error handling and logging

    Attributes:
        host (str): Redis server hostname (default: "127.0.0.1")
        port (int): Redis server port (default: 6379)
        name (str): Database name identifier (default: "clients")
        password (Optional[str]): Redis authentication password
        username (Optional[str]): Redis authentication username (default: "default")
        db (Optional[int]): Redis database number for single instance
        cluster_nodes (Optional[List[dict]]): Redis Cluster node configuration
        cluster_hash_tag (Optional[str]): Fixed Redis Cluster hash tag for single-slot writes
        index_prefix (str): Key prefix for all database operations (default: "client")
        max_connections (int): Maximum connection pool size (default: 64)
        retry_attempts (int): Number of retry attempts (default: 3)
        retry_delay (float): Delay between retry attempts in seconds (default: 0.1)
        use_ssl (bool): Enable SSL/TLS connection (default: False)
        ssl_certfile (Optional[str]): Path to SSL certificate file
        ssl_keyfile (Optional[str]): Path to SSL private key file
        ssl_ca_certs (Optional[str]): Path to CA certificates file
        ssl_cert_reqs (str): SSL certificate requirements ("required", "optional", "none") (default: "required")
        ssl_check_hostname (bool): Verify SSL hostname (default: True)
    """
    host: str = "127.0.0.1"
    port: int = 6379
    name: str = "clients"
    password: Optional[str] = None
    username: Optional[str] = "default"
    db: Optional[int] = 0
    cluster_nodes: Optional[List[dict]] = None
    cluster_hash_tag: Optional[str] = None
    index_prefix: str = "client"
    max_connections: int = 64
    retry_attempts: int = 3
    retry_delay: float = 0.1
    use_ssl: bool = False
    ssl: Optional[bool] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    ssl_ca_certs: Optional[str] = None
    ssl_cert_reqs: str = "required"
    ssl_check_hostname: bool = True
    # Opt-in in-process TTL cache for the API-key admission lookup. Under a
    # 400-client reconnect storm the single materialized-hash HGET measured
    # ~60ms against a busy shared Redis; identity records change only through
    # explicit operator actions. SECURITY TRADE-OFF: a revoke/update becomes
    # effective on OTHER hub processes only after the TTL expires (this
    # process invalidates immediately) -- keep the TTL small (seconds).
    # 0 disables the cache (default).
    api_key_cache_ttl: float = 0.0
    api_key_cache_size: int = 2048


    def __post_init__(self):
        """
        Initialize the RedisDB connection with automatic cluster/single instance detection.
        """
        self._normalize_parameters()
        self._validate_parameters()
        self._api_key_cache: dict = {}
        self._api_key_cache_lock = threading.Lock()
        # Bumped by every invalidation. A lookup snapshots the generation
        # BEFORE reading Redis and the store is dropped if it changed, so an
        # in-flight read that overlapped a mutation can never re-cache the
        # pre-mutation value.
        self._api_key_cache_gen = 0
        LOG.info("Redis database initialized with hiredis for optimal performance")

        self.is_cluster = self._detect_cluster()
        if self.is_cluster:
            LOG.info("Redis Cluster detected, using cluster client")
            self.redis = self._create_cluster_connection()
            if self.cluster_hash_tag:
                LOG.info(
                    "Redis Cluster hash-tag mode enabled; multi-key writes use "
                    "single-slot transactions for this namespace"
                )
            else:
                LOG.warning(
                    "Redis Cluster legacy mode enabled; primary records remain "
                    "authoritative, indexed search acceleration is disabled, and "
                    "name/api_key lookups fall back to scans. Enable "
                    "cluster_hash_tag for transactional indexed cluster mode."
                )
        else:
            LOG.info("Single Redis instance detected, using standard client")
            self.redis = self._create_single_connection()

        counter_key = self._counter_key()
        if self.redis.setnx(counter_key, 0):
            LOG.debug(f"Initialized client counter to 0 for {self.index_prefix}")

        self.redisearch_available = self._check_redisearch_availability()
        if self.redisearch_available:
            self._setup_redisearch_index()
            LOG.info("RediSearch module detected, enabling advanced search features")
        else:
            LOG.info("RediSearch module not available, using basic indexing")

        if not self.health_check():
            LOG.error("Redis connection health check failed during initialization")
            raise redis.ConnectionError("Failed to establish healthy Redis connection")

        self._maybe_migrate()

    def _normalize_parameters(self):
        """Normalize optional parameters and backward-compatible aliases."""
        if self.host is None:
            self.host = "127.0.0.1"
        if self.port is None:
            self.port = 6379
        if self.db is None:
            self.db = 0
        if self.username is None:
            self.username = "default"
        if self.name is None:
            self.name = "clients"
        if self.index_prefix is None:
            self.index_prefix = "client"
        if isinstance(self.cluster_hash_tag, str):
            self.cluster_hash_tag = self.cluster_hash_tag.strip()
        if self.cluster_hash_tag == "":
            self.cluster_hash_tag = None
        if self.ssl is not None:
            self.use_ssl = bool(self.ssl)

    def _validate_parameters(self):
        """
        Validate input parameters to ensure they are within acceptable ranges.
        """
        if not isinstance(self.port, int) or not (1 <= self.port <= 65535):
            raise ValueError(f"Port must be an integer between 1 and 65535, got {self.port}")

        if not isinstance(self.db, int) or self.db < 0:
            raise ValueError(f"Database ID must be a non-negative integer, got {self.db}")

        if not isinstance(self.max_connections, int) or self.max_connections < 1:
            raise ValueError(f"max_connections must be a positive integer, got {self.max_connections}")

        if not isinstance(self.retry_attempts, int) or self.retry_attempts < 0:
            raise ValueError(f"retry_attempts must be a non-negative integer, got {self.retry_attempts}")

        if not isinstance(self.retry_delay, (int, float)) or self.retry_delay < 0:
            raise ValueError(f"retry_delay must be a non-negative number, got {self.retry_delay}")

        if self.username and not isinstance(self.username, str):
            raise ValueError(f"Username must be a string, got {type(self.username)}")

        if self.password and not isinstance(self.password, str):
            raise ValueError(f"Password must be a string, got {type(self.password)}")

        if self.cluster_hash_tag is not None:
            if not isinstance(self.cluster_hash_tag, str) or not self.cluster_hash_tag.strip():
                raise ValueError("cluster_hash_tag must be a non-empty string when provided")
            if "{" in self.cluster_hash_tag or "}" in self.cluster_hash_tag:
                raise ValueError("cluster_hash_tag cannot contain '{' or '}'")

        if not isinstance(self.api_key_cache_ttl, (int, float)) or self.api_key_cache_ttl < 0:
            raise ValueError(
                f"api_key_cache_ttl must be a non-negative number, got {self.api_key_cache_ttl}"
            )

        if not isinstance(self.api_key_cache_size, int) or self.api_key_cache_size < 1:
            raise ValueError(
                f"api_key_cache_size must be a positive integer, got {self.api_key_cache_size}"
            )

        if self.ssl_cert_reqs not in ["required", "optional", "none"]:
            raise ValueError(f"ssl_cert_reqs must be 'required', 'optional', or 'none', got {self.ssl_cert_reqs}")

    def _base_prefix(self) -> str:
        """Return the active Redis namespace prefix."""
        if self.cluster_hash_tag:
            return f"{self.index_prefix}:{{{self.cluster_hash_tag}}}"
        return self.index_prefix

    def _key(self, *parts: Union[str, int]) -> str:
        """Build a Redis key within the active namespace."""
        return ":".join([self._base_prefix(), *[str(part) for part in parts]])

    def _client_key(self, client_id: Union[str, int]) -> str:
        return self._key("client", client_id)

    def _name_index_key(self, name: str) -> str:
        return self._key("name", name)

    def _api_key_index_key(self, api_key: str) -> str:
        return self._key("api_key", api_key)

    def _api_key_cache_get(self, api_key: str):
        """Return (hit, client) from the TTL cache; (False, None) on miss."""
        if self.api_key_cache_ttl <= 0:
            return False, None
        with self._api_key_cache_lock:
            entry = self._api_key_cache.get(api_key)
            if entry is None:
                return False, None
            ts, client = entry
            if time.monotonic() - ts >= self.api_key_cache_ttl:
                self._api_key_cache.pop(api_key, None)
                return False, None
            return True, client

    def _api_key_cache_gen_snapshot(self) -> int:
        """Generation token to pass to ``_api_key_cache_store``.

        Snapshot BEFORE reading Redis: if any invalidation lands between the
        snapshot and the store, the store is dropped rather than re-caching a
        value read before the mutation committed.
        """
        if self.api_key_cache_ttl <= 0:
            return 0
        with self._api_key_cache_lock:
            return self._api_key_cache_gen

    def _api_key_cache_store(self, api_key: str, client, gen: int) -> None:
        if self.api_key_cache_ttl <= 0:
            return
        with self._api_key_cache_lock:
            if gen != self._api_key_cache_gen:
                return
            if api_key not in self._api_key_cache and (
                len(self._api_key_cache) >= self.api_key_cache_size
            ):
                self._api_key_cache.clear()
            self._api_key_cache[api_key] = (time.monotonic(), client)

    def _api_key_cache_invalidate(self, api_key=None) -> None:
        """Drop one key (or everything) from the TTL cache.

        Called AFTER the Redis write commits on every path that maintains the
        materialized API-key hash, so THIS process reflects mutations
        immediately; other processes see them within the TTL. Also bumps the
        cache generation so an in-flight lookup that read Redis before the
        commit cannot store its stale result afterwards.
        """
        if self.api_key_cache_ttl <= 0:
            return
        with self._api_key_cache_lock:
            self._api_key_cache_gen += 1
            if api_key is None:
                self._api_key_cache.clear()
            else:
                self._api_key_cache.pop(str(api_key), None)

    def _api_key_record_key(self) -> str:
        """Return the materialized API-key-to-client-record hash key."""
        return self._key("api_key_records")

    def _search_doc_key(self, client_id: Union[str, int]) -> str:
        return self._key("idx", client_id)

    def _counter_key(self) -> str:
        return self._key("count")

    def _schema_version_key(self) -> str:
        return self._key("schema_version")

    def _id_sequence_key(self) -> str:
        return self._key("id_seq")

    def _search_index_name(self) -> str:
        if self.cluster_hash_tag:
            return f"{self.index_prefix}_{self.cluster_hash_tag}_search_index"
        return f"{self.index_prefix}_search_index"

    def _scan_pattern(self, *parts: Union[str, int]) -> str:
        return f"{self._key(*parts)}:*"

    def _pipeline(self):
        """Return a pipeline appropriate for the current Redis topology.

        Single-node Redis: transactional (MULTI/EXEC) so ``add_item`` /
        ``update_client`` multi-key writes land atomically. Cluster with
        a hash-tag namespace: transactional within the single slot.
        Legacy cluster mode (no hash tag) cannot do multi-key MULTI/EXEC
        across slots, so the pipeline is non-transactional there.
        """
        if getattr(self, "is_cluster", False):
            if self.cluster_hash_tag:
                return self.redis.pipeline(transaction=True)
            return self.redis.pipeline()
        return self.redis.pipeline(transaction=True)

    def _legacy_cluster_mode(self) -> bool:
        """Return True when running on Redis Cluster without a hash-tag namespace."""
        return bool(getattr(self, "is_cluster", False) and not self.cluster_hash_tag)

    def _retry_policy(self) -> redis.retry.Retry:
        """Build a shared retry policy for Redis clients."""
        return redis.retry.Retry(
            redis.backoff.ExponentialBackoff(base=self.retry_delay),
            retries=self.retry_attempts,
        )

    def _get_ssl_kwargs(self) -> dict:
        """
        Build SSL kwargs compatible with redis-py clients.

        Returns:
            Keyword arguments for SSL-enabled Redis connections.
        """
        if not self.use_ssl:
            return {}

        check_hostname = self.ssl_check_hostname
        if self.ssl_cert_reqs == "none":
            check_hostname = False

        ssl_kwargs = {
            "ssl": True,
            "ssl_cert_reqs": self.ssl_cert_reqs,
            "ssl_check_hostname": check_hostname,
        }
        if self.ssl_certfile:
            ssl_kwargs["ssl_certfile"] = self.ssl_certfile
        if self.ssl_keyfile:
            ssl_kwargs["ssl_keyfile"] = self.ssl_keyfile
        if self.ssl_ca_certs:
            ssl_kwargs["ssl_ca_certs"] = self.ssl_ca_certs
        return ssl_kwargs

    def _get_startup_nodes(self) -> List[ClusterNode]:
        """Normalize startup nodes into redis-py ClusterNode objects."""
        raw_nodes = self.cluster_nodes or [{"host": self.host, "port": self.port}]
        startup_nodes = []
        for node in raw_nodes:
            if isinstance(node, ClusterNode):
                startup_nodes.append(node)
            elif isinstance(node, dict):
                startup_nodes.append(ClusterNode(node["host"], int(node["port"])))
            else:
                raise ValueError(f"Unsupported cluster node type: {type(node)}")
        return startup_nodes

    def _detect_cluster(self) -> bool:
        """
        Detect if Redis is running in cluster mode.

        Returns:
            True if cluster mode is detected, False otherwise.
        """
        if self.cluster_nodes:
            return True

        test_client = None
        try:
            connection_kwargs = {
                "host": self.host,
                "port": self.port,
                "password": self.password,
                "username": self.username,
                "db": 0,
                "socket_connect_timeout": 2,
                "socket_timeout": 2,
            }
            connection_kwargs.update(self._get_ssl_kwargs())

            test_client = redis.StrictRedis(**connection_kwargs)
            test_client.ping()

            try:
                info = test_client.info("cluster")
                cluster_enabled = info.get("cluster_enabled")
                if cluster_enabled in (1, "1", True):
                    return True
                if cluster_enabled in (0, "0", False):
                    return False
            except redis.ResponseError as e:
                LOG.debug(f"INFO cluster failed during detection: {e}")

            try:
                cluster_info = test_client.execute_command("CLUSTER", "INFO")
                cluster_info = cluster_info.decode() if isinstance(cluster_info, bytes) else str(cluster_info)
                return "cluster_state:" in cluster_info or "cluster_slots_assigned:" in cluster_info
            except redis.ResponseError as e:
                if "cluster support disabled" in str(e).lower():
                    return False
                LOG.debug(f"CLUSTER INFO failed during detection: {e}")
                return False

        except Exception as e:
            LOG.debug(f"Cluster detection failed: {e}")
            return False
        finally:
            if test_client is not None:
                try:
                    test_client.close()
                except Exception:
                    pass

    def _create_single_connection(self) -> redis.StrictRedis:
        """
        Create connection to single Redis instance.

        Returns:
            Redis client instance
        """
        connection_kwargs = {
            'host': self.host,
            'port': self.port,
            'db': self.db,
            'password': self.password if self.password else None,
            'username': self.username,
            'decode_responses': True,
            'socket_connect_timeout': 5,
            'socket_timeout': 5,
            'health_check_interval': 30,
            'max_connections': self.max_connections,
        }
        connection_kwargs.update(self._get_ssl_kwargs())

        client = redis.StrictRedis(
            retry=self._retry_policy(),
            retry_on_error=[redis.ConnectionError, redis.TimeoutError],
            **connection_kwargs,
        )
        self.redis_pool = client.connection_pool
        return client

    def _create_cluster_connection(self) -> redis.RedisCluster:
        """
        Create connection to Redis Cluster.

        Returns:
            RedisCluster client instance
        """
        startup_nodes = self._get_startup_nodes()
        connection_kwargs = {
            "startup_nodes": startup_nodes,
            "password": self.password,
            "username": self.username,
            "decode_responses": True,
            "socket_connect_timeout": 5,
            "socket_timeout": 5,
            "max_connections": self.max_connections,
            "skip_full_coverage_check": True,
            "cluster_error_retry_attempts": self.retry_attempts,
            "retry": self._retry_policy(),
        }
        connection_kwargs.update(self._get_ssl_kwargs())

        try:
            return redis.RedisCluster(**connection_kwargs)
        except Exception as e:
            LOG.warning(f"Failed to create cluster connection: {e}")
            raise

    def _check_redisearch_availability(self) -> bool:
        """
        Check if RediSearch module is available on the Redis server.

        Returns:
            True if RediSearch is available, False otherwise.
        """
        if self._legacy_cluster_mode():
            LOG.info(
                "RediSearch is disabled in legacy Redis Cluster mode; "
                "enable cluster_hash_tag to use indexed cluster search."
            )
            return False
        try:
            modules = self.redis.execute_command("MODULE", "LIST")
            for module in modules:
                name = None
                if isinstance(module, dict):
                    name = module.get("name")
                elif isinstance(module, (list, tuple)):
                    for i in range(0, len(module) - 1, 2):
                        if str(module[i]).lower() == "name":
                            name = module[i + 1]
                            break
                if str(name).lower() == "search":
                    return True
        except Exception as e:
            LOG.debug(f"RediSearch module check failed: {e}")

        try:
            self.redis.execute_command("FT._LIST")
            return True
        except Exception:
            return False

    def _setup_redisearch_index(self):
        """
        Set up RediSearch index for clients if available.
        """
        try:
            index_name = self._search_index_name()
            self.redis.execute_command(
                "FT.CREATE", index_name,
                "ON", "HASH",
                "PREFIX", "1", f"{self._key('idx')}:",
                "SCHEMA",
                "name", "TEXT",
                "api_key", "TEXT"
            )
            LOG.debug(f"Created RediSearch index '{index_name}'")
        except redis.ResponseError as e:
            if "Index already exists" in str(e):
                LOG.debug(f"RediSearch index '{index_name}' already exists")
            else:
                LOG.warning(f"Failed to create RediSearch index: {e}")
        except Exception as e:
            LOG.warning(f"Error setting up RediSearch index: {e}")

    def _get_next_client_id(self) -> int:
        """
        Generate the next sequential client ID using Redis INCR for atomicity.

        Returns:
            The next available client ID
        """
        return int(self.redis.incr(self._id_sequence_key()))

    def _claim_item_key(self, item_key: str) -> bool:
        """
        Reserve a client key while a new record is being created.

        Uses an expiring marker in Redis so interrupted writes do not leave
        permanent locks behind.
        """
        try:
            return bool(self.redis.set(item_key, CREATE_MARKER, nx=True, ex=CREATE_MARKER_TTL))
        except TypeError:
            return bool(self.redis.setnx(item_key, CREATE_MARKER))

    def _maybe_migrate(self) -> None:
        """Run schema migration if the stored namespace version is behind
        ``SCHEMA_VERSION``.

        The version is stored at a per-namespace key ``{prefix}:schema_version``
        so multi-hub deployments using ``index_prefix``/``cluster_hash_tag``
        each track their own migration state. Tolerates older HPM that
        predates ``SCHEMA_VERSION`` by falling back to ``1``.
        """
        target = getattr(AbstractDB, "SCHEMA_VERSION", 1)
        try:
            raw = self.redis.get(self._schema_version_key())
            stored = int(raw) if raw is not None else 1
        except (ValueError, TypeError):
            stored = 1
        # Tolerate older HPM that predates the forward-compat guard, the
        # same way SCHEMA_VERSION is read defensively above.
        if hasattr(self, "_check_forward_compat"):
            self._check_forward_compat(stored)
        if stored < target:
            LOG.info("RedisDB: migrating namespace '%s' schema v%d -> v%d",
                     self._base_prefix(), stored, target)
            # Bundle the per-record rewrites and the schema sentinel SET
            # into one MULTI/EXEC so they land atomically — a crash can
            # never leave the namespace in a "rows migrated but stale
            # sentinel" (or vice versa) state.
            try:
                pipe = self.redis.pipeline(transaction=True)
            except Exception:
                pipe = None
            if pipe is None:
                self.migrate(from_version=stored)
                self.redis.set(self._schema_version_key(), int(target))
            else:
                self._migrate_into_pipeline(pipe, from_version=stored)
                pipe.set(self._schema_version_key(), int(target))
                pipe.execute()

    def migrate(self, from_version: int) -> None:
        """Migrate stored client records to the current ``SCHEMA_VERSION``.

        Idempotent and crash-safe: a partial migration re-run produces
        the same final state. A row that has already been migrated has
        no top-level legacy keys, so the per-row update is a no-op.

        v1 -> v2: fold each record's top-level ``intent_blacklist`` /
        ``skill_blacklist`` JSON values into the record's ``metadata``
        dict (``setdefault`` — explicit metadata values are never
        clobbered), then remove the legacy top-level keys.
        ``message_blacklist`` is purged outright (the field is not
        part of the ``Client`` data model); any residual
        ``metadata["message_blacklist"]`` from a prior migration run
        is also stripped.
        """
        self._migrate_into_pipeline(self.redis, from_version=from_version)

    def _migrate_into_pipeline(self, writer, from_version: int) -> None:
        """Internal migration loop that issues writes through ``writer``
        (either ``self.redis`` for a non-transactional pass or a
        ``Redis.pipeline(transaction=True)`` so the rewrites and the
        schema sentinel land atomically). ``writer`` must expose a
        ``.set(key, value)`` method — both ``Redis`` and ``Pipeline``
        do."""
        if from_version >= 2:
            return
        legacy_keys = ("intent_blacklist", "skill_blacklist")
        for key, raw in _iter_client_records_safely(self):
            if raw == CREATE_MARKER:
                continue
            try:
                record = json.loads(raw)
            except (TypeError, ValueError) as e:
                LOG.warning("RedisDB migrate v2: skipping unparseable record "
                            "at %s: %s", key, e)
                continue
            if not isinstance(record, dict):
                continue
            metadata = record.get("metadata") if isinstance(
                record.get("metadata"), dict) else {}
            changed = False
            # Strip message_blacklist outright (top-level + metadata).
            if "message_blacklist" in record:
                record.pop("message_blacklist", None)
                changed = True
            if metadata.pop("message_blacklist", None) is not None:
                changed = True
            for lk in legacy_keys:
                if lk in record:
                    val = record.pop(lk)
                    changed = True
                    if val and lk not in metadata:
                        metadata[lk] = list(val) if isinstance(
                            val, (list, tuple)) else val
            if changed:
                record["metadata"] = metadata
                try:
                    serialized = json.dumps(record)
                    writer.set(key, serialized)
                    api_key = record.get("api_key")
                    if api_key and api_key != "revoked":
                        writer.hset(
                            self._api_key_record_key(), api_key, serialized
                        )
                        self._api_key_cache_invalidate(api_key)
                except Exception as e:
                    LOG.error("RedisDB migrate v2: failed to rewrite %s: %s",
                              key, e)

    def add_item(self, client: Client) -> bool:
        """
        Add a new client to the Redis database or update existing client.

        Args:
            client: The client object to add or update.

        Returns:
            True if the client was added/updated successfully, False otherwise.
        """
        claimed_key = None
        try:
            raw_client_id = getattr(client, "client_id", None)
            client_id_was_provided = isinstance(raw_client_id, int)

            # Ensure client has a valid ID
            if not hasattr(client, 'client_id') or client.client_id is None:
                client.client_id = self._get_next_client_id()
            elif isinstance(client.client_id, str):
                try:
                    client.client_id = int(client.client_id)
                    client_id_was_provided = True
                except ValueError:
                    client.client_id = self._get_next_client_id()
            elif not isinstance(client.client_id, int):
                client.client_id = self._get_next_client_id()

            # Ensure client attributes are initialized
            self._ensure_client_attributes(client)
            retry_delay = getattr(self, "retry_delay", 0.1)
            max_marker_waits = max(1, getattr(self, "retry_attempts", 0) + 1)
            marker_waits = 0

            while True:
                item_key = self._client_key(client.client_id)
                if self._claim_item_key(item_key):
                    claimed_key = item_key
                    break

                existing_data = self.redis.get(item_key)
                if existing_data == CREATE_MARKER:
                    marker_waits += 1
                    if marker_waits > max_marker_waits:
                        if client_id_was_provided:
                            LOG.warning(f"Client '{client.client_id}' is locked by an in-progress write")
                            return False
                        client.client_id = self._get_next_client_id()
                        marker_waits = 0
                        continue
                    time.sleep(retry_delay)
                    continue

                marker_waits = 0
                if client_id_was_provided:
                    LOG.debug(f"Client '{client.client_id}' already exists, updating instead of creating new")
                    return self.update_client(client)

                client.client_id = self._get_next_client_id()

            serialized_client = client.serialize()

            if self._legacy_cluster_mode():
                self.redis.set(item_key, serialized_client)
                self.redis.hset(
                    self._api_key_record_key(),
                    client.api_key,
                    serialized_client,
                )
                self._api_key_cache_invalidate(client.api_key)
                LOG.debug(f"Successfully added client '{client.client_id}' in legacy cluster mode")
                return True

            # Use pipeline for atomic operations
            p = self._pipeline()
            p.set(item_key, serialized_client)
            p.sadd(self._name_index_key(client.name), str(client.client_id))
            p.sadd(self._api_key_index_key(client.api_key), str(client.client_id))
            p.hset(
                self._api_key_record_key(),
                client.api_key,
                serialized_client,
            )
            # Feed RediSearch doc
            p.hset(self._search_doc_key(client.client_id), mapping={
                "name": client.name,
                "api_key": client.api_key,
            })
            p.incr(self._counter_key())
            p.execute()
            # After commit: evict a cached negative lookup for this key so a
            # freshly added client is admitted immediately.
            self._api_key_cache_invalidate(client.api_key)

            LOG.debug(f"Successfully added client '{client.client_id}'")
            return True
        except Exception as e:
            if claimed_key and self.redis.get(claimed_key) == CREATE_MARKER:
                self.redis.delete(claimed_key)
            LOG.error(f"Failed to add client: {e}")
            return False

    def delete_item(self, client: Client) -> bool:
        """
        Revoke a client's credentials by updating their record.
        This is the method called by hivemind-core delete-client command.

        Args:
            client: The client object to revoke.

        Returns:
            True if the revocation was successful, False otherwise.
        """
        return self.remove_client(str(client.client_id))

    def remove_client(self, client_id: str) -> bool:
        """
        Revoke a client's credentials by updating their record.

        Args:
            client_id: The ID of the client to revoke.

        Returns:
            True if the revocation was successful, False otherwise.
        """
        try:
            item_key = self._client_key(client_id)
            client_data = self.redis.get(item_key)
            if not client_data:
                LOG.warning(f"Client '{client_id}' not found for revocation")
                return False

            # Parse the serialized data (assuming JSON)
            data = json.loads(client_data)
            old_name = data.get('name', '')
            old_api_key = data.get('api_key', '')
            if not isinstance(data.get("metadata"), dict):
                data["metadata"] = {}

            # Revoke credentials
            data['name'] = ""
            data['api_key'] = "revoked"
            data['password'] = None
            data['crypto_key'] = None

            if self._legacy_cluster_mode():
                self.redis.set(item_key, json.dumps(data))
                self.redis.hdel(self._api_key_record_key(), old_api_key)
                self._api_key_cache_invalidate(old_api_key)
                LOG.info(f"Successfully revoked client '{client_id}' in legacy cluster mode")
                return True

            p = self._pipeline()
            p.set(item_key, json.dumps(data))

            # Update indices
            p.srem(self._name_index_key(old_name), client_id)
            p.srem(self._api_key_index_key(old_api_key), client_id)
            p.sadd(self._api_key_index_key("revoked"), client_id)
            p.hdel(self._api_key_record_key(), old_api_key)
            # Update RediSearch doc
            p.hset(self._search_doc_key(client_id), mapping={
                "name": "",
                "api_key": "revoked",
            })

            p.execute()
            # After commit, never before: invalidating pre-commit lets a
            # concurrent lookup re-cache the still-unrevoked Redis value.
            self._api_key_cache_invalidate(old_api_key)

            LOG.info(f"Successfully revoked client '{client_id}'")
            return True
        except json.JSONDecodeError as e:
            LOG.error(f"Failed to parse client data for '{client_id}': {e}")
            return False
        except Exception as e:
            LOG.error(f"Unexpected error while revoking client '{client_id}': {e}")
            return False

    def _ensure_client_attributes(self, client: Client):
        """Replace explicit-None list fields with []. Needed for legacy records
        where a list column was stored as ``null``: ``Client(intent_blacklist=None)``
        keeps None (``default_factory`` only fires when the kwarg is omitted).
        ``metadata`` is handled by ``Client.__post_init__`` (>=0.5.0).
        """
        # message_blacklist is no longer part of the Client data model.
        for attr in ('intent_blacklist', 'skill_blacklist'):
            if getattr(client, attr, None) is None:
                setattr(client, attr, [])

    @staticmethod
    def _deserialize_client(client_data) -> Client:
        """Pre-clean a stored record before handing it to ``cast2client``.

        ``Client.deserialize`` raises ``TypeError`` if ``metadata`` is present
        but not a dict (intentional in plugin-manager >=0.5.0). Records written
        before metadata existed have no ``metadata`` key — they're handled by
        Client's default factory. Records hand-edited or written by buggy
        callers may carry a non-dict ``metadata`` value; coerce those to ``{}``
        here so a single bad row doesn't break iteration over the DB.
        """
        if isinstance(client_data, str):
            client_data = json.loads(client_data)
        if isinstance(client_data, dict) and "metadata" in client_data \
                and not isinstance(client_data["metadata"], dict):
            client_data = dict(client_data)
            client_data["metadata"] = {}
        return cast2client(client_data)

    def update_client(self, client: Client) -> bool:
        """
        Update an existing client's information in Redis.

        Args:
            client: The client object with updated information.

        Returns:
            True if the update was successful, False otherwise.
        """
        try:
            item_key = self._client_key(client.client_id)
            old_client_data = self.redis.get(item_key)
            if not old_client_data:
                LOG.warning(f"Client '{client.client_id}' not found for update")
                return False

            old_client = self._deserialize_client(old_client_data)
            self._ensure_client_attributes(client)

            if self._legacy_cluster_mode():
                serialized_client = client.serialize()
                self.redis.set(item_key, serialized_client)
                self.redis.hdel(
                    self._api_key_record_key(), old_client.api_key
                )
                self.redis.hset(
                    self._api_key_record_key(),
                    client.api_key,
                    serialized_client,
                )
                self._api_key_cache_invalidate(old_client.api_key)
                self._api_key_cache_invalidate(client.api_key)
                LOG.debug(f"Successfully updated client '{client.client_id}' in legacy cluster mode")
                return True

            p = self._pipeline()
            serialized_client = client.serialize()
            p.set(item_key, serialized_client)
            
            # Update indices only if values changed
            if old_client.name != client.name:
                p.srem(self._name_index_key(old_client.name), str(client.client_id))
                p.sadd(self._name_index_key(client.name), str(client.client_id))
            if old_client.api_key != client.api_key:
                p.srem(self._api_key_index_key(old_client.api_key), str(client.client_id))
                p.sadd(self._api_key_index_key(client.api_key), str(client.client_id))
                p.hdel(self._api_key_record_key(), old_client.api_key)
            p.hset(
                self._api_key_record_key(),
                client.api_key,
                serialized_client,
            )
            # Update RediSearch doc
            p.hset(self._search_doc_key(client.client_id), mapping={
                "name": client.name,
                "api_key": client.api_key,
            })

            p.execute()
            # After commit, never before (a concurrent lookup could otherwise
            # re-cache the pre-update value). The new key is invalidated even
            # when unchanged: a same-key update still rewrites the record.
            if old_client.api_key != client.api_key:
                self._api_key_cache_invalidate(old_client.api_key)
            self._api_key_cache_invalidate(client.api_key)
            LOG.debug(f"Successfully updated client '{client.client_id}'")
            return True
        except Exception as e:
            LOG.error(f"Failed to update client '{client.client_id}': {e}")
            return False

    def update_last_seen(self, api_key: str, seen_at: float) -> bool:
        """Atomically advance ``last_seen`` for the matching API key."""
        try:
            client = self.get_client_by_api_key(api_key)
            if client is None:
                return False
            updated = bool(
                self.redis.eval(
                    _ADVANCE_LAST_SEEN_LUA,
                    1,
                    self._client_key(client.client_id),
                    api_key,
                    float(seen_at),
                    CREATE_MARKER,
                )
            )
            if updated:
                # The Lua script updates the authoritative primary record.
                # Invalidate its derived admission record so the next lookup
                # repairs it from that new value instead of returning stale
                # session metadata.
                self.redis.hdel(self._api_key_record_key(), api_key)
                self._api_key_cache_invalidate(api_key)
            return updated
        except Exception as e:
            LOG.error(f"Failed to update last_seen for client '{api_key}': {e}")
            return False

    def get_client_by_id(self, client_id: int) -> Optional[Client]:
        """Fetch a single client by primary key with one ``GET``.

        Targeted lookup used by :meth:`refresh` on the admission hot
        path — avoids any ``scan_iter`` / index walk. Returns ``None``
        if the key is missing, holds a stale create marker, or fails
        to deserialize.
        """
        if client_id is None:
            return None
        try:
            raw = self.redis.get(self._client_key(client_id))
        except Exception as e:
            LOG.error(f"Failed to fetch client {client_id} from Redis: {e}")
            return None
        if not raw or raw == CREATE_MARKER:
            return None
        try:
            return self._deserialize_client(raw)
        except Exception as e:
            LOG.warning(f"Failed to deserialize client {client_id}: {e}")
            return None

    def refresh(self, client_id: int) -> Optional[Client]:
        """Targeted single-key fetch — no global ``sync()``.

        Overrides the base default so the admission chain never triggers
        a full keyspace scan + index rebuild per inbound message.
        """
        return self.get_client_by_id(client_id)

    def _search_with_redisearch(self, key: str, val: str) -> List[Client]:
        """
        Search using RediSearch if available.

        Args:
            key: The field to search by
            val: The value to search for

        Returns:
            List of matching clients
        """
        try:
            index_name = self._search_index_name()
            def _esc(v: str) -> str:
                return v.replace('\\', '\\\\').replace('"', '\\"').replace(':', '\\:')
            query = f'@{key}:"{_esc(str(val))}"'
            results = self.redis.execute_command("FT.SEARCH", index_name, query)
            res = []
            client_keys = []
            if results and len(results) > 1:
                for i in range(1, len(results), 2):
                    doc_key = results[i]
                    if doc_key.startswith(f"{self._key('idx')}:"):
                        client_id = doc_key.split(":")[-1]
                        client_keys.append(self._client_key(client_id))
                for client in self._load_clients(client_keys):
                    if hasattr(client, key) and getattr(client, key) == val:
                        res.append(client)
            return res
        except Exception:
            return []

    def _search_with_index(self, key: str, val: str) -> List[Client]:
        """
        Search using Redis sets for indexed fields.

        Args:
            key: The field to search by
            val: The value to search for

        Returns:
            List of matching clients
        """
        LOG.debug(f"Searching for clients by indexed field '{key}' with value '{val}'")
        client_ids = self.redis.smembers(self._key(key, val))
        client_keys = [self._client_key(cid) for cid in client_ids]
        res = self._load_clients(client_keys)
        LOG.debug(f"Found {len(res)} clients matching '{key}={val}'")
        return res

    def _search_brute_force(self, key: str, val) -> List[Client]:
        """
        Fallback search by scanning all clients.

        Args:
            key: The field to search by
            val: The value to search for

        Returns:
            List of matching clients
        """
        res = []
        for batch in self._iter_key_batches(self.redis.scan_iter(self._scan_pattern("client"), count=100)):
            for client in self._load_clients(batch):
                if hasattr(client, key) and getattr(client, key) == val:
                    res.append(client)
        return res

    def get_client_by_api_key_with_metrics(
            self, api_key: str) -> tuple[Optional[Client], dict[str, float]]:
        """Return an API-key match and exact Redis/deserialization timings.

        The materialized hash turns the admission hot path into one ``HGET``.
        Existing namespaces are repaired lazily from the authoritative
        secondary index; all create, update, revoke, and sync paths explicitly
        maintain or invalidate the hash.
        """
        timings = {
            "redis_command_ms": 0.0,
            "redis_deserialize_ms": 0.0,
            "redis_round_trips": 0.0,
        }
        if api_key is None:
            return None, timings
        api_key = str(api_key)

        hit, cached = self._api_key_cache_get(api_key)
        if hit:
            timings["cache_hit"] = 1.0
            return cached, timings
        cache_gen = self._api_key_cache_gen_snapshot()

        command_started = time.monotonic()
        raw = self.redis.hget(self._api_key_record_key(), api_key)
        timings["redis_command_ms"] += (
            time.monotonic() - command_started
        ) * 1000
        timings["redis_round_trips"] += 1
        if raw:
            deserialize_started = time.monotonic()
            try:
                client = self._deserialize_client(raw)
                self._ensure_client_attributes(client)
            except Exception as error:
                LOG.warning(
                    "Failed to deserialize materialized API-key record: %s",
                    error,
                )
                client = None
            timings["redis_deserialize_ms"] += (
                time.monotonic() - deserialize_started
            ) * 1000
            if client is not None and client.api_key == api_key:
                self._api_key_cache_store(api_key, client, cache_gen)
                return client, timings
            command_started = time.monotonic()
            self.redis.hdel(self._api_key_record_key(), api_key)
            self._api_key_cache_invalidate(api_key)
            timings["redis_command_ms"] += (
                time.monotonic() - command_started
            ) * 1000
            timings["redis_round_trips"] += 1

        # Operators can maintain this index in legacy Redis Cluster mode too.
        # Preserve that bounded compatibility path before the authoritative
        # scan needed for namespaces written by old/manual clients.
        command_started = time.monotonic()
        client_ids = self.redis.smembers(self._api_key_index_key(api_key))
        raws = self._get_many([
            self._client_key(client_id) for client_id in client_ids
        ])
        timings["redis_command_ms"] += (
            time.monotonic() - command_started
        ) * 1000
        timings["redis_round_trips"] += 2 if client_ids else 1
        client = None
        deserialize_started = time.monotonic()
        for candidate_raw in raws:
            if not candidate_raw or candidate_raw == CREATE_MARKER:
                continue
            try:
                candidate = self._deserialize_client(candidate_raw)
                self._ensure_client_attributes(candidate)
            except Exception as error:
                LOG.warning(
                    "Failed to deserialize indexed API-key record: %s",
                    error,
                )
                continue
            if candidate.api_key == api_key:
                client = candidate
                break
        timings["redis_deserialize_ms"] += (
            time.monotonic() - deserialize_started
        ) * 1000

        if client is None and self._legacy_cluster_mode():
            command_started = time.monotonic()
            matches = self._search_brute_force("api_key", api_key)
            timings["redis_command_ms"] += (
                time.monotonic() - command_started
            ) * 1000
            client = matches[0] if matches else None

        if client is not None:
            command_started = time.monotonic()
            self.redis.hset(
                self._api_key_record_key(), api_key, client.serialize()
            )
            timings["redis_command_ms"] += (
                time.monotonic() - command_started
            ) * 1000
            timings["redis_round_trips"] += 1
        self._api_key_cache_store(api_key, client, cache_gen)
        return client, timings

    def get_client_by_api_key(self, api_key: str) -> Optional[Client]:
        """Return the client for an API key without requiring a full sync.

        API-key lookup is on the admission path in hivemind-core, so unknown
        keys must fail through indexed lookups only. ``sync()`` remains the
        explicit repair path for interrupted/manual writes that leave secondary
        indexes stale.
        """
        client, _timings = self.get_client_by_api_key_with_metrics(api_key)
        return client

    def search_by_value(self, key: str, val) -> List[Client]:
        """
        Search for clients by a specific key-value pair in Redis.

        Args:
            key: The key to search by.
            val: The value to search for.

        Returns:
            A list of clients that match the search criteria.
        """
        if key == 'api_key':
            client = self.get_client_by_api_key(val)
            return [client] if client else []

        if self._legacy_cluster_mode():
            return self._search_brute_force(key, val)

        if self.redisearch_available and key in ['name', 'api_key']:
            redisearch_results = self._search_with_redisearch(key, val)
            if redisearch_results:
                return redisearch_results

        if key in ['name', 'api_key']:
            return self._search_with_index(key, val)

        return self._search_brute_force(key, val)

    def sync(self):
        """
        Rebuild counters and secondary indexes from stored client records.

        This repairs drift after interrupted writes or manual Redis changes.
        """
        try:
            if self.redisearch_available:
                self._setup_redisearch_index()

            client_records = []
            max_client_id = 0
            for item_key in self.redis.scan_iter(self._scan_pattern("client"), count=100):
                client_data = self.redis.get(item_key)
                if not client_data:
                    continue
                if client_data == CREATE_MARKER:
                    self.redis.delete(item_key)
                    continue
                try:
                    client = self._deserialize_client(client_data)
                    self._ensure_client_attributes(client)
                    client_records.append(client)
                    max_client_id = max(max_client_id, int(client.client_id))
                except Exception as e:
                    LOG.warning(f"Skipping invalid client record '{item_key}': {e}")

            index_keys = []
            for pattern in (
                self._scan_pattern("name"),
                self._scan_pattern("api_key"),
                self._scan_pattern("idx"),
            ):
                index_keys.extend(list(self.redis.scan_iter(pattern, count=100)))

            p = self._pipeline()
            for key in index_keys:
                p.delete(key)
            p.delete(self._api_key_record_key())

            p.set(self._counter_key(), len(client_records))
            p.set(self._id_sequence_key(), max_client_id)

            if not self._legacy_cluster_mode():
                for client in client_records:
                    if client.api_key != "revoked" and client.name:
                        p.sadd(self._name_index_key(client.name), str(client.client_id))
                    p.sadd(self._api_key_index_key(client.api_key), str(client.client_id))
                    if client.api_key != "revoked":
                        p.hset(
                            self._api_key_record_key(),
                            client.api_key,
                            client.serialize(),
                        )
                    p.hset(self._search_doc_key(client.client_id), mapping={
                        "name": client.name,
                        "api_key": client.api_key,
                    })

            p.execute()
            # Full-rebuild committed: drop the whole cache after, not before,
            # so no lookup can re-cache pre-rebuild state mid-sync.
            self._api_key_cache_invalidate()
            LOG.info(f"Redis database sync complete for '{self.index_prefix}' ({len(client_records)} clients)")
            return True
        except Exception as e:
            LOG.error(f"Failed to sync Redis database: {e}")
            return False

    def _count_client_records(self) -> int:
        """Count stored client records, excluding stale create markers."""
        count = 0
        for batch in self._iter_key_batches(self.redis.scan_iter(self._scan_pattern("client"), count=100)):
            for client_data in self._get_many(batch):
                if client_data and client_data != CREATE_MARKER:
                    count += 1
        return count

    def _iter_key_batches(self, keys: Iterable[str], batch_size: int = 100) -> Iterable[List[str]]:
        """Yield keys in fixed-size batches for pipelined reads."""
        batch = []
        for key in keys:
            batch.append(key)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _get_many(self, keys: List[str]) -> List[Optional[str]]:
        """Fetch multiple keys efficiently across single-node and cluster clients."""
        if not keys:
            return []

        if not getattr(self, "is_cluster", False) and hasattr(self.redis, "mget"):
            try:
                return list(self.redis.mget(keys))
            except Exception as e:
                LOG.debug(f"mget batch read failed, falling back to pipeline: {e}")

        pipe = self.redis.pipeline()
        for key in keys:
            pipe.get(key)
        return pipe.execute()

    def _load_clients(self, keys: List[str], *, include_revoked: bool = False) -> List[Client]:
        """Load and deserialize client records from Redis keys."""
        clients = []
        for key, client_data in zip(keys, self._get_many(keys)):
            if not client_data or client_data == CREATE_MARKER:
                continue
            try:
                client = self._deserialize_client(client_data)
                if not include_revoked and client.api_key == "revoked":
                    continue
                self._ensure_client_attributes(client)
                clients.append(client)
            except Exception as e:
                LOG.warning(f"Failed to deserialize client data for '{key}': {e}")
        return clients

    def __len__(self) -> int:
        """
        Get the number of items in the Redis database.

        Returns:
            The number of clients in the database.
        """
        if self._legacy_cluster_mode():
            try:
                return self._count_client_records()
            except Exception as e:
                LOG.warning(f"Failed to count client records in legacy cluster mode: {e}")
                return 0

        counter_key = self._counter_key()
        try:
            count = self.redis.get(counter_key)
            if count is not None:
                return int(count)
            self.redis.setnx(counter_key, 0)
            return 0
        except Exception as e:
            LOG.warning(f"Failed to get client count from counter: {e}")
            return 0

    def __iter__(self) -> Iterable['Client']:
        """
        Iterate over all clients in Redis.

        Returns:
            An iterator over the clients in the database.
        """
        for batch in self._iter_key_batches(self.redis.scan_iter(self._scan_pattern("client"), count=100)):
            for client in self._load_clients(batch):
                yield client

    def __del__(self):
        """
        Cleanup resources when the object is destroyed.
        """
        try:
            if hasattr(self, 'redis_pool') and self.redis_pool:
                self.redis_pool.disconnect()
        except Exception as e:
            LOG.error(f"Error during Redis pool cleanup: {e}")

    def health_check(self) -> bool:
        """
        Perform a health check on the Redis connection.

        Returns:
            True if Redis is healthy, False otherwise
        """
        try:
            self.redis.ping()
            return True
        except Exception as e:
            LOG.error(f"Redis health check failed: {e}")
            return False
