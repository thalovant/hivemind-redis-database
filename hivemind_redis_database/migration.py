import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Tuple

from ovos_utils.log import LOG

from hivemind_redis_database import CREATE_MARKER, RedisDB


@dataclass
class MigrationSummary:
    copied_records: int = 0
    skipped_markers: int = 0
    target_namespace: str = ""
    source_namespace: str = ""


def _load_config(path: str, plugin_name: str) -> Dict:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if "database" in raw:
        raw = raw["database"]
    if raw.get("module") == plugin_name:
        return dict(raw.get(plugin_name, {}))
    if plugin_name in raw and isinstance(raw[plugin_name], dict):
        return dict(raw[plugin_name])
    return dict(raw)


def _iter_client_records(db: RedisDB) -> Iterable[Tuple[str, str]]:
    """Every record of ``db``'s namespace: its client index, or in legacy
    cluster mode its key walk (see ``RedisDB._iter_client_keys``)."""
    for key in db._iter_client_keys():
        value = db.redis.get(key)
        if not value:
            continue
        if value == CREATE_MARKER:
            yield key, CREATE_MARKER
            continue
        yield key, value


def migrate_namespace(
    source_db: RedisDB,
    target_db: RedisDB,
    *,
    clear_target: bool = False,
    dry_run: bool = False,
) -> MigrationSummary:
    if source_db._base_prefix() == target_db._base_prefix():
        raise ValueError("Source and target namespaces must be different")

    summary = MigrationSummary(
        source_namespace=source_db._base_prefix(),
        target_namespace=target_db._base_prefix(),
    )

    if clear_target and not dry_run:
        for pattern in (
            target_db._scan_pattern("client"),
            target_db._scan_pattern("name"),
            target_db._scan_pattern("api_key"),
            target_db._scan_pattern("idx"),
        ):
            for key in list(target_db.redis.scan_iter(pattern, count=100)):
                target_db.redis.delete(key)
        target_db.redis.delete(target_db._counter_key())
        target_db.redis.delete(target_db._id_sequence_key())
        target_db.redis.delete(target_db._client_ids_key())
        target_db.redis.delete(target_db._client_ids_built_key())

    for source_key, value in _iter_client_records(source_db):
        if value == CREATE_MARKER:
            summary.skipped_markers += 1
            continue

        client_id = source_key.split(":")[-1]
        if not dry_run:
            target_db.redis.set(target_db._client_key(client_id), value)
            # The target's sync() finds records through its client index.
            target_db.redis.sadd(target_db._client_ids_key(), client_id)
        summary.copied_records += 1

    if not dry_run:
        if not target_db.ensure_client_index():
            raise RuntimeError("Target namespace client index could not be built")
        if not target_db.sync():
            raise RuntimeError("Target namespace sync failed after copy")

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Migrate HiveMind Redis client records from a legacy namespace to a cluster-hash-tag namespace."
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to server.json or a JSON file containing hivemind-redis-db-plugin configuration",
    )
    parser.add_argument(
        "--plugin-name",
        default="hivemind-redis-db-plugin",
        help="Plugin config key to read from the config file",
    )
    parser.add_argument(
        "--target-cluster-hash-tag",
        required=True,
        help="Cluster hash tag for the target namespace, for example 'clients'",
    )
    parser.add_argument(
        "--source-cluster-hash-tag",
        default=None,
        help="Optional source cluster hash tag if migrating from one tagged namespace to another",
    )
    parser.add_argument(
        "--clear-target",
        action="store_true",
        help="Delete existing target namespace keys before copying records",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be copied without writing to Redis",
    )
    args = parser.parse_args()

    config = _load_config(args.config, args.plugin_name)
    source_config = dict(config)
    source_config["cluster_hash_tag"] = args.source_cluster_hash_tag
    target_config = dict(config)
    target_config["cluster_hash_tag"] = args.target_cluster_hash_tag

    source_db = RedisDB(**source_config)
    target_db = RedisDB(**target_config)
    summary = migrate_namespace(
        source_db,
        target_db,
        clear_target=args.clear_target,
        dry_run=args.dry_run,
    )

    LOG.info(
        "Migration complete: copied=%s skipped_markers=%s source=%s target=%s dry_run=%s",
        summary.copied_records,
        summary.skipped_markers,
        summary.source_namespace,
        summary.target_namespace,
        args.dry_run,
    )
    print(
        json.dumps(
            {
                "copied_records": summary.copied_records,
                "skipped_markers": summary.skipped_markers,
                "source_namespace": summary.source_namespace,
                "target_namespace": summary.target_namespace,
                "dry_run": args.dry_run,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
