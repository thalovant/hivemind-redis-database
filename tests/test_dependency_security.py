"""Keep the real install metadata and classic lock behavior security-pinned."""

from importlib.metadata import metadata, version
from pathlib import Path

from combo_lock import ComboLock
from packaging.requirements import Requirement


def test_wheel_metadata_keeps_exact_filelock_pin() -> None:
    """Installed distribution metadata must constrain the actual runtime graph."""
    requirements = [
        Requirement(value)
        for value in metadata("hivemind-redis-database").get_all("Requires-Dist", [])
    ]
    selected = [item for item in requirements if item.name == "filelock"]
    assert len(selected) == 1
    assert str(selected[0].specifier) == "==3.20.3"
    assert selected[0].marker is None
    assert all(item.url is None for item in requirements)
    assert version("filelock") == "3.20.3"


def test_bus_client_bitstream_contract() -> None:
    """Fresh installs must retain the BitStream API used by the bus client."""
    from bitstring import BitStream
    from hivemind_bus_client.serialization import HiveMindBinaryPayloadType

    assert BitStream("0b1").bin == "1"
    assert HiveMindBinaryPayloadType is not None


def test_combo_lock_excludes_other_instances_and_releases(tmp_path: Path) -> None:
    """The selected filelock must preserve the API used by transitive consumers."""
    held = ComboLock(tmp_path / "dependency.lock")
    contender = ComboLock(tmp_path / "dependency.lock")
    with held:
        assert contender.acquire(blocking=False) is False
    assert contender.acquire(blocking=False) is True
    contender.release()


def test_legacy_client_api_support_is_explicit() -> None:
    """The declared range must preserve the Client representation we test."""
    requirements = [
        Requirement(value)
        for value in metadata("hivemind-redis-database").get_all("Requires-Dist", [])
    ]
    selected = [item for item in requirements if item.name == "hivemind-plugin-manager"]
    runtime = next(item for item in selected if item.marker is None)
    assert runtime.specifier.contains("0.6.0a1", prereleases=True)
    assert runtime.specifier.contains("0.8.0a3", prereleases=True)
    for item in selected:
        assert not item.specifier.contains("0.9.0a1", prereleases=True)
        assert not item.specifier.contains("0.10.0a1", prereleases=True)


def test_topology_extra_uses_the_existing_policy_e2e_stack() -> None:
    """Keep the complete legacy topology suite on its supported constructor API."""
    requirements = [
        Requirement(value)
        for value in metadata("hivemind-redis-database").get_all("Requires-Dist", [])
    ]
    test_requirements = {
        item.name: str(item.specifier)
        for item in requirements
        if item.marker is not None and item.marker.evaluate({"extra": "test"})
    }
    assert test_requirements["hivescope"] == "==0.4.0a2"
    assert test_requirements["hivemind-core"] == "==4.3.0a2"
    assert test_requirements["hivemind-ovos-agent-plugin"] == "==0.2.0a1"
    assert test_requirements["hivemind-plugin-manager"] == "==0.6.0a1"

