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

