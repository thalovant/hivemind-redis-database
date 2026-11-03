"""The package's own dependency graph never selects an affected filelock."""
import re
import tomllib
from pathlib import Path

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


def _dependencies() -> list[str]:
    return tomllib.loads(PYPROJECT.read_text())["project"]["dependencies"]


def test_filelock_is_pinned_outside_the_advisory_ranges_on_every_python() -> None:
    filelock = [d for d in _dependencies() if re.match(r"filelock\b", d)]
    assert "filelock==3.20.3; python_version < '3.11'" in filelock
    assert "filelock>=4.2.1,<5; python_version >= '3.11'" in filelock
    assert len(filelock) == 2


def test_bitstring_stays_below_5_for_the_bus_client() -> None:
    assert "bitstring>=4.1.1,<5" in _dependencies()
