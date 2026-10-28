import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
LIBRARY = ROOT / "plc_bridge" / "pyproject.toml"
FRAMEWORK = ROOT / "exts" / "loupe.simulation.bridge" / "config" / "extension.toml"


@pytest.mark.skipif(not FRAMEWORK.exists(), reason="framework extension not in this checkout")
def test_the_framework_pin_starts_at_this_library_version():
    # The startup version check catches a stale install only if the lower
    # bound moves with every library release.
    version = re.search(r'^version\s*=\s*"([^"]+)"', LIBRARY.read_text(), re.M).group(1)
    pin = re.search(r'"plc-bridge>=([^,"]+),<', FRAMEWORK.read_text()).group(1)
    assert pin == version, f"extension.toml pins plc-bridge>={pin}, the library is {version}"
