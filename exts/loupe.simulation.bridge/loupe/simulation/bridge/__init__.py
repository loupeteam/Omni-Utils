"""
loupe.simulation.bridge: the vendor-neutral PLC bridge for Omniverse Kit.

Copyright (c) 2024 Loupe, https://loupe.team
Part of Omni-Utils, licensed under the MIT License.

Public surface, re-exported here:

    registry   register(), get(), names(): vendor extensions register a driver
    Option     one entry of a driver's option schema
    get_system(), get_plc(name), on_sample_main(name, cb)   data delivery
    Manager    the 0.2.x bus-based API, on the neutral bus names
    Manager_Events, get_stream_name, legacy_bus_names_enabled, BUS_NAMESPACE
               the bus names, for the vendor extensions' deprecated modules
"""

import sys as _sys

# plc_bridge is a pip requirement that Kit's pipapi installs before this module
# loads. Kit has its working directory on sys.path, so when Kit is started from
# a checkout of this repo the bare plc_bridge/ folder at the root is importable
# as an empty namespace package. pipapi's import check then caches that empty
# package in sys.modules before the wheel is installed, and the real package can
# no longer be imported in that process. Drop such an entry (no __file__: a
# namespace package, never the real one) so the imports below find the
# installed package.
_mod = _sys.modules.get("plc_bridge")
if _mod is not None and getattr(_mod, "__file__", None) is None:
    del _sys.modules["plc_bridge"]
del _mod, _sys

from . import registry  # noqa: E402,F401
from .registry import Option, DriverSpec  # noqa: E402,F401
from .delivery import get_system, get_plc, on_sample_main  # noqa: E402,F401
from .bus import (  # noqa: E402,F401
    BUS_NAMESPACE, Manager, Manager_Events, get_stream_name, legacy_bus_names_enabled)
from .extension import *  # noqa: E402,F401,F403
