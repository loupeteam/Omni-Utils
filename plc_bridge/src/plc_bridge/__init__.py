"""
Vendor-neutral PLC bridge: the driver contract and the polling runtime.
No Omniverse dependency.

Copyright (c) 2024 Loupe, https://loupe.team
Part of Omni-Utils, licensed under the MIT License.
"""

from .driver import PlcDriver, ReadResult
from .runtime import (
    CONNECTED,
    CONNECTING,
    DISCONNECTED,
    EVENT_CONNECTION,
    EVENT_DATA,
    EVENT_ENABLED,
    EVENT_STATUS,
    PlcRuntime,
)
from .symbols import nest, nest_symbol

__all__ = [
    "PlcDriver",
    "ReadResult",
    "PlcRuntime",
    "nest",
    "nest_symbol",
    "EVENT_DATA",
    "EVENT_STATUS",
    "EVENT_CONNECTION",
    "EVENT_ENABLED",
    "CONNECTING",
    "CONNECTED",
    "DISCONNECTED",
]
