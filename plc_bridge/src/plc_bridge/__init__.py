"""
Vendor-neutral PLC bridge: the driver contract and the polling runtime.
No Omniverse dependency.

Copyright (c) 2024 Loupe, https://loupe.team
Part of Omni-Utils, licensed under the MIT License.
"""

from .driver import (
    ERROR_CODES,
    ERROR_MISSING,
    ERROR_OTHER,
    ERROR_REJECTED,
    PlcDriver,
    ReadResult,
    SymbolError,
    symbol_error,
)
from .runtime import (
    CONNECTED,
    CONNECTING,
    DISCONNECTED,
    EVENT_CONNECTION,
    EVENT_DATA,
    EVENT_ENABLED,
    EVENT_PROBLEM,
    EVENT_SAMPLE,
    EVENT_STATUS,
    EVENT_WRITE,
    PROBLEM_CONNECT,
    PROBLEM_OK,
    PROBLEM_READ,
    PROBLEM_WRITE,
    PlcRuntime,
    Problem,
    Sample,
    WRITE_NOT_CONNECTED,
    WRITE_STOPPED,
    WRITE_SUPERSEDED,
    WriteHandle,
    WriteResult,
    freeze,
    thaw,
)
from .symbols import nest, nest_symbol

__all__ = [
    "PlcDriver",
    "ReadResult",
    "SymbolError",
    "symbol_error",
    "ERROR_CODES",
    "ERROR_MISSING",
    "ERROR_REJECTED",
    "ERROR_OTHER",
    "PlcRuntime",
    "Sample",
    "Problem",
    "WriteHandle",
    "WriteResult",
    "WRITE_NOT_CONNECTED",
    "WRITE_STOPPED",
    "WRITE_SUPERSEDED",
    "freeze",
    "thaw",
    "nest",
    "nest_symbol",
    "EVENT_SAMPLE",
    "EVENT_DATA",
    "EVENT_PROBLEM",
    "EVENT_STATUS",
    "EVENT_CONNECTION",
    "EVENT_ENABLED",
    "EVENT_WRITE",
    "PROBLEM_CONNECT",
    "PROBLEM_READ",
    "PROBLEM_WRITE",
    "PROBLEM_OK",
    "CONNECTING",
    "CONNECTED",
    "DISCONNECTED",
]
