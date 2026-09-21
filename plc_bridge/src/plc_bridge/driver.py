"""
The contract a PLC vendor driver implements.

Copyright (c) 2024 Loupe, https://loupe.team
Part of Omni-Utils, licensed under the MIT License.

Plain Python: nothing in this package may import from Omniverse or Kit.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence


@dataclass
class ReadResult:
    """
    The outcome of one PlcDriver.read call.

    Attributes:
        values: flat symbol name -> value, exactly as the symbols were requested
            ("GVL.Axes[0].Pos", "Program:struct.member"). The runtime does the
            nesting, so every vendor's data has the same shape.
        errors: symbol name -> reason, for symbols the PLC rejected. A symbol is
            in one of the two dicts, never both. An error is never delivered as
            a value.
    """

    values: dict = field(default_factory=dict)
    errors: dict = field(default_factory=dict)


class PlcDriver(ABC):
    """
    One connection to one PLC.

    Rules every implementation follows, so that drivers can be swapped:

    * **Synchronous.** Every method blocks until done. A driver built on an
      async transport owns its event loop and hides it.
    * **Two caller threads.** The runtime calls `read` from its read thread and
      `write` from its write thread, possibly at the same moment. `connect` and
      `disconnect` are only called from the read thread. The driver makes that
      safe itself (separate connections, a lock, whatever suits the transport).
    * **Stateless about the read list.** The symbols arrive with each `read`.
    * **Failures.** A symbol the PLC rejects goes into `ReadResult.errors`. A
      failure of the connection or the whole request raises; the runtime reports
      it and keeps polling.
    """

    #: Characters that separate the parts of a symbol name for this vendor.
    #: Beckhoff "GVL.struct.member" -> "."; B&R "Program:struct.member" -> ":.".
    symbol_separators: str = "."

    @abstractmethod
    def connect(self) -> None:
        """Open the connection. Raises when the PLC cannot be reached."""

    @abstractmethod
    def disconnect(self) -> None:
        """Close the connection. Safe to call when not connected; never raises."""

    @abstractmethod
    def is_connected(self) -> bool:
        """True while the connection is open."""

    @abstractmethod
    def read(self, symbols: Sequence[str]) -> ReadResult:
        """Read the symbols in one request. An empty list returns an empty result."""

    @abstractmethod
    def write(self, values: Mapping[str, Any]) -> None:
        """Write flat symbol name -> value in one request."""
