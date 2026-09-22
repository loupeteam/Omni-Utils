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
      async transport owns its event loop (on a thread of its own) and hides
      it, e.g. `asyncio.run_coroutine_threadsafe(coro, loop).result(timeout)`.
    * **Bounded.** Every method returns or raises within a timeout the driver
      chooses, a few seconds at most. The runtime's stop() only waits
      JOIN_TIMEOUT_SEC for a call in flight; a call that can block longer (a
      websocket `recv` on a half-open connection can wait for the ping timeout)
      must carry its own timeout. `disconnect()` must also unblock a `read` or
      `write` that is in flight on another thread.
    * **Two caller threads.** The runtime calls `read` from its read thread and
      `write` from its write thread, possibly at the same moment. `connect` and
      `disconnect` are called from the read thread, and `disconnect` also from
      whichever thread calls stop(). The driver makes that safe itself
      (separate connections, a lock, whatever suits the transport).
    * **Stateless about the read list.** The symbols arrive with each `read`;
      the runtime never calls `read` with an empty list.
    * **Failures.** A symbol the PLC rejects goes into `ReadResult.errors` or
      the dict returned by `write`. A failure of the connection or the whole
      request raises; the runtime reports it, asks `is_connected()`, and either
      keeps polling or reconnects.
    """

    #: Characters that separate the parts of a symbol name for this vendor.
    #: Beckhoff "GVL.struct.member" -> "."; B&R "Program:struct.member" -> ":.".
    #: Must not be empty.
    symbol_separators: str = "."

    @abstractmethod
    def connect(self) -> None:
        """Open the connection. Raises when the PLC cannot be reached."""

    @abstractmethod
    def disconnect(self) -> None:
        """
        Close the connection and unblock any call in flight. Safe to call when
        not connected, and from a thread other than the one reading; never raises.
        """

    @abstractmethod
    def is_connected(self) -> bool:
        """
        True while the transport is open. Must reflect a connection the peer
        dropped (the runtime asks after a failed read or write to decide
        whether to reconnect), be cheap, and be safe to call from any thread.
        """

    @abstractmethod
    def read(self, symbols: Sequence[str]) -> ReadResult:
        """Read the symbols in one request. Never called with an empty list."""

    @abstractmethod
    def write(self, values: Mapping[str, Any]) -> Mapping[str, str]:
        """
        Write flat symbol name -> value in one request.

        Returns:
            symbol name -> reason for each symbol the PLC rejected; empty (or
            None) when every write succeeded. Raise only when the whole request
            failed.
        """
