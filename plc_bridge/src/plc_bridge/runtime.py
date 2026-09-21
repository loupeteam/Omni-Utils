"""
The vendor-neutral polling runtime.

Copyright (c) 2024 Loupe, https://loupe.team
Part of Omni-Utils, licensed under the MIT License.

Plain Python: nothing in this package may import from Omniverse or Kit.
"""

import logging
import threading
import time
from typing import Any, Callable, Iterable

from .driver import PlcDriver
from .symbols import nest

logger = logging.getLogger(__name__)

# Event kinds delivered to listeners.
EVENT_DATA = "data"              # payload: nested dict of the values read
EVENT_STATUS = "status"          # payload: str, human readable
EVENT_CONNECTION = "connection"  # payload: CONNECTING | CONNECTED | DISCONNECTED
EVENT_ENABLED = "enabled"        # payload: bool

CONNECTING = "Connecting"
CONNECTED = "Connected"
DISCONNECTED = "Disconnected"

# How long stop() waits for a worker to notice that it should stop.
JOIN_TIMEOUT_SEC = 2.0
# How long the read loop idles between checks while disabled or disconnected.
IDLE_SEC = 1.0


class PlcRuntime:
    """
    Polls one PLC through a PlcDriver and reports what it reads.

    A read thread connects, reads the variable list every `refresh_ms` and emits
    the result; a write thread flushes queued writes. Both are daemons and both
    stop within JOIN_TIMEOUT_SEC of stop().

    Listeners are called **on the worker threads**. A host with a main thread
    (a UI, Omniverse Kit) has to marshal to it itself. A listener that raises is
    logged and does not disturb the polling.

        plc = PlcRuntime(AdsDriver("10.20.30.40.1.1"), refresh_ms=20, enabled=True)
        plc.set_read_variables(["GVL.Axes[0].ActualPosition"])
        plc.on_data(print)
        plc.start()
        ...
        plc.stop()
    """

    def __init__(self, driver: PlcDriver, name: str = "PLC1", refresh_ms: float = 20,
                 enabled: bool = False, write_sleep: float = 0.001):
        self._driver = driver
        self._name = name
        self.refresh_ms = refresh_ms
        self.write_sleep = write_sleep
        self._enabled = enabled

        self._read_variables = []
        self._variables_lock = threading.RLock()

        self._write_queue = {}
        self._write_lock = threading.RLock()

        self._listeners = {}
        self._listeners_lock = threading.RLock()

        self._is_connected = False
        self._was_connected = False
        self._reconnect = False
        # Symbols the last read reported as failed, so the status is emitted when
        # the set changes rather than on every scan.
        self._failed_symbols = frozenset()

        self._stop = threading.Event()
        self._stop.set()
        self._threads = []

    # region - Properties

    name = property(lambda self: self._name)
    driver = property(lambda self: self._driver)
    is_connected = property(lambda self: self._is_connected)
    is_running = property(lambda self: not self._stop.is_set())

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool):
        self._enabled = value
        self._emit(EVENT_ENABLED, value)

    # endregion
    # region - Read list

    @property
    def read_variables(self) -> list:
        """The symbols read every scan, in order. A copy."""
        with self._variables_lock:
            return list(self._read_variables)

    def add_read_variables(self, names: Iterable[str]):
        """
        Add symbols to the cyclic read list. Blank entries are dropped and
        whitespace is stripped (a Windows multiline field leaves a '\\r' behind,
        and a PLC reports a padded name as not found).
        """
        with self._variables_lock:
            for name in names:
                name = name.strip()
                if name and name not in self._read_variables:
                    self._read_variables.append(name)

    def set_read_variables(self, names: Iterable[str]):
        """Replace the cyclic read list. Same cleaning as add_read_variables."""
        with self._variables_lock:
            self._read_variables = []
            self.add_read_variables(names)

    # endregion
    # region - Listeners

    def add_listener(self, kind: str, callback: Callable[[Any], None]) -> Callable[[], None]:
        """
        Call callback(payload) for every event of this kind.

        Returns:
            A function that removes the listener again.
        """
        with self._listeners_lock:
            self._listeners.setdefault(kind, []).append(callback)

        def remove():
            with self._listeners_lock:
                callbacks = self._listeners.get(kind, [])
                if callback in callbacks:
                    callbacks.remove(callback)

        return remove

    def on_data(self, callback):
        """callback(data: dict), the nested values of one read."""
        return self.add_listener(EVENT_DATA, callback)

    def on_status(self, callback):
        """callback(text: str), errors and recoveries in words."""
        return self.add_listener(EVENT_STATUS, callback)

    def on_connection(self, callback):
        """callback(state: str), one of CONNECTING, CONNECTED, DISCONNECTED."""
        return self.add_listener(EVENT_CONNECTION, callback)

    def on_enabled(self, callback):
        """callback(enabled: bool), when `enabled` is set."""
        return self.add_listener(EVENT_ENABLED, callback)

    def _emit(self, kind: str, payload):
        with self._listeners_lock:
            callbacks = list(self._listeners.get(kind, ()))
        for callback in callbacks:
            try:
                callback(payload)
            except Exception:
                logger.exception("%s: a '%s' listener raised", self._name, kind)

    # endregion
    # region - Writes

    def queue_write(self, name: str, value: Any):
        """Queue one write. A later value for the same symbol replaces a pending one."""
        with self._write_lock:
            self._write_queue[name] = value

    # endregion
    # region - Lifecycle

    def start(self):
        """Start the worker threads. Does nothing when already running."""
        if self.is_running:
            return
        self._stop.clear()
        # Daemons, so a host that never reaches stop() (a fast shutdown, a script
        # that just exits) is not kept alive by these loops.
        self._threads = [
            threading.Thread(target=self._read_loop, name=f"{self._name}-read", daemon=True),
            threading.Thread(target=self._write_loop, name=f"{self._name}-write", daemon=True),
        ]
        for thread in self._threads:
            thread.start()

    def stop(self):
        """
        Stop the worker threads and disconnect. The join is bounded: a worker can
        be inside a read that takes seconds when the PLC has gone away, and the
        caller is often a UI thread.
        """
        self._stop.set()
        for thread in self._threads:
            if thread is threading.current_thread():
                continue
            thread.join(timeout=JOIN_TIMEOUT_SEC)
            if thread.is_alive():
                logger.warning("%s did not stop within %.0fs; leaving it to the daemon flag",
                               thread.name, JOIN_TIMEOUT_SEC)
        self._threads = []

    def reconnect(self):
        """Drop the connection and open it again on the next scan, e.g. after the address changed."""
        self._reconnect = True

    # endregion
    # region - Scans
    # One iteration of each loop. Public so that tests, and hosts that bring
    # their own scheduling, can drive the runtime without threads.

    def scan_read(self) -> bool:
        """
        Connect or disconnect as `enabled` asks, then read once and emit.

        Returns:
            True when a read was attempted, False when idle (disabled or not connected).
        """
        if self._reconnect and self._is_connected:
            self._reconnect = False
            self._is_connected = False
            self._driver.disconnect()
        self._reconnect = False

        if self._enabled and not self._is_connected:
            self._emit(EVENT_CONNECTION, CONNECTING)
            try:
                # Close anything left from a previous connection first
                self._driver.disconnect()
                self._driver.connect()
            except Exception as e:
                self._is_connected = False
                self._emit(EVENT_STATUS, f"Error Connecting: {e}")
            else:
                self._is_connected = True
                self._emit(EVENT_CONNECTION, CONNECTED)

        if not self._enabled and self._is_connected:
            # Clear the flag first: the write thread checks it before using the
            # connection that disconnect() is about to close.
            self._is_connected = False
            self._driver.disconnect()

        if not self._is_connected and self._was_connected:
            self._emit(EVENT_CONNECTION, DISCONNECTED)
        self._was_connected = self._is_connected

        if not self._is_connected or not self._enabled:
            return False

        try:
            result = self._driver.read(self.read_variables)
        except Exception as e:
            self._emit(EVENT_STATUS, f"Error Reading: {e}")
            return True

        if result.values:
            self._emit(EVENT_DATA, nest(result.values, self._driver.symbol_separators))
        elif result.errors:
            # Every symbol failed: the PLC has no program, or has gone away
            self._emit(EVENT_STATUS, "Error Reading: all {} symbol(s) failed".format(len(result.errors)))
        self._report_failed_symbols(result.errors)
        return True

    def scan_write(self) -> bool:
        """
        Flush the queued writes in one request.

        Returns:
            True when something was written.
        """
        if not self._is_connected or not self._write_queue:
            return False
        with self._write_lock:
            values, self._write_queue = self._write_queue, {}
        try:
            self._driver.write(values)
        except Exception as e:
            self._emit(EVENT_STATUS, f"Error Writing: {e}")
            return False
        return True

    def _report_failed_symbols(self, errors: dict):
        """Emit once when symbols start failing and once when they recover."""
        failed = frozenset(errors)
        if failed == self._failed_symbols:
            return
        if failed:
            self._emit(EVENT_STATUS, "Error Reading: " + "; ".join(
                f"{name}: {text}" for name, text in sorted(errors.items())))
        else:
            self._emit(EVENT_STATUS, "Reading OK")
        self._failed_symbols = failed

    # endregion
    # region - Worker threads

    def _read_loop(self):
        next_scan = time.monotonic()
        while not self._stop.is_set():
            # Hold the refresh period from scan start to scan start. When a scan
            # overran, start the next one now rather than trying to catch up.
            now = time.monotonic()
            next_scan = max(next_scan, now)
            if self._stop.wait(next_scan - now):
                break
            next_scan += self.refresh_ms / 1000

            try:
                active = self.scan_read()
            except Exception:
                logger.exception("%s: read scan failed", self._name)
                active = False
            if not active:
                next_scan = time.monotonic() + IDLE_SEC

        self._is_connected = False
        self._driver.disconnect()

    def _write_loop(self):
        while not self._stop.wait(self.write_sleep):
            try:
                self.scan_write()
            except Exception:
                logger.exception("%s: write scan failed", self._name)

    # endregion
