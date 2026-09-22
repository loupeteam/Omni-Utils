"""
The vendor-neutral polling runtime.

Copyright (c) 2024 Loupe, https://loupe.team
Part of Omni-Utils, licensed under the MIT License.

Plain Python: nothing in this package may import from Omniverse or Kit.
"""

import logging
import math
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

# How long stop() waits, in total, for the workers to notice that they should stop.
JOIN_TIMEOUT_SEC = 2.0
# How long the read loop idles between checks while disabled or disconnected.
# enabled = True and reconnect() cut the wait short.
IDLE_SEC = 1.0
# An unchanged read problem is repeated at this interval, so a status display
# that expires old entries (the bridge UI drops them after 3 s) keeps showing it.
PROBLEM_REPEAT_SEC = 2.0
# The longest refresh period honoured. Bounds Event.wait (which overflows on
# huge values) and keeps a typo from parking the loop for hours.
MAX_PERIOD_SEC = 60.0


class _Run:
    """
    One start()..stop() of the worker threads. Each run has its own stop and
    wake events: a worker from an earlier run that outlived stop() (stuck in a
    driver call) still sees *its* stop set after a later start(), so it exits
    instead of polling next to the new pair.
    """

    def __init__(self, name: str, read_target, write_target):
        self.stop = threading.Event()
        self.wake = threading.Event()
        self.threads = [
            threading.Thread(target=read_target, args=(self,), name=f"{name}-read", daemon=True),
            threading.Thread(target=write_target, args=(self,), name=f"{name}-write", daemon=True),
        ]

    def wait(self, timeout: float) -> bool:
        """
        Sleep up to `timeout` seconds; return early when woken or stopped.

        Returns:
            True when the run should end.
        """
        if timeout > 0:
            self.wake.wait(timeout)
        self.wake.clear()
        return self.stop.is_set()


class PlcRuntime:
    """
    Polls one PLC through a PlcDriver and reports what it reads.

    A read thread connects, reads the variable list every `refresh_ms` and emits
    the result; a write thread flushes queued writes. Both are daemons, and
    stop() returns within JOIN_TIMEOUT_SEC whether or not they have.

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
        self._refresh_ms = refresh_ms
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
        # The last read problem reported (an exception text, or the failed
        # symbols), so a status is emitted when it changes rather than on every
        # scan, and "Reading OK" once when it clears.
        self._read_problem = None
        self._read_problem_at = 0.0
        # Set by the write thread when a write failed and the driver says the
        # transport is gone; the read thread verifies and acts on it.
        self._transport_suspect = False

        self._run = None
        # Held for the whole of start() and stop(), so a start() during a stop()
        # that is waiting on a stuck worker waits for it to finish instead of
        # having its fresh connection closed by that stop(). Re-entrant, so a
        # listener that calls start() or stop() from inside one does not
        # deadlock. stop() itself emits nothing while holding it, and the
        # worker it joins emits no DISCONNECTED on its way out (stop() reports
        # that afterwards), so a DISCONNECTED listener that restarts neither
        # deadlocks nor stalls the stop() for the join timeout.
        self._lifecycle_lock = threading.RLock()
        # Serialises the connection state changes (drop, connected) between the
        # read thread, a stop() on another thread, and a worker on its way out.
        self._connection_lock = threading.Lock()

    # region - Properties

    name = property(lambda self: self._name)
    driver = property(lambda self: self._driver)
    is_connected = property(lambda self: self._is_connected)
    is_running = property(lambda self: self._run is not None)

    @property
    def refresh_ms(self):
        """The read period in milliseconds. Setting it takes effect at once."""
        return self._refresh_ms

    @refresh_ms.setter
    def refresh_ms(self, value):
        self._refresh_ms = value
        self._wake()

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool):
        self._enabled = value
        self._emit(EVENT_ENABLED, value)
        self._wake()

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
        with self._lifecycle_lock:
            if self._run is not None:
                return
            # A new run reports its own problems from scratch.
            self._read_problem = None
            # Daemons, so a host that never reaches stop() (a fast shutdown, a
            # script that just exits) is not kept alive by these loops.
            self._run = run = _Run(self._name, self._read_loop, self._write_loop)
            for thread in run.threads:
                thread.start()

    def stop(self):
        """
        Stop the worker threads and disconnect. Returns within JOIN_TIMEOUT_SEC
        in total: a worker can be inside a driver call that takes seconds when
        the PLC has gone away, and the caller is often a UI thread. A worker
        that outlives the join exits on its own when the driver call returns.
        """
        with self._lifecycle_lock:
            run, self._run = self._run, None
            if run is None:
                return
            run.stop.set()
            run.wake.set()
            deadline = time.monotonic() + JOIN_TIMEOUT_SEC
            for thread in run.threads:
                if thread is threading.current_thread():
                    continue
                thread.join(timeout=max(0.0, deadline - time.monotonic()))
                if thread.is_alive():
                    logger.warning("%s did not stop within %.0fs; leaving it to the daemon flag",
                                   thread.name, JOIN_TIMEOUT_SEC)
            # Close the connection from here too. The read thread does it on its
            # way out, but if it is stuck inside a driver call this is what
            # unblocks it (the contract asks disconnect() to do that) and what
            # guarantees the PLC link is closed when stop() returns.
            # disconnect() is idempotent.
            report = self._close_connection()
        # Outside the lock: a DISCONNECTED listener may call start() or stop().
        if report:
            self._emit(EVENT_CONNECTION, DISCONNECTED)

    def reconnect(self):
        """Drop the connection and open it again on the next scan, e.g. after the address changed."""
        self._reconnect = True
        self._wake()

    def _wake(self):
        run = self._run
        if run is not None:
            run.wake.set()

    # endregion
    # region - Scans
    # One iteration of each loop. Public so that tests, and hosts that bring
    # their own scheduling, can drive the runtime without threads.

    def scan_read(self) -> bool:
        """
        Connect or disconnect as `enabled` asks, then read once and emit.

        Returns:
            True when the runtime is active (connected and enabled), False when
            idle, so a scheduler can slow down.
        """
        return self._scan_read(None)

    def scan_write(self) -> bool:
        """
        Flush the queued writes in one request.

        Returns:
            True when something was written.
        """
        return self._scan_write(None)

    def _superseded(self, run) -> bool:
        """
        True for a worker of a run that stop() has ended while it was inside a
        driver call. Such a worker must not report anything or touch the
        connection: a later start() may own both by now.
        """
        return run is not None and run is not self._run

    def _scan_read(self, run) -> bool:
        # Read-and-clear in one step: a reconnect() that lands during the scan is
        # kept for the next one instead of being cleared unseen.
        wanted, self._reconnect = self._reconnect, False
        if wanted and self._is_connected:
            self._drop_connection()
        # The write thread saw the transport go: ask the driver again from here
        # before acting, since the link may have been reopened since.
        suspect, self._transport_suspect = self._transport_suspect, False
        if suspect and self._is_connected:
            self._check_transport()

        if self._enabled and not self._is_connected:
            self._emit(EVENT_CONNECTION, CONNECTING)
            try:
                # Close anything left from a previous connection first
                with self._connection_lock:
                    self._driver.disconnect()
                self._driver.connect()
            except Exception as e:
                if self._superseded(run):
                    return False
                self._is_connected = False
                self._emit(EVENT_STATUS, f"Error Connecting: {e}")
            else:
                # The check and the state change are one step under the lock: a
                # stop() that gives up on us between them would report nothing,
                # and this CONNECTED would never get its DISCONNECTED.
                with self._connection_lock:
                    if self._superseded(run):
                        return False
                    self._is_connected = True
                    self._was_connected = True
                self._transport_suspect = False
                self._emit(EVENT_CONNECTION, CONNECTED)

        if not self._enabled and self._is_connected:
            self._drop_connection()

        if not self._is_connected or not self._enabled:
            return False

        symbols = self.read_variables
        if not symbols:
            return True  # nothing to ask for; no round trip

        try:
            result = self._driver.read(symbols)
            data = nest(result.values, self._driver.symbol_separators) if result.values else None
        except Exception as e:
            if self._superseded(run):
                return False
            self._set_read_problem(f"Error Reading: {e}")
            self._check_transport()
            return True
        if self._superseded(run):
            return False

        if data:
            self._emit(EVENT_DATA, data)
            if self._superseded(run):  # a data listener may have called stop()
                return False
        if result.errors:
            detail = "; ".join(f"{name}: {text}" for name, text in sorted(result.errors.items()))
            if data:
                self._set_read_problem("Error Reading: " + detail)
            else:
                # Every symbol failed: the PLC has no program, or has gone away
                self._set_read_problem(
                    "Error Reading: all {} symbol(s) failed: {}".format(len(result.errors), detail))
        else:
            self._set_read_problem(None)
        return True

    def _scan_write(self, run) -> bool:
        if not self._is_connected or not self._write_queue:
            return False
        with self._write_lock:
            values, self._write_queue = self._write_queue, {}
        try:
            errors = self._driver.write(values) or {}
        except Exception as e:
            if self._superseded(run):
                return False
            self._emit(EVENT_STATUS, f"Error Writing: {e}")
            # The write thread never touches the connection itself: it flags the
            # transport for the read thread, which checks again and reconnects
            # only if the link really is gone (it may have been reopened by then).
            if not self._transport_alive():
                self._transport_suspect = True
                self._wake()
            return False
        if self._superseded(run):
            return False
        if errors:
            self._emit(EVENT_STATUS, "Error Writing: " + "; ".join(
                f"{name}: {text}" for name, text in sorted(errors.items())))
        return True

    def _close_connection(self) -> bool:
        """
        Close the connection and clear the state. Under the lock: stop() on
        another thread and a worker on its exit path can both get here, and
        DISCONNECTED must be reported exactly once.

        Returns:
            True when the caller has to emit DISCONNECTED (always outside a lock).
        """
        with self._connection_lock:
            # Clear the flag first: the write thread checks it before using the
            # connection that disconnect() is about to close.
            self._is_connected = False
            self._driver.disconnect()
            report, self._was_connected = self._was_connected, False
        return report

    def _drop_connection(self):
        # Report it here, not on the next scan: that scan may reconnect first
        # (enabled and not connected), and DISCONNECTED would never be seen.
        if self._close_connection():
            self._emit(EVENT_CONNECTION, DISCONNECTED)

    def _transport_alive(self) -> bool:
        try:
            return bool(self._driver.is_connected())
        except Exception:
            return False

    def _check_transport(self):
        """
        After a failed read, ask the driver whether the connection is still
        there. If not, drop it: the next scan reports DISCONNECTED and
        reconnects instead of failing every scan until someone toggles `enabled`.
        Read thread only.
        """
        if not self._transport_alive():
            self._drop_connection()

    def _set_read_problem(self, text):
        """
        Emit a read problem when it changes, then again every PROBLEM_REPEAT_SEC
        while it persists, and "Reading OK" once when it clears. A problem that
        repeats every scan (a symbol the PLC rejects, a name the parser cannot
        index) would otherwise flood the listeners at the scan rate; never
        repeating it would let a status display that expires entries go blank
        while the problem is still there.
        """
        now = time.monotonic()
        if text == self._read_problem:
            if text is None or now - self._read_problem_at < PROBLEM_REPEAT_SEC:
                return
        self._read_problem = text
        self._read_problem_at = now
        self._emit(EVENT_STATUS, text if text else "Reading OK")

    # endregion
    # region - Worker threads

    def _read_loop(self, run: _Run):
        try:
            self._read_loop_body(run)
        finally:
            self._read_loop_exit()

    def _read_loop_body(self, run: _Run):
        next_scan = time.monotonic()
        while not run.stop.is_set():
            # Nothing in here may end the thread: a bad refresh_ms (None from an
            # unset prim attribute, inf, nan, a string) is logged and treated as
            # idle, and the loop keeps going so that enabled / stop() still work.
            try:
                # Hold the refresh period from scan start to scan start. When a
                # scan overran, start the next one now rather than catching up.
                now = time.monotonic()
                next_scan = max(next_scan, now)
                if run.wait(min(next_scan - now, MAX_PERIOD_SEC)):
                    break
                # An early wake (enabled, reconnect, refresh_ms) scans now, so
                # the next scan is one period from here, not from the old
                # target: a lowered refresh rate or an enable takes effect at
                # once instead of after the rest of the old period. An on-time
                # wake keeps the fixed cadence.
                next_scan = min(next_scan, time.monotonic()) + self._period()
                active = self._scan_read(run)
            except Exception:
                logger.exception("%s: read scan failed", self._name)
                active = False
            if not active:
                next_scan = time.monotonic() + IDLE_SEC

    def _period(self) -> float:
        """The refresh period in seconds, validated: finite, not negative, capped."""
        period = float(self.refresh_ms) / 1000
        if not math.isfinite(period) or period < 0:
            raise ValueError(f"refresh_ms must be a finite, non-negative number, got {self.refresh_ms!r}")
        return min(period, MAX_PERIOD_SEC)

    def _read_loop_exit(self):
        # Close the connection on the way out, but do not report it: stop() does
        # that after its join, outside the lifecycle lock, so a DISCONNECTED
        # listener may call start() without waiting on the joiner. The check and
        # the close are one step under the lock: a worker that outlived its
        # stop() must not close what a later start() opened, and a new run can
        # only own a connection once _run is set.
        with self._connection_lock:
            if self._run is None:
                self._is_connected = False
                try:
                    self._driver.disconnect()
                except Exception:
                    logger.exception("%s: disconnect on exit failed", self._name)

    def _write_loop(self, run: _Run):
        while not run.stop.wait(self.write_sleep):
            try:
                self._scan_write(run)
            except Exception:
                logger.exception("%s: write scan failed", self._name)

    # endregion
