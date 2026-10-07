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
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any, Callable, Iterable, Optional

from .driver import PlcDriver
from .symbols import nest

logger = logging.getLogger(__name__)

# Event kinds delivered to listeners.
EVENT_SAMPLE = "sample"          # payload: Sample, one per successful read
EVENT_DATA = "data"              # payload: dict, Sample.nested (0.3 convenience)
EVENT_PROBLEM = "problem"        # payload: Problem, when a problem starts, persists, or clears
EVENT_STATUS = "status"          # payload: str, Problem.text (0.3 convenience)
EVENT_CONNECTION = "connection"  # payload: CONNECTING | CONNECTED | DISCONNECTED
EVENT_ENABLED = "enabled"        # payload: bool
EVENT_WRITE = "write"            # payload: WriteResult, one per flushed batch

CONNECTING = "Connecting"
CONNECTED = "Connected"
DISCONNECTED = "Disconnected"

# Problem kinds
PROBLEM_CONNECT = "connect"
PROBLEM_READ = "read"
PROBLEM_WRITE = "write"
PROBLEM_OK = "ok"                # a read problem cleared

# How long stop() waits for the worker to notice that it should stop.
JOIN_TIMEOUT_SEC = 2.0
# How long the loop idles between checks while disabled or disconnected.
# enabled = True, reconnect() and queue_write() cut the wait short.
IDLE_SEC = 1.0
# An unchanged problem is repeated at this interval, so a status display that
# expires old entries (the bridge UI drops them after 3 s) keeps showing it.
PROBLEM_REPEAT_SEC = 2.0
# The longest refresh period honoured. Bounds Event.wait (which overflows on
# huge values) and keeps a typo from parking the loop for hours.
MAX_PERIOD_SEC = 60.0


@dataclass(frozen=True)
class Sample:
    """
    One successful read.

    Attributes:
        seq: counts up by one per sample for the life of the runtime, so a
            consumer that polls `latest()` can tell a new sample from the one
            it already handled.
        t: time.monotonic() when the read returned.
        values: flat symbol name -> value, as the driver returned them.
        errors: symbol name -> reason for the symbols the PLC rejected in the
            same read. A symbol is in one of the two dicts, never both.
        separators: the driver's symbol separators, for `nested`.
    """

    seq: int
    t: float
    values: dict
    errors: dict
    separators: str = "."

    @cached_property
    def nested(self) -> dict:
        """The values as nested dicts and lists ("GVL.Axes[0].Pos" -> data["GVL"]["Axes"][0]["Pos"])."""
        return nest(self.values, self.separators)


@dataclass(frozen=True)
class Problem:
    """
    Something the runtime wants a person to know about, in a form a program can
    act on too.

    Attributes:
        kind: PROBLEM_CONNECT, PROBLEM_READ, PROBLEM_WRITE, or PROBLEM_OK when a
            read problem has cleared.
        text: the human readable form ("Error Reading: GVL.x: symbol not found").
        symbols: the symbols involved, when known.
    """

    kind: str
    text: str
    symbols: tuple = ()


class WriteHandle:
    """
    The fate of one queued write. `wait()` blocks until the write was sent (or
    failed); `error` is None on success, otherwise the reason. A later write of
    the same symbol replaces a pending one, and the replaced handle is resolved
    with error "superseded".
    """

    def __init__(self, name: str, value: Any):
        self.name = name
        self.value = value
        self.error: Optional[str] = None
        self._done = threading.Event()

    def _resolve(self, error: Optional[str]):
        self.error = error
        self._done.set()

    @property
    def done(self) -> bool:
        return self._done.is_set()

    @property
    def ok(self) -> bool:
        return self._done.is_set() and self.error is None

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Block until resolved. Returns False on timeout."""
        return self._done.wait(timeout)

    def __repr__(self):
        state = "pending" if not self.done else ("ok" if self.ok else f"error={self.error!r}")
        return f"WriteHandle({self.name!r}, {self.value!r}, {state})"


@dataclass(frozen=True)
class WriteResult:
    """
    One flushed write batch.

    Attributes:
        values: what was sent.
        errors: symbol -> reason for the symbols the PLC rejected.
        error: the reason when the whole request failed, else None.
    """

    values: dict
    errors: dict = field(default_factory=dict)
    error: Optional[str] = None


class _Run:
    """
    One start()..stop() of the worker thread. Each run has its own stop and
    wake events: a worker from an earlier run that outlived stop() (stuck in a
    driver call) still sees *its* stop set after a later start(), so it exits
    instead of polling next to the new one.
    """

    def __init__(self, name: str, target):
        self.stop = threading.Event()
        self.wake = threading.Event()
        self.thread = threading.Thread(target=target, args=(self,), name=f"{name}-plc", daemon=True)

    def wait(self, timeout: float) -> bool:
        """
        Sleep up to `timeout` seconds; return early when woken or stopped.

        Returns:
            True when woken (or stopped) before the timeout ran out, False when
            the timeout ran out. Check `stop` for which.
        """
        woken = self.wake.wait(timeout) if timeout > 0 else self.wake.is_set()
        self.wake.clear()
        return woken


class PlcRuntime:
    """
    Polls one PLC through a PlcDriver and reports what it reads.

    One worker thread per PLC: every `refresh_ms` it flushes the queued writes
    and then reads the variable list, in that order, so the sample that follows
    a write reflects it. The thread is a daemon, and stop() returns within
    JOIN_TIMEOUT_SEC whether or not it has.

    Listeners are called **on the worker thread**. A host with a main thread
    (a UI, Omniverse Kit) has to marshal to it itself, or poll `latest()` from
    its own loop. A listener that raises is logged and does not disturb the
    polling.

        plc = PlcRuntime(AdsDriver("10.20.30.40.1.1"), refresh_ms=20, enabled=True)
        plc.set_read_variables(["GVL.Axes[0].ActualPosition"])
        plc.on_sample(lambda s: print(s.seq, s.values))
        plc.start()
        ...
        handle = plc.queue_write("GVL.Command.Blend", 1.0)
        handle.wait(1.0); print(handle.ok)
        ...
        plc.stop()
    """

    def __init__(self, driver: PlcDriver, name: str = "PLC1", refresh_ms: float = 20,
                 enabled: bool = False, write_sleep: float = 0.001):
        self._driver = driver
        self._name = name
        self._refresh_ms = refresh_ms
        # 0.3.0.dev kept a write thread with its own sleep; writes now go out on
        # the scan that follows queue_write(). Accepted and ignored.
        self.write_sleep = write_sleep
        self._enabled = enabled

        self._read_variables = []
        self._variables_lock = threading.RLock()

        self._write_queue = {}       # symbol -> (value, WriteHandle)
        self._write_lock = threading.RLock()

        self._listeners = {}
        self._listeners_lock = threading.RLock()

        self._latest: Optional[Sample] = None
        self._seq = 0

        self._is_connected = False
        self._was_connected = False
        self._reconnect = False
        # Problems reported so far, by slot (connect / read), so each is emitted
        # when it changes, repeated every PROBLEM_REPEAT_SEC while it persists,
        # and (for reads) cleared with one OK.
        self._problems = {}
        self._problems_at = {}

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
        # worker, a stop() on another thread, and a worker on its way out.
        self._connection_lock = threading.Lock()

    # region - Properties

    name = property(lambda self: self._name)
    driver = property(lambda self: self._driver)
    is_connected = property(lambda self: self._is_connected)
    is_running = property(lambda self: self._run is not None)

    @property
    def refresh_ms(self):
        """The scan period in milliseconds. Setting it takes effect at once."""
        return self._refresh_ms

    @refresh_ms.setter
    def refresh_ms(self, value):
        changed = value != self._refresh_ms
        self._refresh_ms = value
        if changed:
            self._wake()

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool):
        # The event goes out on every assignment (hosts re-apply their options
        # and listen for it); the loop is woken only when the value changed,
        # so a host that re-applies options at a high rate does not turn the
        # refresh period into that rate.
        changed = value != self._enabled
        self._enabled = value
        self._emit(EVENT_ENABLED, value)
        if changed:
            self._wake()

    def latest(self) -> Optional[Sample]:
        """The newest sample, or None before the first read. Safe from any thread."""
        return self._latest

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

    def on_sample(self, callback):
        """callback(sample: Sample), once per successful read."""
        return self.add_listener(EVENT_SAMPLE, callback)

    def on_data(self, callback):
        """callback(data: dict), the nested values of one read (Sample.nested)."""
        return self.add_listener(EVENT_DATA, callback)

    def on_problem(self, callback):
        """callback(problem: Problem), when a problem starts, persists, or clears."""
        return self.add_listener(EVENT_PROBLEM, callback)

    def on_status(self, callback):
        """callback(text: str), Problem.text for every problem event."""
        return self.add_listener(EVENT_STATUS, callback)

    def on_connection(self, callback):
        """callback(state: str), one of CONNECTING, CONNECTED, DISCONNECTED."""
        return self.add_listener(EVENT_CONNECTION, callback)

    def on_enabled(self, callback):
        """callback(enabled: bool), when `enabled` is set."""
        return self.add_listener(EVENT_ENABLED, callback)

    def on_write(self, callback):
        """callback(result: WriteResult), once per flushed write batch."""
        return self.add_listener(EVENT_WRITE, callback)

    def _emit(self, kind: str, payload):
        with self._listeners_lock:
            callbacks = list(self._listeners.get(kind, ()))
        for callback in callbacks:
            try:
                callback(payload)
            except Exception:
                logger.exception("%s: a '%s' listener raised", self._name, kind)

    def _emit_problem(self, problem: Problem):
        self._emit(EVENT_PROBLEM, problem)
        self._emit(EVENT_STATUS, problem.text)

    # endregion
    # region - Writes

    def queue_write(self, name: str, value: Any) -> WriteHandle:
        """
        Queue one write; it goes out on the next scan, before that scan's read.
        A later value for the same symbol replaces a pending one (whose handle
        is resolved with error "superseded").

        Returns:
            A WriteHandle to wait on or inspect. Ignore it if you do not care.
        """
        handle = WriteHandle(name, value)
        with self._write_lock:
            previous = self._write_queue.get(name)
            self._write_queue[name] = (value, handle)
        if previous is not None:
            previous[1]._resolve("superseded")
        self._wake()
        return handle

    # endregion
    # region - Lifecycle

    def start(self):
        """Start the worker thread. Does nothing when already running."""
        with self._lifecycle_lock:
            if self._run is not None:
                return
            # A new run reports its own problems from scratch.
            self._problems = {}
            self._problems_at = {}
            # A daemon, so a host that never reaches stop() (a fast shutdown, a
            # script that just exits) is not kept alive by the loop.
            self._run = run = _Run(self._name, self._loop)
            run.thread.start()

    def stop(self):
        """
        Stop the worker thread and disconnect. Returns within JOIN_TIMEOUT_SEC:
        the worker can be inside a driver call that takes seconds when the PLC
        has gone away, and the caller is often a UI thread. A worker that
        outlives the join exits on its own when the driver call returns.
        Pending writes are resolved with error "stopped".
        """
        with self._lifecycle_lock:
            run, self._run = self._run, None
            if run is None:
                return
            run.stop.set()
            run.wake.set()
            if run.thread is not threading.current_thread():
                run.thread.join(timeout=JOIN_TIMEOUT_SEC)
                if run.thread.is_alive():
                    logger.warning("%s did not stop within %.0fs; leaving it to the daemon flag",
                                   run.thread.name, JOIN_TIMEOUT_SEC)
            # Close the connection from here too. The worker does it on its way
            # out, but if it is stuck inside a driver call this is what unblocks
            # it (the contract asks disconnect() to do that) and what guarantees
            # the PLC link is closed when stop() returns. disconnect() is idempotent.
            report = self._close_connection()
            self._fail_pending_writes("stopped")
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
    # One iteration each. Public so that tests, and hosts that bring their own
    # scheduling, can drive the runtime without a thread.

    def scan(self) -> bool:
        """
        One full iteration: flush queued writes, then connect / read as
        `enabled` asks.

        Returns:
            True when the runtime is active (connected and enabled), False when
            idle, so a scheduler can slow down.
        """
        return self._scan(None)

    def scan_write(self) -> bool:
        """Flush the queued writes in one request. Returns True when something was written."""
        return self._scan_write(None)

    def scan_read(self) -> bool:
        """Connect or disconnect as `enabled` asks, then read once and emit. Returns True when active."""
        return self._scan_read(None)

    def _superseded(self, run) -> bool:
        """
        True for a worker of a run that stop() has ended while it was inside a
        driver call. Such a worker must not report anything or touch the
        connection: a later start() may own both by now.
        """
        return run is not None and run is not self._run

    def _scan(self, run) -> bool:
        # Writes first, so the read that follows reflects them.
        self._scan_write(run)
        if self._superseded(run):
            return False
        return self._scan_read(run)

    def _scan_read(self, run) -> bool:
        # Read-and-clear in one step: a reconnect() that lands during the scan is
        # kept for the next one instead of being cleared unseen.
        wanted, self._reconnect = self._reconnect, False
        if wanted and self._is_connected:
            self._drop_connection()

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
                self._report(PROBLEM_CONNECT, Problem(PROBLEM_CONNECT, f"Error Connecting: {e}"))
            else:
                # The check and the state change are one step under the lock: a
                # stop() that gives up on us between them would report nothing,
                # and this CONNECTED would never get its DISCONNECTED.
                with self._connection_lock:
                    if self._superseded(run):
                        return False
                    self._is_connected = True
                    self._was_connected = True
                self._problems.pop(PROBLEM_CONNECT, None)
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
            sample = Sample(self._seq + 1, time.monotonic(), dict(result.values),
                            dict(result.errors), self._driver.symbol_separators)
            if sample.values:
                sample.nested  # parse now: a name the parser cannot index is a read problem
        except Exception as e:
            if self._superseded(run):
                return False
            self._report(PROBLEM_READ, Problem(PROBLEM_READ, f"Error Reading: {e}"))
            self._check_transport()
            return True
        if self._superseded(run):
            return False

        if sample.values:
            self._seq = sample.seq
            self._latest = sample
            self._emit(EVENT_SAMPLE, sample)
            self._emit(EVENT_DATA, sample.nested)
            if self._superseded(run):  # a data listener may have called stop()
                return False
        if sample.errors:
            detail = "; ".join(f"{name}: {text}" for name, text in sorted(sample.errors.items()))
            symbols_failed = tuple(sorted(sample.errors))
            if sample.values:
                self._report(PROBLEM_READ, Problem(PROBLEM_READ, "Error Reading: " + detail, symbols_failed))
            else:
                # Every symbol failed: the PLC has no program, or has gone away
                self._report(PROBLEM_READ, Problem(
                    PROBLEM_READ,
                    "Error Reading: all {} symbol(s) failed: {}".format(len(sample.errors), detail),
                    symbols_failed))
        else:
            self._report(PROBLEM_READ, None)
        return True

    def _scan_write(self, run) -> bool:
        if not self._is_connected or not self._write_queue:
            return False
        with self._write_lock:
            queued, self._write_queue = self._write_queue, {}
        values = {name: value for name, (value, _) in queued.items()}
        try:
            errors = dict(self._driver.write(values) or {})
        except Exception as e:
            if self._superseded(run):
                self._resolve_writes(queued, {}, str(e))
                return False
            reason = str(e)
            self._resolve_writes(queued, {}, reason)
            self._emit(EVENT_WRITE, WriteResult(values, {}, reason))
            self._emit_problem(Problem(PROBLEM_WRITE, f"Error Writing: {e}", tuple(sorted(values))))
            self._check_transport()
            return False
        self._resolve_writes(queued, errors, None)
        if self._superseded(run):
            return False
        self._emit(EVENT_WRITE, WriteResult(values, errors))
        if errors:
            self._emit_problem(Problem(
                PROBLEM_WRITE,
                "Error Writing: " + "; ".join(f"{name}: {text}" for name, text in sorted(errors.items())),
                tuple(sorted(errors))))
        return True

    @staticmethod
    def _resolve_writes(queued: dict, errors: dict, whole: Optional[str]):
        for name, (_, handle) in queued.items():
            handle._resolve(whole if whole is not None else errors.get(name))

    def _fail_pending_writes(self, reason: str):
        with self._write_lock:
            queued, self._write_queue = self._write_queue, {}
        self._resolve_writes(queued, {}, reason)

    def _close_connection(self) -> bool:
        """
        Close the connection and clear the state. Under the lock: stop() on
        another thread and a worker on its exit path can both get here, and
        DISCONNECTED must be reported exactly once.

        Returns:
            True when the caller has to emit DISCONNECTED (always outside a lock).
        """
        with self._connection_lock:
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
        After a failed read or write, ask the driver whether the connection is
        still there. If not, drop it: the next scan reports DISCONNECTED and
        reconnects instead of failing every scan until someone toggles `enabled`.
        """
        if not self._transport_alive():
            self._drop_connection()

    def _report(self, slot: str, problem: Optional[Problem]):
        """
        Emit a problem when it changes, then again every PROBLEM_REPEAT_SEC
        while it persists, and (for the read slot) one OK when it clears. A
        problem that repeats every scan (a symbol the PLC rejects, a name the
        parser cannot index) would otherwise flood the listeners at the scan
        rate; never repeating it would let a status display that expires
        entries go blank while the problem is still there.
        """
        now = time.monotonic()
        current = self._problems.get(slot)
        if problem is None:
            if current is None:
                return
            self._problems.pop(slot, None)
            if slot == PROBLEM_READ:
                self._emit_problem(Problem(PROBLEM_OK, "Reading OK"))
            return
        if current is not None and current.text == problem.text:
            if now - self._problems_at.get(slot, 0.0) < PROBLEM_REPEAT_SEC:
                return
        self._problems[slot] = problem
        self._problems_at[slot] = now
        self._emit_problem(problem)

    # endregion
    # region - Worker thread

    def _loop(self, run: _Run):
        try:
            self._loop_body(run)
        finally:
            self._loop_exit()

    def _loop_body(self, run: _Run):
        next_scan = time.monotonic()
        while not run.stop.is_set():
            # Nothing in here may end the thread: a bad refresh_ms (None from an
            # unset prim attribute, inf, nan, a string) is logged and treated as
            # idle, and the loop keeps going so that enabled / stop() still work.
            try:
                # Hold the period from scan start to scan start. When a scan
                # overran, start the next one now rather than catching up.
                now = time.monotonic()
                next_scan = max(next_scan, now)
                woken = run.wait(min(next_scan - now, MAX_PERIOD_SEC))
                if run.stop.is_set():
                    break
                if woken:
                    # An early wake (enabled, reconnect, refresh_ms, a queued
                    # write) scans now, and the next scan is one period from
                    # here, not from the old target.
                    next_scan = time.monotonic() + self._period()
                else:
                    # The timeout ran out: keep the absolute cadence. A timed
                    # wait may return a little early (timer granularity), and
                    # rebasing on that would run faster than the period.
                    next_scan += self._period()
                active = self._scan(run)
            except Exception:
                logger.exception("%s: scan failed", self._name)
                active = False
            if not active:
                next_scan = time.monotonic() + IDLE_SEC

    def _period(self) -> float:
        """The scan period in seconds, validated: finite, not negative, capped."""
        period = float(self.refresh_ms) / 1000
        if not math.isfinite(period) or period < 0:
            raise ValueError(f"refresh_ms must be a finite, non-negative number, got {self.refresh_ms!r}")
        return min(period, MAX_PERIOD_SEC)

    def _loop_exit(self):
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

    # endregion
