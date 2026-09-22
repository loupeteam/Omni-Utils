import threading
import time

import pytest

from plc_bridge import (CONNECTED, CONNECTING, DISCONNECTED, PlcDriver, PlcRuntime,
                        ReadResult)


class FakeDriver(PlcDriver):
    def __init__(self):
        self.connected = False
        self.connect_error = None
        self.read_error = None
        self.write_error = None
        self.write_rejects = {}
        self.values = {}
        self.errors = {}
        self.reads = []
        self.writes = []
        self.connects = 0
        self.disconnects = 0

    def connect(self):
        self.connects += 1
        if self.connect_error:
            raise self.connect_error
        self.connected = True

    def disconnect(self):
        self.disconnects += 1
        self.connected = False

    def is_connected(self):
        return self.connected

    def read(self, symbols):
        self.reads.append(list(symbols))
        if self.read_error:
            raise self.read_error
        return ReadResult(
            values={s: self.values[s] for s in symbols if s in self.values},
            errors={s: self.errors[s] for s in symbols if s in self.errors},
        )

    def write(self, values):
        if self.write_error:
            raise self.write_error
        self.writes.append(dict(values))
        return {k: v for k, v in self.write_rejects.items() if k in values}


class Recorder:
    def __init__(self, runtime):
        self.data, self.status, self.connection, self.enabled = [], [], [], []
        runtime.on_data(self.data.append)
        runtime.on_status(self.status.append)
        runtime.on_connection(self.connection.append)
        runtime.on_enabled(self.enabled.append)


@pytest.fixture
def driver():
    d = FakeDriver()
    d.values = {"GVL.a": 1, "GVL.arr[1]": 2}
    return d


@pytest.fixture
def plc(driver):
    runtime = PlcRuntime(driver, name="T", enabled=True)
    runtime.set_read_variables(["GVL.a", "GVL.arr[1]"])
    return runtime


# region - scans

def test_disabled_runtime_stays_idle_and_never_connects(driver):
    plc = PlcRuntime(driver)
    rec = Recorder(plc)
    assert plc.scan_read() is False
    assert driver.connects == 0 and rec.connection == [] and rec.data == []


def test_connects_reads_and_emits_nested_data(plc, driver):
    rec = Recorder(plc)
    assert plc.scan_read() is True
    assert rec.connection == [CONNECTING, CONNECTED]
    assert rec.data == [{"GVL": {"a": 1, "arr": [None, 2]}}]
    assert driver.reads == [["GVL.a", "GVL.arr[1]"]]
    assert plc.is_connected


def test_connect_failure_is_a_status_and_is_retried(plc, driver):
    rec = Recorder(plc)
    driver.connect_error = OSError("no route")
    assert plc.scan_read() is False
    assert rec.status == ["Error Connecting: no route"]
    driver.connect_error = None
    assert plc.scan_read() is True
    assert rec.connection == [CONNECTING, CONNECTING, CONNECTED]


def test_disable_disconnects_and_reports_once(plc, driver):
    rec = Recorder(plc)
    plc.scan_read()
    plc.enabled = False
    assert plc.scan_read() is False
    assert plc.scan_read() is False
    assert rec.connection == [CONNECTING, CONNECTED, DISCONNECTED]
    assert rec.enabled == [False]
    assert not driver.connected


def test_failed_symbol_is_reported_once_and_recovers(plc, driver):
    rec = Recorder(plc)
    driver.errors = {"GVL.arr[1]": "symbol not found"}
    del driver.values["GVL.arr[1]"]
    plc.scan_read()
    plc.scan_read()
    assert rec.data == [{"GVL": {"a": 1}}] * 2
    assert rec.status == ["Error Reading: GVL.arr[1]: symbol not found"]
    driver.errors = {}
    driver.values["GVL.arr[1]"] = 2
    plc.scan_read()
    assert rec.status[-1] == "Reading OK"


def test_total_failure_emits_no_data(plc, driver):
    rec = Recorder(plc)
    driver.values = {}
    driver.errors = {"GVL.a": "symbol not found", "GVL.arr[1]": "symbol not found"}
    plc.scan_read()
    assert rec.data == []
    assert rec.status[0] == "Error Reading: all 2 symbol(s) failed"
    assert "GVL.a: symbol not found" in rec.status[1]


def test_read_exception_is_a_status_and_polling_continues(plc, driver):
    rec = Recorder(plc)
    driver.read_error = RuntimeError("boom")
    assert plc.scan_read() is True
    assert rec.status == ["Error Reading: boom"]
    driver.read_error = None
    plc.scan_read()
    assert len(rec.data) == 1


def test_reconnect_drops_and_reopens(plc, driver):
    plc.scan_read()
    plc.reconnect()
    plc.scan_read()
    assert driver.connects == 2


def test_separators_come_from_the_driver(plc, driver):
    driver.symbol_separators = ":."
    driver.values = {"Prog:s.m": 5}
    plc.set_read_variables(["Prog:s.m"])
    rec = Recorder(plc)
    plc.scan_read()
    assert rec.data == [{"Prog": {"s": {"m": 5}}}]

# endregion
# region - writes

def test_writes_are_batched_and_last_value_wins(plc, driver):
    plc.scan_read()
    plc.queue_write("GVL.a", 1)
    plc.queue_write("GVL.a", 2)
    plc.queue_write("GVL.b", 3)
    assert plc.scan_write() is True
    assert driver.writes == [{"GVL.a": 2, "GVL.b": 3}]
    assert plc.scan_write() is False


def test_writes_wait_for_a_connection(plc, driver):
    plc.queue_write("GVL.a", 1)
    assert plc.scan_write() is False
    plc.scan_read()
    assert plc.scan_write() is True
    assert driver.writes == [{"GVL.a": 1}]


def test_rejected_symbols_of_a_write_are_a_status(plc, driver):
    rec = Recorder(plc)
    plc.scan_read()
    driver.write_rejects = {"GVL.b": "symbol not found"}
    plc.queue_write("GVL.a", 1)
    plc.queue_write("GVL.b", 2)
    assert plc.scan_write() is True
    assert rec.status == ["Error Writing: GVL.b: symbol not found"]
    assert driver.writes == [{"GVL.a": 1, "GVL.b": 2}]


def test_write_failure_is_a_status(plc, driver):
    rec = Recorder(plc)
    plc.scan_read()
    driver.write_error = RuntimeError("denied")
    plc.queue_write("GVL.a", 1)
    assert plc.scan_write() is False
    assert rec.status == ["Error Writing: denied"]

# endregion
# region - read list and listeners

def test_empty_read_list_makes_no_round_trip(driver):
    plc = PlcRuntime(driver, enabled=True)
    rec = Recorder(plc)
    assert plc.scan_read() is True
    assert driver.reads == [] and rec.data == []


def test_read_variables_are_cleaned_and_copied(driver):
    plc = PlcRuntime(driver)
    plc.set_read_variables([" GVL.a\r", "", "GVL.b", "GVL.a"])
    plc.read_variables.append("x")
    assert plc.read_variables == ["GVL.a", "GVL.b"]
    plc.add_read_variables(["GVL.b", "GVL.c"])
    assert plc.read_variables == ["GVL.a", "GVL.b", "GVL.c"]


def test_a_raising_listener_does_not_stop_the_others(plc):
    seen = []
    plc.on_data(lambda data: 1 / 0)
    plc.on_data(seen.append)
    plc.scan_read()
    assert len(seen) == 1


def test_listener_can_be_removed(plc):
    seen = []
    remove = plc.on_data(seen.append)
    plc.scan_read()
    remove()
    remove()
    plc.scan_read()
    assert len(seen) == 1

# endregion
# region - threads

def test_threads_poll_write_and_stop_quickly(plc, driver):
    plc.refresh_ms = 5
    got_data = threading.Event()
    plc.on_data(lambda data: got_data.set())
    plc.start()
    try:
        assert plc.is_running
        assert got_data.wait(2)
        plc.queue_write("GVL.a", 9)
        deadline = time.monotonic() + 2
        while not driver.writes and time.monotonic() < deadline:
            time.sleep(0.005)
        assert driver.writes == [{"GVL.a": 9}]
    finally:
        started = time.monotonic()
        plc.stop()
    assert time.monotonic() - started < 1
    assert not plc.is_running
    assert not driver.connected
    assert all(t.daemon for t in threading.enumerate() if t.name.startswith("T-"))


def test_stop_is_quick_while_idle(driver):
    plc = PlcRuntime(driver)  # disabled: the read loop idles for IDLE_SEC at a time
    plc.start()
    time.sleep(0.05)
    started = time.monotonic()
    plc.stop()
    assert time.monotonic() - started < 0.5


def test_start_twice_and_restart(plc):
    plc.start()
    plc.start()
    assert len([t for t in threading.enumerate() if t.name.startswith("T-")]) == 2
    plc.stop()
    plc.start()
    assert plc.is_running
    plc.stop()

# endregion


# region - review findings

def test_unrepresentable_symbol_is_a_status_at_full_rate(plc, driver):
    """A parse failure is reported like a read failure and does not idle the loop."""
    rec = Recorder(plc)
    driver.values = {"GVL.a": 1, "GVL.arr2d[0,1]": 2}
    plc.set_read_variables(list(driver.values))
    assert plc.scan_read() is True
    assert rec.data == []
    assert rec.status == ["Error Reading: cannot index symbol 'GVL.arr2d[0,1]': 'arr2d[0,1]'"]
    assert plc.is_connected


def test_dropped_transport_is_detected_and_reconnected(plc, driver):
    rec = Recorder(plc)
    plc.scan_read()
    # the peer goes away: reads raise and the driver says the link is gone
    driver.connected = False
    driver.read_error = ConnectionError("reset by peer")
    plc.scan_read()
    assert rec.status == ["Error Reading: reset by peer"]
    assert not plc.is_connected
    driver.read_error = None
    plc.scan_read()
    assert rec.connection == [CONNECTING, CONNECTED, DISCONNECTED, CONNECTING, CONNECTED]
    assert driver.connects == 2


def test_read_error_with_live_transport_keeps_the_connection(plc, driver):
    plc.scan_read()
    driver.read_error = RuntimeError("bad request")
    plc.scan_read()
    assert plc.is_connected and driver.connects == 1


def test_write_error_with_dead_transport_drops_the_connection(plc, driver):
    plc.scan_read()
    driver.connected = False
    driver.write_error = ConnectionError("gone")
    plc.queue_write("GVL.a", 1)
    plc.scan_write()
    assert not plc.is_connected


def test_reconnect_requested_during_a_scan_is_not_lost(plc, driver):
    plc.scan_read()
    original_read = driver.read

    def read_and_reconnect(symbols):
        # reconnect() lands while the scan is inside the driver, after the
        # flag was sampled and cleared
        plc.reconnect()
        driver.read = original_read
        return original_read(symbols)

    driver.read = read_and_reconnect
    plc.scan_read()
    plc.scan_read()
    assert driver.connects == 2


def test_reconnect_while_disconnected_does_nothing(plc, driver):
    plc.reconnect()
    assert plc.scan_read() is True
    assert driver.disconnects == 1  # the pre-connect cleanup only, no extra drop


def test_stop_waits_at_most_the_join_timeout_in_total(driver, monkeypatch):
    import plc_bridge.runtime as rt
    monkeypatch.setattr(rt, "JOIN_TIMEOUT_SEC", 0.3)
    release = threading.Event()
    driver.read = lambda symbols: (release.wait(5), ReadResult())[1]
    driver.write = lambda values: (release.wait(5), {})[1]
    plc = PlcRuntime(driver, name="T", enabled=True)
    plc.set_read_variables(["GVL.a"])
    plc.start()
    plc.queue_write("GVL.a", 1)
    time.sleep(0.1)  # both workers are now inside the driver
    started = time.monotonic()
    plc.stop()
    elapsed = time.monotonic() - started
    release.set()
    assert elapsed < 0.5, elapsed


def test_restart_while_a_worker_is_stuck_leaves_no_zombie(driver, monkeypatch):
    import plc_bridge.runtime as rt
    monkeypatch.setattr(rt, "JOIN_TIMEOUT_SEC", 0.2)
    release = threading.Event()

    def slow_read(symbols):
        release.wait(5)
        return ReadResult(values={"GVL.a": 1})

    driver.read = slow_read
    plc = PlcRuntime(driver, name="Z", enabled=True, refresh_ms=5)
    plc.set_read_variables(["GVL.a"])
    plc.start()
    time.sleep(0.05)
    plc.stop()  # gives up on the stuck read thread
    plc.start()
    driver.read = lambda symbols: ReadResult(values={"GVL.a": 2})
    release.set()  # the old thread returns from its read now
    time.sleep(0.3)
    workers = [t for t in threading.enumerate() if t.name.startswith("Z-")]
    assert sorted(t.name for t in workers) == ["Z-read", "Z-write"]
    assert plc.is_connected  # the zombie's exit did not close the new run's connection
    plc.stop()


def test_enabled_wakes_an_idle_read_loop(driver):
    plc = PlcRuntime(driver, refresh_ms=5)  # disabled: the loop idles IDLE_SEC at a time
    connected = threading.Event()
    plc.on_connection(lambda s: connected.set() if s == CONNECTED else None)
    plc.set_read_variables(["GVL.a"])
    plc.start()
    time.sleep(0.05)
    started = time.monotonic()
    plc.enabled = True
    assert connected.wait(0.5)
    assert time.monotonic() - started < 0.5
    plc.stop()


def test_toggling_enabled_while_running(plc, driver):
    plc.refresh_ms = 2
    rec = Recorder(plc)
    plc.start()
    try:
        deadline = time.monotonic() + 2
        while CONNECTED not in rec.connection and time.monotonic() < deadline:
            time.sleep(0.005)
        plc.enabled = False
        deadline = time.monotonic() + 2
        while DISCONNECTED not in rec.connection and time.monotonic() < deadline:
            time.sleep(0.005)
        plc.enabled = True
        deadline = time.monotonic() + 2
        while rec.connection.count(CONNECTED) < 2 and time.monotonic() < deadline:
            time.sleep(0.005)
    finally:
        plc.stop()
    assert rec.connection[:5] == [CONNECTING, CONNECTED, DISCONNECTED, CONNECTING, CONNECTED]
    assert not driver.connected


def test_concurrent_start_calls_start_one_pair(driver):
    plc = PlcRuntime(driver, name="C")
    threads = [threading.Thread(target=plc.start) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len([t for t in threading.enumerate() if t.name.startswith("C-")]) == 2
    plc.stop()


def test_listener_removed_from_inside_a_callback(plc):
    seen = []
    holder = {}

    def once(data):
        seen.append(data)
        holder["remove"]()

    holder["remove"] = plc.on_data(once)
    plc.scan_read()
    plc.scan_read()
    assert len(seen) == 1

# endregion
