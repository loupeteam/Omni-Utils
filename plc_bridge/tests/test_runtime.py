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


def test_write_failure_is_a_status(plc, driver):
    rec = Recorder(plc)
    plc.scan_read()
    driver.write_error = RuntimeError("denied")
    plc.queue_write("GVL.a", 1)
    assert plc.scan_write() is False
    assert rec.status == ["Error Writing: denied"]

# endregion
# region - read list and listeners

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
