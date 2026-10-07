"""
Kit tests for loupe.simulation.bridge: the registry, prim discovery (neutral
and legacy), on_sample_main coalescing, the bus adapter on both names, the
mirror watch list and write-back, secrets and the autoConnect setting.

They run against a fake in-memory driver, so no PLC and no vendor library is
needed. tests/vendor_drivers.py registers the real drivers for the harness.
"""

import os
import tempfile
import threading
import time

import carb.settings
import omni.kit.app
import omni.kit.test
import omni.usd
from pxr import Sdf

from plc_bridge import PlcDriver, ReadResult

from .. import registry
from ..bus import (
    BUS_NAMESPACE, EVENT_TYPE_CONNECTION, EVENT_TYPE_DATA_INIT, EVENT_TYPE_DATA_READ, EVENT_TYPE_ENABLE,
    EVENT_TYPE_STATUS, EVENT_TYPE_WRITE, Manager, get_stream_name)
from ..BridgeManager import Manager_Events
from ..delivery import get_system
from ..registry import Option
from ..schema import (
    ATTR_DRIVER, ATTR_ENABLE, ATTR_MIRROR_SYMBOLS, ATTR_VARIABLES, author_config, discover, resolve_secret)
from ..System import System

MAIN_THREAD = threading.current_thread()
SETTING_AUTO_CONNECT = "/exts/loupe.simulation.bridge/autoConnect"
SETTING_LEGACY_BUS = "/exts/loupe.simulation.bridge/legacyBusNames"


class FakeDriver(PlcDriver):
    """Answers every read from a dict; a symbol starting with 'bad' is an error."""

    symbol_separators = "."
    values = {"GVL.a": 1.0, "GVL.b": 2.0, "GVL.arr": [10.0, None, 12.0], "GVL.s": {"x": 1, "y": {"z": True}},
              "Prog:x": 5.0}

    def __init__(self, address="127.0.0.1", port=1, token="", flag=False):
        self.address = address
        self.port = port
        self.token = token
        self.flag = flag
        self.connected = False
        self.reads = 0
        self.writes = []

    def connect(self):
        self.connected = True

    def disconnect(self):
        self.connected = False

    def is_connected(self):
        return self.connected

    def read(self, symbols):
        self.reads += 1
        result = ReadResult()
        for symbol in symbols:
            if symbol.startswith("bad"):
                result.errors[symbol] = "symbol not found"
            else:
                result.values[symbol] = self.values.get(symbol, self.reads)
        return result

    def write(self, values):
        self.writes.append(dict(values))
        return {}


class ColonDriver(FakeDriver):
    symbol_separators = ":."


OPTIONS = [
    Option("Address", "str", "127.0.0.1", "Address"),
    Option("Port", "int", 1, "Port"),
    Option("Token", "str", "", "Token", secret=True),
    Option("Flag", "bool", False, "Flag"),
]


async def ticks(n):
    app = omni.kit.app.get_app()
    for _ in range(n):
        await app.next_update_async()


async def until(condition, seconds=3.0):
    """Tick the app until condition() is true; False on timeout. No fixed waits."""
    t0 = time.time()
    while time.time() - t0 < seconds:
        if condition():
            return True
        await ticks(1)
    return condition()


class BridgeTestCase(omni.kit.test.AsyncTestCase):
    """A fresh empty stage, the fake drivers registered, the extension's System in use."""

    async def setUp(self):
        self.settings = carb.settings.get_settings()
        self.settings.set(SETTING_AUTO_CONNECT, True)
        self.settings.set(SETTING_LEGACY_BUS, True)
        registry.clear()
        self.own_system = get_system() is None
        self.system = get_system() or System()
        if self.own_system:
            self.system.install_default_components()
        registry.register("fake", FakeDriver, OPTIONS, legacy_namespace="fake_bridge", title="Fake")
        registry.register("colon", ColonDriver, OPTIONS[:2], legacy_namespace="colon_bridge")
        self.ctx = omni.usd.get_context()
        await self.ctx.new_stage_async()
        self.stage = self.ctx.get_stage()
        self.system.cleanup()

    async def tearDown(self):
        self.system.cleanup()
        registry.clear()
        if self.own_system:
            self.system.dispose()
        await self.ctx.close_stage_async()
        self.settings.set(SETTING_AUTO_CONNECT, True)
        self.settings.set(SETTING_LEGACY_BUS, True)

    def define(self, path, attrs):
        prim = self.stage.DefinePrim(path, "Scope")
        for name, value in attrs.items():
            if isinstance(value, bool):
                t = Sdf.ValueTypeNames.Bool
            elif isinstance(value, int):
                t = Sdf.ValueTypeNames.Int
            elif isinstance(value, list):
                t = Sdf.ValueTypeNames.StringArray
            else:
                t = Sdf.ValueTypeNames.String
            prim.CreateAttribute(name, t, custom=True).Set(value)
        return prim


class TestRegistry(omni.kit.test.AsyncTestCase):
    async def setUp(self):
        registry.clear()

    async def tearDown(self):
        registry.clear()

    async def test_register_get_and_defaults(self):
        spec = registry.register("fake", FakeDriver, OPTIONS, defaults={"Port": 9})
        self.assertIs(registry.get("fake"), spec)
        self.assertEqual(registry.names(), ["fake"])
        self.assertEqual(spec.defaults, {"Address": "127.0.0.1", "Port": 9, "Token": "", "Flag": False})
        self.assertEqual(spec.namespace, "fake")
        self.assertEqual(spec.attribute("Port"), "fake:Port")
        self.assertIsNone(spec.legacy_namespace)

    async def test_create_driver_uses_snake_case_kwargs(self):
        spec = registry.register("fake", FakeDriver, OPTIONS)
        driver = spec.create_driver({"Address": "10.0.0.1", "Port": "7", "Flag": "true"})
        self.assertEqual((driver.address, driver.port, driver.flag), ("10.0.0.1", 7, True))
        self.assertEqual(registry.snake_case("AmsNetId"), "ams_net_id")

    async def test_apply_option_changes_live_driver(self):
        spec = registry.register("fake", FakeDriver, OPTIONS)
        driver = spec.create_driver({})
        self.assertTrue(spec.apply_option(driver, "Port", 42))
        self.assertEqual(driver.port, 42)
        self.assertFalse(spec.apply_option(driver, "Port", 42))
        self.assertFalse(spec.apply_option(driver, "Unknown", 1))

    async def test_validation(self):
        with self.assertRaises(ValueError):
            Option("x", "list", None)
        with self.assertRaises(ValueError):
            registry.register("fake", FakeDriver, [Option("A", "str", ""), Option("A", "int", 0)])
        with self.assertRaises(ValueError):
            registry.register("fake", FakeDriver, OPTIONS, defaults={"Nope": 1})
        with self.assertRaises(ValueError):
            registry.register("bad:name", FakeDriver)

    async def test_listeners_and_unregister(self):
        events = []
        remove = registry.add_listener(lambda event, spec: events.append((event, spec.name)))
        registry.register("fake", FakeDriver)
        registry.unregister("fake")
        remove()
        registry.register("fake", FakeDriver)
        self.assertEqual(events, [("registered", "fake"), ("unregistered", "fake")])
        self.assertIsNone(registry.get("nope"))
        self.assertIs(registry.by_legacy_namespace("x"), None)

    async def test_secret_resolution(self):
        os.environ["LOUPE_BRIDGE_TEST_TOKEN"] = "s3cret"
        self.assertEqual(resolve_secret("env:LOUPE_BRIDGE_TEST_TOKEN"), "s3cret")
        carb.settings.get_settings().set("/exts/loupe.simulation.bridge/tests/token", "from-setting")
        self.assertEqual(resolve_secret("setting:/exts/loupe.simulation.bridge/tests/token"), "from-setting")
        with self.assertRaises(LookupError):
            resolve_secret("env:LOUPE_BRIDGE_TEST_MISSING_TOKEN")


class TestDiscovery(BridgeTestCase):
    async def test_neutral_and_legacy_prims(self):
        self.define("/PLC/N1", {ATTR_DRIVER: "fake", ATTR_ENABLE: False, "bridge:RefreshRate": 30,
                                ATTR_VARIABLES: ["GVL.a", "GVL.b"], "fake:Address": "10.1.1.1", "fake:Port": 5})
        self.define("/PLC/L1", {"fake_bridge:Address": "10.2.2.2", "fake_bridge:Enable": False,
                                "fake_bridge:RefreshRate": 40, "fake_bridge:Variables": "GVL.a, GVL.b,"})
        self.define("/World/Other", {"foo:bar": 1})
        warned = set()
        configs, unresolved, invalid = discover(self.stage, "/PLC/", warned)
        self.assertEqual({c.name for c in configs}, {"N1", "L1"})
        self.assertEqual((unresolved, invalid), ({}, {}))
        n1 = next(c for c in configs if c.name == "N1")
        l1 = next(c for c in configs if c.name == "L1")
        self.assertEqual((n1.driver, n1.legacy, n1.refresh_ms, n1.variables), ("fake", False, 30, ["GVL.a", "GVL.b"]))
        self.assertEqual(n1.options, {"Address": "10.1.1.1", "Port": 5, "Token": "", "Flag": False})
        self.assertEqual((l1.driver, l1.legacy, l1.refresh_ms, l1.variables), ("fake", True, 40, ["GVL.a", "GVL.b"]))
        self.assertEqual(l1.options["Address"], "10.2.2.2")
        self.assertEqual(l1.options["Port"], 1)
        # the deprecation warning is tracked once per prim
        self.assertEqual(warned, {"/PLC/L1"})
        discover(self.stage, "/PLC/", warned)
        self.assertEqual(warned, {"/PLC/L1"})

        names = self.system.find_and_create_components()
        self.assertEqual(sorted(names), ["L1", "N1"])
        rt = self.system.get_component("L1")
        self.assertTrue(rt.legacy)
        self.assertEqual(rt.driver.address, "10.2.2.2")
        self.assertEqual(rt.read_variables, ["GVL.a", "GVL.b"])
        self.assertEqual(rt.refresh_rate, 40)
        # legacy and neutral option spellings both apply
        rt.options = {"fake_bridge:Port": 8, "fake:Address": "3.3.3.3", "bridge:Variables": ["GVL.a"]}
        self.assertEqual((rt.driver.port, rt.driver.address, rt.read_variables), (8, "3.3.3.3", ["GVL.a"]))
        # the prim going away removes the component
        self.stage.RemovePrim("/PLC/L1")
        self.assertEqual(self.system.find_and_create_components(), ["N1"])

    async def test_unregistered_driver_is_reported_then_picked_up(self):
        self.define("/PLC/X1", {ATTR_DRIVER: "later", "later:Address": "1.1.1.1"})
        self.assertEqual(self.system.find_and_create_components(), [])
        self.assertEqual(self.system.unresolved, {"/PLC/X1": "later"})
        registry.register("later", FakeDriver, OPTIONS[:1])
        # the registry listener rescans the stage
        self.assertEqual(self.system.get_component_names(), ["X1"])
        self.assertEqual(self.system.get_component("X1").driver.address, "1.1.1.1")
        registry.unregister("later")
        self.assertEqual(self.system.get_component_names(), [])

    async def test_add_component_authors_neutral_prim_and_keeps_secret_reference(self):
        os.environ["LOUPE_BRIDGE_TEST_TOKEN"] = "s3cret"
        rt = self.system.add_component("New1", {"fake:Port": 3, "Token": "env:LOUPE_BRIDGE_TEST_TOKEN"}, driver="fake")
        prim = self.stage.GetPrimAtPath("/PLC/New1")
        self.assertTrue(prim.IsValid())
        self.assertEqual(prim.GetAttribute(ATTR_DRIVER).Get(), "fake")
        self.assertEqual(prim.GetAttribute("fake:Port").Get(), 3)
        self.assertEqual(prim.GetAttribute("fake:Token").Get(), "env:LOUPE_BRIDGE_TEST_TOKEN")
        self.assertEqual(rt.driver.token, "s3cret")
        self.assertEqual(rt.options["fake:Token"], "env:LOUPE_BRIDGE_TEST_TOKEN")
        rt.set_read_variables(["GVL.a"])
        self.system.write_options_to_stage("New1")
        self.assertEqual(list(prim.GetAttribute(ATTR_VARIABLES).Get()), ["GVL.a"])
        self.assertEqual(prim.GetAttribute("fake:Token").Get(), "env:LOUPE_BRIDGE_TEST_TOKEN")

    async def test_auto_connect_setting_holds_enabled_prims(self):
        self.define("/PLC/H1", {ATTR_DRIVER: "fake", ATTR_ENABLE: True})
        self.settings.set(SETTING_AUTO_CONNECT, False)
        self.system.find_and_create_components()
        rt = self.system.get_component("H1")
        self.assertFalse(rt.enable_communication)
        self.assertTrue(rt.held)
        rt.enable_communication = True
        self.assertIsNone(rt.held)
        self.system.cleanup()
        self.settings.set(SETTING_AUTO_CONNECT, True)
        self.system.find_and_create_components()
        rt = self.system.get_component("H1")
        self.assertTrue(rt.enable_communication)
        self.assertIsNone(rt.held)


class TestDelivery(BridgeTestCase):
    async def test_on_sample_main_coalesces_on_the_main_thread(self):
        self.define("/PLC/D1", {ATTR_DRIVER: "fake", ATTR_ENABLE: True, "bridge:RefreshRate": 1,
                                ATTR_VARIABLES: ["GVL.a"], "bridge:MirrorToUsd": False})
        self.system.find_and_create_components()
        rt = self.system.get_component("D1")
        worker = []
        main = []
        threads = set()
        rt.plc.on_sample(lambda s: worker.append(s.seq))
        from .. import on_sample_main
        remove = on_sample_main("D1", lambda s: (main.append(s.seq), threads.add(threading.current_thread())))
        frames0 = self.system.delivery.frames
        t0 = time.time()
        while time.time() - t0 < 1.0:
            await ticks(1)
        frames = self.system.delivery.frames - frames0
        remove()
        self.assertGreater(len(worker), 0)
        self.assertGreater(len(main), 0)
        self.assertEqual(threads, {MAIN_THREAD})
        self.assertLessEqual(len(main), frames + 1)
        self.assertLessEqual(len(main), len(worker))
        self.assertEqual(main, sorted(set(main)))  # newest only, never backwards
        # a 1 ms period produces more samples than frames, so gaps must show in seq
        if len(worker) > frames + 1:
            self.assertGreater(max(b - a for a, b in zip(main, main[1:])), 1)
        self.assertEqual(self.system.delivery.listeners("D1"), 0)

    async def test_registration_outlives_the_runtime(self):
        from .. import on_sample_main
        got = []
        remove = on_sample_main("D2", got.append)
        self.define("/PLC/D2", {ATTR_DRIVER: "fake", ATTR_ENABLE: True, "bridge:RefreshRate": 5,
                                ATTR_VARIABLES: ["GVL.a"], "bridge:MirrorToUsd": False})
        self.system.find_and_create_components()
        t0 = time.time()
        while time.time() - t0 < 1.0 and not got:
            await ticks(1)
        remove()
        self.assertTrue(got)


class TestBus(BridgeTestCase):
    def _subscribe(self, name, got):
        """Subscriptions on both namespaces, filled before the component exists."""
        bus = omni.kit.app.get_app().get_message_bus_event_stream()
        legacy = Manager_Events("fake_bridge")
        pairs = [
            ("neutral", EVENT_TYPE_DATA_READ, "data"), ("legacy", legacy.EVENT_TYPE_DATA_READ, "data"),
            ("status_neutral", EVENT_TYPE_STATUS, "status"), ("status_legacy", legacy.EVENT_TYPE_STATUS, "status"),
            ("init_neutral", EVENT_TYPE_DATA_INIT, "meta"), ("init_legacy", legacy.EVENT_TYPE_DATA_INIT, "meta"),
            ("conn_neutral", EVENT_TYPE_CONNECTION, "status"), ("conn_legacy", legacy.EVENT_TYPE_CONNECTION, "status"),
            ("enable_neutral", EVENT_TYPE_ENABLE, "status"), ("enable_legacy", legacy.EVENT_TYPE_ENABLE, "status"),
            ("write", EVENT_TYPE_WRITE, "values"),
        ]
        subs = []
        for key, event_type, field in pairs:
            got.setdefault(key, [])
            subs.append(bus.create_subscription_to_push_by_type(
                get_stream_name(event_type, name),
                lambda e, key=key, field=field: got[key].append(e.payload[field])))
        return subs

    async def test_adapter_emits_neutral_and_legacy_names(self):
        got = {}
        subs = self._subscribe("B1", got)
        manager = Manager("B1", namespace="fake_bridge")
        manager.register_data_callback(lambda e: got.setdefault("manager", []).append(e.payload["meta"]["name"]))
        # Subscribed before the runtime exists: the first events are not missed.
        self.define("/PLC/B1", {ATTR_DRIVER: "fake", ATTR_ENABLE: True, "bridge:RefreshRate": 5,
                                ATTR_VARIABLES: ["GVL.a", "bad.x"], "bridge:MirrorToUsd": False})
        self.system.find_and_create_components()
        # DATA_INIT is pushed once per namespace when the adapter is built
        self.assertEqual(got["init_neutral"], [{"name": "B1"}])
        self.assertEqual(got["init_legacy"], [{"name": "B1"}])
        self.assertTrue(await until(lambda: got["neutral"] and got["legacy"] and got.get("manager")
                                    and got["status_legacy"] and "Connected" in got["conn_neutral"]))
        self.assertEqual(got["neutral"][0]["GVL"]["a"], 1.0)
        self.assertEqual(got["manager"][0], "B1")
        # the very first connection events reached the listeners (start() after attach)
        self.assertEqual(got["conn_neutral"][:2], ["Connecting", "Connected"])
        self.assertEqual(got["conn_legacy"][:2], ["Connecting", "Connected"])
        problem = got["status_neutral"][0]
        self.assertEqual(problem["kind"], "read")
        self.assertIn("bad.x", problem["symbols"])
        self.assertIsInstance(got["status_legacy"][0], str)
        self.assertIn("bad.x", got["status_legacy"][0])
        # requests arrive on both names; the WRITE event reports the flushed batch
        rt = self.system.get_component("B1")
        manager.write_variable("GVL.a", 9.0)
        Manager("B1").add_cyclic_read_variables(["GVL.b"])
        self.assertTrue(await until(lambda: rt.driver.writes and got["write"]))
        self.assertEqual(rt.driver.writes[-1], {"GVL.a": 9.0})
        self.assertEqual(dict(got["write"][-1]), {"GVL.a": 9.0})
        self.assertIn("GVL.b", rt.read_variables)
        # ENABLE goes out on every assignment, on both names, with the value
        before = len(got["enable_neutral"])
        rt.enable_communication = False
        self.assertEqual(got["enable_neutral"][before:], [{"enabled": False}])
        self.assertEqual(got["enable_legacy"][-1], {"enabled": False})
        self.assertTrue(await until(lambda: got["conn_neutral"][-1] == "Disconnected"))
        self.assertEqual(got["conn_legacy"][-1], "Disconnected")
        manager.cleanup()
        subs.clear()

    async def test_manager_on_the_neutral_namespace(self):
        got = []
        manager = Manager("B3")
        manager.register_data_callback(lambda e: got.append(e.payload["data"]))
        self.define("/PLC/B3", {ATTR_DRIVER: "fake", ATTR_ENABLE: True, "bridge:RefreshRate": 5,
                                ATTR_VARIABLES: ["GVL.a"], "bridge:MirrorToUsd": False})
        self.system.find_and_create_components()
        self.assertTrue(await until(lambda: got))
        self.assertEqual(got[0], {"GVL": {"a": 1.0}})
        rt = self.system.get_component("B3")
        manager.write_variables({"GVL.a": 2.0, "GVL.b": 3.0})
        self.assertTrue(await until(lambda: rt.driver.writes))
        self.assertEqual(rt.driver.writes[-1], {"GVL.a": 2.0, "GVL.b": 3.0})
        manager.cleanup()

    async def test_legacy_names_off_by_setting(self):
        self.settings.set(SETTING_LEGACY_BUS, False)
        self.define("/PLC/B2", {ATTR_DRIVER: "fake", "bridge:MirrorToUsd": False})
        self.system.find_and_create_components()
        adapter = self.system.get_part("B2", "bus")
        self.assertEqual(adapter.namespaces, [BUS_NAMESPACE])
        self.settings.set(SETTING_LEGACY_BUS, True)
        self.system.cleanup()
        self.system.find_and_create_components()
        self.assertEqual(self.system.get_part("B2", "bus").namespaces, [BUS_NAMESPACE, "fake_bridge"])


class TestMirror(BridgeTestCase):
    async def _wait_for(self, path, seconds=2.0):
        t0 = time.time()
        while time.time() - t0 < seconds:
            prim = self.stage.GetPrimAtPath(path)
            if prim and prim.IsValid() and prim.GetAttribute("value").HasValue():
                return prim
            await ticks(1)
        return None

    async def test_watch_list_arrays_and_structs(self):
        self.define("/PLC/M1", {ATTR_DRIVER: "fake", ATTR_ENABLE: True, "bridge:RefreshRate": 5,
                                ATTR_VARIABLES: ["GVL.a", "GVL.b", "GVL.arr", "GVL.s"],
                                ATTR_MIRROR_SYMBOLS: ["GVL.a", "GVL.arr", "GVL.s"]})
        self.system.find_and_create_components()
        self.assertIsNotNone(self.system.get_part("M1", "mirror"))
        a = await self._wait_for("/PLC/M1/GVL/a")
        self.assertIsNotNone(a)
        self.assertEqual(a.GetAttribute("value").Get(), 1.0)
        self.assertEqual(a.GetAttribute("symbol").Get(), "GVL.a")
        # a None-padded array mirrors its present elements as _<index> prims
        self.assertIsNotNone(await self._wait_for("/PLC/M1/GVL/arr/_0"))
        self.assertIsNotNone(await self._wait_for("/PLC/M1/GVL/arr/_2"))
        self.assertFalse(self.stage.GetPrimAtPath("/PLC/M1/GVL/arr/_1").IsValid())
        self.assertEqual(self.stage.GetPrimAtPath("/PLC/M1/GVL/arr/_2").GetAttribute("symbol").Get(), "GVL.arr[2]")
        # a struct read whole expands into members
        z = await self._wait_for("/PLC/M1/GVL/s/y/z")
        self.assertIsNotNone(z)
        self.assertEqual(z.GetAttribute("value").Get(), True)
        self.assertEqual(z.GetAttribute("symbol").Get(), "GVL.s.y.z")
        # not on the watch list
        await ticks(5)
        self.assertFalse(self.stage.GetPrimAtPath("/PLC/M1/GVL/b").IsValid())
        # the mirror lives in the session layer, not the root layer
        self.assertIsNone(self.stage.GetRootLayer().GetPrimAtPath("/PLC/M1/GVL"))
        self.assertIsNotNone(self.stage.GetSessionLayer().GetPrimAtPath("/PLC/M1/GVL/a"))

    async def test_mirror_off_by_prim(self):
        self.define("/PLC/M2", {ATTR_DRIVER: "fake", "bridge:MirrorToUsd": False})
        self.system.find_and_create_components()
        self.assertIsNone(self.system.get_part("M2", "mirror"))
        self.assertIsNotNone(self.system.get_part("M2", "bus"))

    async def test_write_back_keeps_the_driver_symbol_spelling(self):
        self.define("/PLC/M3", {ATTR_DRIVER: "colon", ATTR_ENABLE: True, "bridge:RefreshRate": 5,
                                ATTR_VARIABLES: ["Prog:x"]})
        self.system.find_and_create_components()
        rt = self.system.get_component("M3")
        prim = await self._wait_for("/PLC/M3/Prog/x")
        self.assertIsNotNone(prim)
        self.assertEqual(prim.GetAttribute("symbol").Get(), "Prog:x")
        prim.GetAttribute("write:value").Set(6.5)
        t0 = time.time()
        while time.time() - t0 < 2.0 and not rt.driver.writes:
            await ticks(1)
        self.assertEqual(rt.driver.writes[-1], {"Prog:x": 6.5})
        # write:pause holds edits; write:once sends one and resets itself
        prim.GetAttribute("write:pause").Set(True)
        prim.GetAttribute("write:value").Set(7.5)
        await ticks(3)
        self.assertEqual(rt.driver.writes[-1], {"Prog:x": 6.5})
        prim.GetAttribute("write:once").Set(True)
        t0 = time.time()
        while time.time() - t0 < 2.0 and rt.driver.writes[-1] != {"Prog:x": 7.5}:
            await ticks(1)
        self.assertEqual(rt.driver.writes[-1], {"Prog:x": 7.5})
        self.assertFalse(prim.GetAttribute("write:once").Get())

    async def test_author_config_round_trip(self):
        from ..schema import PlcConfig
        spec = registry.get("fake")
        config = PlcConfig("R1", "/PLC/R1", "fake", enabled=True, refresh_ms=15, variables=["GVL.a"],
                           mirror=False, mirror_symbols=["GVL.a"], options={"Address": "9.9.9.9", "Port": 2, "Token": "x"})
        prim = self.stage.DefinePrim("/PLC/R1", "Scope")
        author_config(prim, config, spec, {"Token": "env:T"})
        configs, _, _ = discover(self.stage, "/PLC/", set())
        back = configs[0]
        self.assertEqual((back.enabled, back.refresh_ms, back.variables, back.mirror, back.mirror_symbols),
                         (True, 15, ["GVL.a"], False, ["GVL.a"]))
        self.assertEqual(back.options["Address"], "9.9.9.9")
        self.assertEqual(back.options["Token"], "env:T")


class TestRobustness(BridgeTestCase):
    async def test_malformed_prim_does_not_stop_the_others(self):
        self.define("/PLC/Good1", {ATTR_DRIVER: "fake", "fake:Port": 3})
        bad_port = self.define("/PLC/BadPort", {ATTR_DRIVER: "fake"})
        bad_port.CreateAttribute("fake:Port", Sdf.ValueTypeNames.String, custom=True).Set("8000a")
        bad_vars = self.define("/PLC/BadVars", {ATTR_DRIVER: "fake"})
        bad_vars.CreateAttribute(ATTR_VARIABLES, Sdf.ValueTypeNames.Int, custom=True).Set(5)
        self.define("/PLC/Good2", {ATTR_DRIVER: "fake", "fake:Port": 4})
        names = self.system.find_and_create_components()
        self.assertEqual(sorted(names), ["Good1", "Good2"])
        self.assertEqual(sorted(self.system.invalid), ["/PLC/BadPort", "/PLC/BadVars"])
        self.assertIn("8000a", self.system.invalid["/PLC/BadPort"])
        self.assertEqual(self.system.get_component("Good2").driver.port, 4)

    async def test_unresolvable_secret_is_reported_not_dropped(self):
        os.environ.pop("LOUPE_BRIDGE_TEST_MISSING_TOKEN", None)
        self.define("/PLC/S1", {ATTR_DRIVER: "fake", "fake:Token": "env:LOUPE_BRIDGE_TEST_MISSING_TOKEN"})
        self.define("/PLC/S2", {ATTR_DRIVER: "fake"})
        names = self.system.find_and_create_components()
        self.assertEqual(names, ["S2"])
        self.assertIn("/PLC/S1", self.system.invalid)
        self.assertIn("LOUPE_BRIDGE_TEST_MISSING_TOKEN", self.system.invalid["/PLC/S1"])

    async def test_set_driver_option_keeps_state_when_the_secret_fails(self):
        os.environ["LOUPE_BRIDGE_TEST_TOKEN"] = "good"
        os.environ.pop("LOUPE_BRIDGE_TEST_MISSING_TOKEN", None)
        rt = self.system.add_component("S3", {"Token": "env:LOUPE_BRIDGE_TEST_TOKEN"}, driver="fake")
        with self.assertRaises(LookupError):
            rt.set_driver_option("Token", "env:LOUPE_BRIDGE_TEST_MISSING_TOKEN")
        self.assertEqual(rt.driver.token, "good")
        self.assertEqual(rt.options["fake:Token"], "env:LOUPE_BRIDGE_TEST_TOKEN")
        self.system.write_options_to_stage("S3")
        self.assertEqual(self.stage.GetPrimAtPath("/PLC/S3").GetAttribute("fake:Token").Get(),
                         "env:LOUPE_BRIDGE_TEST_TOKEN")

    async def test_write_to_usd_retypes_a_string_variables_attribute(self):
        prim = self.define("/PLC/T1", {ATTR_DRIVER: "fake"})
        prim.CreateAttribute(ATTR_VARIABLES, Sdf.ValueTypeNames.String, custom=True).Set("GVL.a, GVL.b")
        self.system.find_and_create_components()
        rt = self.system.get_component("T1")
        self.assertEqual(rt.read_variables, ["GVL.a", "GVL.b"])
        rt.set_read_variables(["GVL.c"])
        self.system.write_options_to_stage("T1")
        attr = prim.GetAttribute(ATTR_VARIABLES)
        self.assertEqual(attr.GetTypeName(), Sdf.ValueTypeNames.StringArray)
        self.assertEqual(list(attr.Get()), ["GVL.c"])

    async def test_mirror_settings_change_rebuilds_the_mirror(self):
        self.define("/PLC/M4", {ATTR_DRIVER: "fake", ATTR_ENABLE: True, "bridge:RefreshRate": 5,
                                ATTR_VARIABLES: ["GVL.a", "GVL.b"], ATTR_MIRROR_SYMBOLS: ["GVL.a"]})
        self.system.find_and_create_components()
        rt = self.system.get_component("M4")
        first = self.system.get_part("M4", "mirror")
        self.assertEqual(first.watch, ["GVL.a"])
        self.assertTrue(await until(lambda: self.stage.GetPrimAtPath("/PLC/M4/GVL/a").IsValid()))
        self.assertFalse(self.stage.GetPrimAtPath("/PLC/M4/GVL/b").IsValid())
        rt.options = {ATTR_MIRROR_SYMBOLS: ["GVL.b"]}
        second = self.system.get_part("M4", "mirror")
        self.assertIsNot(second, first)
        self.assertEqual(second.watch, ["GVL.b"])
        self.assertTrue(await until(lambda: self.stage.GetPrimAtPath("/PLC/M4/GVL/b").IsValid()))
        rt.options = {"bridge:MirrorToUsd": False}
        self.assertIsNone(self.system.get_part("M4", "mirror"))
        self.assertEqual(self.system.delivery.listeners("M4"), 0)
        rt.options = {"bridge:MirrorToUsd": True}
        self.assertIsNotNone(self.system.get_part("M4", "mirror"))

    async def test_no_delivery_after_removal_from_inside_a_callback(self):
        from .. import on_sample_main
        self.define("/PLC/R1", {ATTR_DRIVER: "fake", ATTR_ENABLE: True, "bridge:RefreshRate": 1,
                                ATTR_VARIABLES: ["GVL.a"], "bridge:MirrorToUsd": False})
        self.system.find_and_create_components()
        calls = []
        later = []

        def first(sample):
            calls.append(sample.seq)
            self.system.remove_component("R1")

        remove_first = on_sample_main("R1", first)
        remove_later = on_sample_main("R1", later.append)
        self.assertTrue(await until(lambda: calls))
        await ticks(10)
        self.assertEqual(len(calls), 1)
        self.assertEqual(later, [])
        self.assertEqual(self.system.get_component_names(), [])
        remove_first()
        remove_later()

    async def test_re_registration_rebuilds_components_on_the_new_class(self):
        self.define("/PLC/RR1", {ATTR_DRIVER: "fake"})
        self.system.find_and_create_components()
        self.assertIs(type(self.system.get_component("RR1").driver), FakeDriver)

        class FakeDriverV2(FakeDriver):
            pass

        registry.register("fake", FakeDriverV2, OPTIONS, legacy_namespace="fake_bridge")
        rt = self.system.get_component("RR1")
        self.assertIs(type(rt.driver), FakeDriverV2)
        self.assertIsNotNone(self.system.get_part("RR1", "bus"))

    async def test_stage_open_and_close_follow_the_extension(self):
        if self.own_system:
            self.skipTest("the extension is not running; stage events are its job")
        folder = tempfile.mkdtemp(prefix="bridge_test_")
        path = os.path.join(folder, "reload.usda").replace("\\", "/")
        with open(path, "w") as f:
            f.write('''#usda 1.0
def Scope "PLC" {
    def Scope "F1" {
        custom string bridge:driver = "fake"
        custom string[] bridge:Variables = ["GVL.a"]
        custom bool bridge:MirrorToUsd = false
    }
}
''')
        await self.ctx.open_stage_async(path)
        self.assertTrue(await until(lambda: self.system.get_component_names() == ["F1"]))
        self.assertEqual(self.system.get_component("F1").read_variables, ["GVL.a"])
        await self.ctx.close_stage_async()
        self.assertTrue(await until(lambda: self.system.get_component_names() == []))
        await self.ctx.new_stage_async()
        self.stage = self.ctx.get_stage()
