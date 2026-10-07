"""
Headless Kit check for the PLC bridge framework (loupe.simulation.bridge).

Run inside Kit with --exec (run.sh / run.ps1 do that). Environment:
  FIXCHECK_STAGE     stage to open; default stages/mixed_test.usda next to this file:
                     a legacy Beckhoff prim /PLC/PLC1 and a neutral B&R prim /PLC/BR1
  FIXCHECK_MODE      "" = live (Beckhoff against the PLC in the stage, B&R against a mock
                     OMJSON server started here), "inject" = synthetic DATA_READ, no PLC
  FIXCHECK_BR_TESTS  folder holding br_bridge's mock_omjson.py (default: found next to
                     the installed br_bridge checkout)
Prints one line per check and "OK -- all fix checks passed" or "FAIL ...".
"""

import asyncio
import importlib.util
import os
import sys
import threading
import time

import carb.events
import carb.settings
import omni.kit.app
import omni.usd

HERE = os.path.dirname(os.path.abspath(__file__))
STAGE = os.path.abspath(os.environ.get("FIXCHECK_STAGE") or os.path.join(HERE, "stages", "mixed_test.usda")).replace("\\", "/")
MODE = os.environ.get("FIXCHECK_MODE", "")
EXT = "loupe.simulation.bridge"
LIVE_SEC = 5.0
MAIN_THREAD = threading.current_thread()

fails = []
app = omni.kit.app.get_app()
mgr = app.get_extension_manager()

print("fix check -- Kit {}, Python {}.{}".format(app.get_build_version(), *sys.version_info[:2]))
print("=" * 68)


def _ver(e):
    v = e.get("version", "")
    return ".".join(str(x) for x in v if x != "") if isinstance(v, (tuple, list)) else str(v)


# --- 1. registered, enabled, clean startup -------------------------------------------
known = [e for e in mgr.get_extensions() if e.get("name", "") == EXT]
for e in known:
    print("  registered   {} {}  enabled={}  path={}".format(
        e.get("name", ""), _ver(e), e.get("enabled", False), e.get("path", "")))
if not any(e.get("enabled", False) for e in known):
    fails.append("extension not enabled")

from loupe.simulation.bridge import Manager, get_plc, get_system, on_sample_main, registry  # noqa: E402
from loupe.simulation.bridge.bus import EVENT_TYPE_DATA_READ, EVENT_TYPE_STATUS, get_stream_name  # noqa: E402
from loupe.simulation.bridge.BridgeManager import Manager_Events  # noqa: E402
from loupe.simulation.bridge.tests.vendor_drivers import register_vendor_drivers  # noqa: E402
import plc_bridge  # noqa: E402
from plc_bridge import Sample  # noqa: E402

print("  startup      clean; plc_bridge from {}".format(os.path.dirname(plc_bridge.__file__)))

# --- 2. drivers: registered by this script until Phase 4 moves it into the vendor exts
drivers = register_vendor_drivers()
print("  drivers      {}".format(drivers))
if drivers.get("beckhoff") is not True or drivers.get("br") is not True:
    fails.append("vendor drivers not importable: {}".format(drivers))

LEGACY_BK = Manager_Events("beckhoff_bridge")
LEGACY_BR = Manager_Events("br_bridge")


def load_mock_omjson():
    """br_bridge/tests/mock_omjson.py from the checkout the driver was installed from."""
    folder = os.environ.get("FIXCHECK_BR_TESTS")
    if not folder:
        import br_bridge
        folder = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(br_bridge.__file__))), "tests")
    path = os.path.join(folder, "mock_omjson.py")
    spec = importlib.util.spec_from_file_location("mock_omjson", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def ticks(n):
    for _ in range(n):
        await app.next_update_async()


async def main():
    ctx = omni.usd.get_context()
    system = get_system()
    delivery = system.delivery
    print("  components   before stage open: {}".format(system.get_component_names()))

    mock = None
    if MODE != "inject":
        mock = load_mock_omjson().MockOmjson({
            "TestProg:counter": 7, "TestProg:lreal": 1.5, "TestProg:bool": True,
            "TestProg:structOfStructs": {"var1": 3, "secondStruct": {"bool": True}},
            "TestProg:arr": [10.0, 11.0, 12.0],
        }).start()
        print("  mock omjson  127.0.0.1:{}".format(mock.port))

    # --- 3. components follow the stage: one legacy Beckhoff prim, one neutral B&R prim
    await ctx.open_stage_async(STAGE)
    await ticks(10)
    names = system.get_component_names()
    print("  components   after stage open (no UI, no manual call): {}".format(names))
    if "PLC1" not in names or "BR1" not in names:
        fails.append("expected PLC1 and BR1 runtimes from the stage, got {}".format(names))
        return
    bk = system.get_component("PLC1")
    br = system.get_component("BR1")
    print("  PLC1         driver={} legacy={} path={} vars={}".format(bk.driver_name, bk.legacy, bk.path, len(bk.read_variables)))
    print("  BR1          driver={} legacy={} path={} vars={}".format(br.driver_name, br.legacy, br.path, len(br.read_variables)))
    if not (bk.driver_name == "beckhoff" and bk.legacy and br.driver_name == "br" and not br.legacy):
        fails.append("prim classification wrong")
    if get_plc("PLC1") is not bk.plc or get_plc("BR1") is not br.plc:
        fails.append("get_plc() does not return the runtimes")

    # --- 4. runtime shape -------------------------------------------------------------
    threads = [t for t in threading.enumerate() if t.name.endswith("-plc")]
    print("  threads      {} worker(s) {}, daemon={}".format(len(threads), sorted(t.name for t in threads), [t.daemon for t in threads]))
    if len(threads) != 2 or not all(t.daemon for t in threads):
        fails.append("expected one daemon worker per PLC")
    before = bk.read_variables
    bk.options = {"beckhoff_bridge:RefreshRate": 25}
    ok1 = bk.refresh_rate == 25 and bk.read_variables == before
    bk.options = {"beckhoff_bridge:Variables": " A.b ,C.d" + chr(13) + ","}
    ok2 = bk.read_variables == ["A.b", "C.d"]
    bk.set_read_variables(before)
    bk.refresh_rate = 20
    br.options = {"bridge:RefreshRate": 25}
    ok3 = br.refresh_rate == 25
    br.refresh_rate = 20
    print("  options set  legacy partial={} legacy replace={} neutral={}".format(ok1, ok2, ok3))
    if not (ok1 and ok2 and ok3):
        fails.append("Runtime.options setter broken")
    if mock is not None:
        br.options = {"br:Port": mock.port}
        print("  br:Port      -> {} (driver.port={})".format(mock.port, br.driver.port))
        if br.driver.port != mock.port:
            fails.append("driver option change did not reach the driver")

    # --- 5. data flows: legacy bus, neutral bus, Manager, on_sample_main, latest() -----
    bus = app.get_message_bus_event_stream()
    counts = {"bk_legacy": 0, "bk_neutral": 0, "bk_manager": 0, "br_neutral": 0, "br_legacy": 0,
              "bk_cb": 0, "br_cb": 0, "bk_main": 0, "br_main": 0}
    main_threads = set()
    seqs = []

    def count(key):
        return lambda ev: counts.__setitem__(key, counts[key] + 1)

    subs = [
        # what a 0.2.x script does under the hood: the vendor bus name as a string
        bus.create_subscription_to_push_by_type(get_stream_name(LEGACY_BK.EVENT_TYPE_DATA_READ, "PLC1"), count("bk_legacy")),
        bus.create_subscription_to_push_by_type(get_stream_name(EVENT_TYPE_DATA_READ, "PLC1"), count("bk_neutral")),
        bus.create_subscription_to_push_by_type(get_stream_name(EVENT_TYPE_DATA_READ, "BR1"), count("br_neutral")),
        bus.create_subscription_to_push_by_type(get_stream_name(LEGACY_BR.EVENT_TYPE_DATA_READ, "BR1"), count("br_legacy")),
    ]
    # a 0.2.x-style Manager on the legacy namespace
    manager = Manager("PLC1", namespace="beckhoff_bridge")
    manager.register_data_callback(count("bk_manager"))
    removers = [
        bk.plc.on_sample(count("bk_cb")),
        br.plc.on_sample(count("br_cb")),
        on_sample_main("PLC1", lambda s: (counts.__setitem__("bk_main", counts["bk_main"] + 1),
                                          main_threads.add(threading.current_thread() is MAIN_THREAD),
                                          seqs.append(s.seq))),
        on_sample_main("BR1", lambda s: counts.__setitem__("br_main", counts["br_main"] + 1)),
    ]
    status = {"bk": [], "br_neutral": [], "br_legacy": []}
    bk.plc.on_connection(status["bk"].append)
    bk.plc.on_status(status["bk"].append)
    subs.append(bus.create_subscription_to_push_by_type(
        get_stream_name(EVENT_TYPE_STATUS, "BR1"), lambda ev: status["br_neutral"].append(ev.payload["status"])))
    subs.append(bus.create_subscription_to_push_by_type(
        get_stream_name(LEGACY_BR.EVENT_TYPE_STATUS, "BR1"), lambda ev: status["br_legacy"].append(ev.payload["status"])))

    frames0 = delivery.frames
    if MODE == "inject":
        print("  mode         injecting synthetic DATA_READ events (no PLC)")
        for i in range(6):
            bus.push(event_type=get_stream_name(LEGACY_BK.EVENT_TYPE_DATA_READ, "PLC1"), payload={
                "meta": {"name": "PLC1"},
                "data": {"GVL_Moonlight": {"Axes": [{"ActualPosition": 15.0 + i}], "Command": {"Blend": 0.5}}}})
            await app.next_update_async()
    else:
        bk.enable_communication = True
        br.enable_communication = True
        t0 = time.time()
        while time.time() - t0 < LIVE_SEC:
            await app.next_update_async()
    frames = delivery.frames - frames0
    latest_bk, latest_br = bk.plc.latest(), br.plc.latest()
    print("  live {:.0f}s      PLC1: legacy bus={} neutral bus={} Manager(legacy)={} on_sample={} latest.seq={}".format(
        LIVE_SEC, counts["bk_legacy"], counts["bk_neutral"], counts["bk_manager"], counts["bk_cb"],
        latest_bk.seq if isinstance(latest_bk, Sample) else None))
    print("               BR1:  legacy bus={} neutral bus={} on_sample={} latest.seq={}".format(
        counts["br_legacy"], counts["br_neutral"], counts["br_cb"],
        latest_br.seq if isinstance(latest_br, Sample) else None))
    gaps = sum(b - a - 1 for a, b in zip(seqs, seqs[1:]))
    print("  main thread  frames={} PLC1 on_sample_main={} BR1 on_sample_main={} main_thread_only={} seq gaps={}".format(
        frames, counts["bk_main"], counts["br_main"], main_threads == {True}, gaps))
    print("  status       PLC1 {}".format(status["bk"][:6]))
    print("  status       BR1 neutral {}".format(status["br_neutral"][:3]))
    print("  status       BR1 legacy  {}".format(status["br_legacy"][:3]))
    if MODE != "inject":
        for key in ("bk_legacy", "bk_neutral", "bk_manager", "br_neutral", "br_legacy"):
            if counts[key] == 0:
                fails.append("no DATA_READ on {}".format(key))
        if counts["bk_cb"] < 0.6 * LIVE_SEC * 50:
            fails.append("PLC1 sample rate well below 50 Hz: {} in {}s".format(counts["bk_cb"], LIVE_SEC))
        if counts["br_cb"] < 0.6 * LIVE_SEC * 50:
            fails.append("BR1 sample rate well below 50 Hz: {} in {}s".format(counts["br_cb"], LIVE_SEC))
        if main_threads != {True}:
            fails.append("on_sample_main delivered off the main thread")
        expected = min(frames, counts["bk_cb"])
        if not (counts["bk_main"] <= frames + 1 and counts["bk_main"] <= counts["bk_cb"] and counts["bk_main"] >= 0.5 * expected):
            fails.append("on_sample_main count {} not about once per frame (frames={}, samples={})".format(
                counts["bk_main"], frames, counts["bk_cb"]))
        if counts["bk_cb"] > frames and gaps == 0:
            fails.append("samples outnumber frames but on_sample_main showed no seq gaps")
        if "Connected" not in status["bk"]:
            fails.append("PLC1 never reported Connected")
        if not any(isinstance(s, dict) and s.get("kind") for s in status["br_neutral"]) and status["br_neutral"]:
            fails.append("neutral STATUS payload is not the structured Problem")
        if any(not isinstance(s, str) for s in status["br_legacy"]):
            fails.append("legacy STATUS payload is not the 0.2.x text")

    # --- 6. mirror: nested value and array element for both PLCs ----------------------
    stage = ctx.get_stage()
    checks = {
        "PLC1 nested": "/PLC/PLC1/GVL_Moonlight/Command/Blend",
        "PLC1 array ": "/PLC/PLC1/GVL_Moonlight/Axes/_0/ActualPosition",
        "BR1 nested ": "/PLC/BR1/TestProg/structOfStructs/secondStruct/bool",
        "BR1 array  ": "/PLC/BR1/TestProg/arr/_1",
        "BR1 scalar ": "/PLC/BR1/TestProg/lreal",
    }
    for label, path in checks.items():
        prim = stage.GetPrimAtPath(path)
        valid = bool(prim and prim.IsValid())
        value = prim.GetAttribute("value").Get() if valid else None
        symbol = prim.GetAttribute("symbol").Get() if valid else None
        print("  mirror {} {} valid={} value={} symbol={}".format(label, path, valid, value, symbol))
        if MODE != "inject" or label.startswith("PLC1"):
            if not valid or value is None:
                fails.append("{} not mirrored at {}".format(label.strip(), path))
    br_lreal = stage.GetPrimAtPath("/PLC/BR1/TestProg/lreal")
    if br_lreal and br_lreal.IsValid() and br_lreal.GetAttribute("symbol").Get() != "TestProg:lreal":
        fails.append("B&R mirror prim lost the ':' in its symbol")

    # --- 7. write-back: a write:value edit becomes a write under the PLC's own symbol ---
    writes = {"bk": [], "br": []}
    removers.append(bk.plc.on_write(writes["bk"].append))
    removers.append(br.plc.on_write(writes["br"].append))
    blend = stage.GetPrimAtPath("/PLC/PLC1/GVL_Moonlight/Command/Blend")
    if blend and blend.IsValid():
        attr = blend.GetAttribute("write:value")
        new_value = (attr.Get() or 0.0) + 1.0
        attr.Set(new_value)
        t0 = time.time()
        while time.time() - t0 < 2.0 and not writes["bk"]:
            await app.next_update_async()
        sent = [w for w in writes["bk"] if "GVL_Moonlight.Command.Blend" in w.values]
        print("  write-back   PLC1 value={} sent={}".format(new_value, sent[-1] if sent else writes["bk"]))
        if MODE != "inject" and (not sent or sent[-1].error or sent[-1].errors):
            fails.append("PLC1 write:value edit was not written")
        if MODE != "inject":
            handle = bk.queue_write("GVL_Moonlight.Command.Blend", new_value)
            got = handle.wait(2.0)
            print("  write ack    PLC1 done={} ok={} error={}".format(got, handle.ok, handle.error))
            if not (got and handle.ok):
                fails.append("PLC1 live write not acknowledged")
    else:
        print("  write-back   PLC1 skipped: no Command/Blend prim")
    if br_lreal and br_lreal.IsValid():
        attr = br_lreal.GetAttribute("write:value")
        new_value = (attr.Get() or 0.0) + 1.0
        attr.Set(new_value)
        t0 = time.time()
        while time.time() - t0 < 2.0 and not writes["br"]:
            await app.next_update_async()
        sent = [w for w in writes["br"] if "TestProg:lreal" in w.values]
        mock_writes = [r for r in (mock.requests if mock else []) if r.get("type") == "write"]
        print("  write-back   BR1 value={} sent={} mock saw={}".format(new_value, sent[-1] if sent else writes["br"], mock_writes[-1:] if mock_writes else None))
        if not sent or sent[-1].error or sent[-1].errors:
            fails.append("B&R write:value edit was not written as TestProg:lreal (colon)")
        if mock is not None and not any("TestProg:lreal" in r.get("data", {}) for r in mock_writes):
            fails.append("the mock OMJSON server did not receive the TestProg:lreal write")
    else:
        print("  write-back   BR1 skipped: no TestProg/lreal prim")

    # --- 8. autoConnect = false: enabled prims come up disabled ------------------------
    settings = carb.settings.get_settings()
    for path in ("/PLC/PLC1", "/PLC/BR1"):
        prim = stage.GetPrimAtPath(path)
        for name in ("beckhoff_bridge:Enable", "bridge:Enable"):
            a = prim.GetAttribute(name)
            if a.IsValid():
                a.Set(True)
    settings.set("/exts/loupe.simulation.bridge/autoConnect", False)
    system.cleanup()
    system.find_and_create_components()
    await ticks(5)
    bk2, br2 = system.get_component("PLC1"), system.get_component("BR1")
    held = {n: (r.enable_communication, bool(r.held)) for n, r in (("PLC1", bk2), ("BR1", br2))}
    print("  autoConnect  false -> enabled/held {}".format(held))
    if held != {"PLC1": (False, True), "BR1": (False, True)}:
        fails.append("autoConnect=false did not hold the enabled prims")
    settings.set("/exts/loupe.simulation.bridge/autoConnect", True)
    if mock is not None:
        br2.options = {"br:Port": mock.port}
    system.cleanup()
    system.find_and_create_components()
    bk3, br3 = system.get_component("PLC1"), system.get_component("BR1")
    if mock is not None:
        br3.options = {"br:Port": mock.port}
    await ticks(30)
    print("  autoConnect  true  -> enabled {} connected {}".format(
        (bk3.enable_communication, br3.enable_communication), (bk3.is_connected, br3.is_connected)))
    if not (bk3.enable_communication and br3.enable_communication):
        fails.append("autoConnect=true did not enable the prims")

    # --- 9. disable disconnects ---------------------------------------------------------
    bk3.enable_communication = False
    br3.enable_communication = False
    await ticks(30)
    print("  connected    PLC1={} BR1={} after disable".format(bk3.is_connected, br3.is_connected))
    if bk3.is_connected or br3.is_connected:
        fails.append("still connected after disable")
    for remove in removers:
        remove()
    subs.clear()
    manager.cleanup()
    if mock is not None:
        mock.stop()

    print("-" * 68)
    if fails:
        print("FAIL -- " + "; ".join(fails))
    else:
        print("OK -- all fix checks passed")
    print("=" * 68)


async def run():
    try:
        await main()
    except Exception as e:
        import traceback
        traceback.print_exc()
        print("FAIL -- exception: {!r}".format(e))
    sys.stdout.flush()
    print("posting quit at {}".format(time.time()), flush=True)
    app.post_quit()
    # omni.kit.window.file cancels a headless quit on a dirty stage (the test
    # edits the prims); do not let that keep the process alive.
    threading.Thread(target=lambda: (time.sleep(15), os._exit(7)), daemon=True).start()


asyncio.ensure_future(run())
