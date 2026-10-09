# PLC Bridge (`loupe.simulation.bridge`)

The vendor-neutral PLC bridge for Omniverse Kit. It owns everything a
simulation touches: the `/PLC` prims, the driver registry, polling, main-thread
delivery, the message bus and the USD mirror. Vendor code lives in thin
extensions (`loupe.simulation.beckhoff_bridge`, `loupe.simulation.br_bridge`)
that register a driver with it. A simulation depends on this extension only
and never imports vendor code; switching a machine from one PLC vendor to
another is a change of two attributes on a prim.

The polling itself is the plain-Python [`plc_bridge`](../../../plc_bridge/README.md)
package (`PlcRuntime`, the `PlcDriver` contract), which this extension lists
as a pip requirement and whose exact version it owns.

## Configuring a PLC

A PLC is a prim under `/PLC` carrying `bridge:driver` (the prim path is the
identity; `/PLC/<name>` is the convention and `<name>` is what the APIs take):

```usda
def Scope "PLC"
{
    def Scope "PLC1"
    {
        custom string   bridge:driver        = "beckhoff"
        custom bool     bridge:Enable        = false
        custom int      bridge:RefreshRate   = 20
        custom string[] bridge:Variables     = ["GVL.Axes[0].ActualPosition", "GVL.Command.Blend"]
        custom bool     bridge:MirrorToUsd   = true
        custom string[] bridge:MirrorSymbols = []
        custom string   beckhoff:AmsNetId    = "10.20.30.40.1.1"
    }
    def Scope "PLC2"
    {
        custom string   bridge:driver        = "br"
        custom bool     bridge:Enable        = false
        custom int      bridge:RefreshRate   = 20
        custom string[] bridge:Variables     = ["TestProg:counter", "TestProg:structOfStructs"]
        custom string   br:Host              = "192.168.1.10"
        custom int      br:Port              = 8000
    }
}
```

| Attribute | Type | Default | Meaning |
|---|---|---|---|
| `bridge:driver` | string | required | the registered driver name (`beckhoff`, `br`) |
| `bridge:Enable` | bool | false | connect and poll |
| `bridge:RefreshRate` | int | 20 | scan period in milliseconds |
| `bridge:Variables` | string[] | [] | the symbols read every scan |
| `bridge:MirrorToUsd` | bool | true | mirror the values as prims under the PLC prim (see below) |
| `bridge:MirrorSymbols` | string[] | [] | mirror only these symbols (and everything under them); empty means all |
| `<driver>:<Option>` | per driver | per driver | the driver's options, e.g. `beckhoff:AmsNetId`, `br:Host`, `br:Port` |

A driver option declared `secret` (a password, a token) is never stored in
clear: its attribute holds `env:NAME` or `setting:/path`, which the framework
resolves right before the driver is built. Writing options back to the prim
keeps the reference.

Prims from 0.2.x (`beckhoff_bridge:AmsNetId`, `beckhoff_bridge:Enable`,
`beckhoff_bridge:Variables` as a comma-separated string; likewise
`br_bridge:*`) still work in 0.3 and are read as the matching driver, with one
deprecation warning per prim. 0.4 reads them only behind a setting; 0.5 not
at all. The window's "Write To USD" button rewrites a prim in the neutral form
(the old attributes stay and can be deleted by hand).

## Settings

| Setting | Default | Meaning |
|---|---|---|
| `/exts/loupe.simulation.bridge/autoConnect` | true | When false, a prim with `bridge:Enable = true` still comes up disabled, so a committed stage cannot open a connection to hardware by surprise. The window says why; enable from there or from code. |
| `/exts/loupe.simulation.bridge/legacyBusNames` | true | Also push the vendor-named bus events (`loupe.simulation.beckhoff_bridge.*`, `loupe.simulation.br_bridge.*`) and accept requests on them. |

## Getting the data

See [docs/CONSUMING.md](../../../docs/CONSUMING.md) for the choice between
callbacks, `latest()`, `on_sample_main` and the bus, and the thread rules. In
short:

```python
from loupe.simulation.bridge import get_plc, on_sample_main

remove = on_sample_main("PLC1", lambda sample: print(sample.seq, sample.values))  # main thread, once per frame
plc = get_plc("PLC1")                     # the plc_bridge.PlcRuntime: on_sample, latest(), queue_write, ...
handle = plc.queue_write("GVL.Command.Blend", 1.0)
```

The 0.2.x `Manager("PLC1")` API is still there (`from loupe.simulation.bridge
import Manager`) and talks on the neutral bus names.

## Compatibility with 0.2.x

**Do not enable a 0.2.x vendor extension (`loupe.simulation.beckhoff_bridge`
0.2.x, `loupe.simulation.br_bridge` 0.1.x) alongside this one.** Both would
own the same `/PLC` prims: two runtimes and two ADS connections per PLC,
every bus event pushed twice, two mirrors fighting over the same prims, and
a `write:value` edit written twice. The 0.3.0 vendor extensions depend on
this one and only register a driver; use those.

What a 0.2.x script that reached the runtime through
`get_system().get_component(name)` still finds: `enable_communication`,
`refresh_rate` / `refresh_period_ms`, `read_variables`, `set_read_variables`,
`queue_write`, `is_connected`, `plc`, `driver`, `name`. Narrower than 0.2.x:
`options` returns the neutral keys (`bridge:Enable`, `bridge:RefreshRate`,
`bridge:Variables` **as a list**, `bridge:MirrorToUsd`, `bridge:MirrorSymbols`,
`<driver>:<Option>`), and the vendor properties `ams_net_id`, `host`, `port`
are gone: read `driver_options` or the driver object (`runtime.driver.ams_net_id`),
write with `set_driver_option("AmsNetId", ...)`. The options setter still
accepts the 0.2.x keys (`beckhoff_bridge:Variables` as a comma-separated
string).

## The USD mirror

When `bridge:MirrorToUsd` is true (the default in 0.3; off by default from
0.4) every value read is mirrored under the PLC prim, in the session layer
(never saved, never a pending change):

```
/PLC/PLC1/GVL/Axes/_0/ActualPosition      double value, write:value, bool write:once, write:pause, string symbol
```

Struct members become child prims, array elements `_<index>` children. A
struct or array read as one symbol is expanded the same way. Edit
`write:value` to write to the PLC (set `write:pause` to edit without
sending, `write:once` to send once); the write goes out under the symbol the
value was read with, so a B&R `TestProg:lreal` keeps its colon.

## The window

Loupe > PLC Bridge lists the PLCs of the stage with their driver, enable,
refresh rate, variables, mirror state, connection, status and live data, and
the driver's own options below (from the driver's `ui_panel` when it
registered one, else one field per option). "Update From USD" re-reads the
prim; "Write To USD" writes the current configuration onto it.

## Registering a driver (vendor extensions)

```python
from loupe.simulation.bridge import registry, Option
from beckhoff_bridge import AdsDriver

registry.register(
    "beckhoff", AdsDriver,
    [Option("AmsNetId", "str", "127.0.0.1.1.1", "PLC AMS Net Id")],
    legacy_namespace="beckhoff_bridge",     # 0.2.x prims and bus names
    title="Beckhoff (ADS)",
)
```

`Option(key, kind, default, label, secret=False, arg=None)` with kinds `str`,
`int`, `float`, `bool`, `str_list`. The driver's constructor gets one keyword
per option, `arg` or the key in snake_case (`AmsNetId` -> `ams_net_id`); a
changed option is assigned to the live driver as an attribute of the same
name and the connection is reopened. `registry.unregister(name)` in
`on_shutdown`; the 0.3.0 vendor extensions do exactly that.

The framework creates one driver per PLC prim and owns it: when the stage
closes or the prim goes away it stops the runtime and then calls the driver's
`close()` (`PlcDriver.close`, a no-op by default), on a thread of its own, so a
PLC that stopped answering does not freeze the app for the driver's timeouts.
A vendor extension also calls `check_extension_requirements(ext_id)` from its
`on_startup` (see Installing). This repo's
tests and harness do not load them:
`loupe.simulation.bridge.tests.vendor_drivers.register_vendor_drivers()`
registers both drivers from their libraries instead.

## Installing

`plc-bridge` is a pip requirement. Until it is on PyPI its wheel ships in the
extension's `wheels/` folder: run `python tools/build_wheels.py` at the repo
root before packaging. From a clone, `python tools/dev_link.py <kit build
root>` installs the checkout editable into Kit's Python instead (see the
repo README). One of the two is required: a clean clone with neither falls
through to PyPI, where `plc-bridge==0.3.0` does not exist, and the
extension fails to start.

## Tests

Kit tests (`omni.kit.test`) under `loupe/simulation/bridge/tests/` against a
fake driver, and the headless harness `tools/kit_check/` against a live
Beckhoff PLC and a mock B&R server. See the repo README.
