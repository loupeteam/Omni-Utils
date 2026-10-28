# Consuming PLC data in Kit

Four ways to get at what a PLC reads, from the `loupe.simulation.bridge`
extension. Pick by what thread you are on and whether you need every sample.

| Way | Thread | What you get | When to use |
|---|---|---|---|
| `on_sample_main(name, cb)` | main, once per app update | the newest `Sample` since the last update | **the default for Kit code**: anything that touches the stage, `omni.ui`, or OmniGraph |
| `get_plc(name).on_sample(cb)` | the PLC's worker thread, every sample | every `Sample`, at the scan rate | edge detection on short pulses, logging, anything that must not miss a packet and does not touch Kit objects |
| `get_plc(name).latest()` | any | the newest `Sample` or `None` | pull from your own loop (a physics step, a timer); compare `seq` to see whether it is new |
| the message bus (`Manager`, carb events) | the worker thread (pushes), your subscription's thread | `data` nested; `status` as the structured problem dict on the neutral names, as the 0.2.x text on the vendor names | scripts written against 0.2.x; code that must not import the extension |

## The Sample

```python
sample.seq        # counts up by one per read for the life of the runtime
sample.t          # time.monotonic() when the read returned
sample.values     # flat: {"GVL.Axes[0].ActualPosition": 12.5, ...}, as the PLC named them
sample.errors     # flat: symbol -> SymbolError for the symbols the PLC failed in that read
sample.missing    # the symbols the PLC does not know (error code "missing"), sorted
sample.nested     # nested: sample.nested["GVL"]["Axes"][0]["ActualPosition"]
```

`values` is the canonical shape; `nested` is built on first use. A symbol is
in `values` or in `errors`, never both, and an error is never delivered as a
value.

A Sample is **read-only**, because the same object goes to every listener and
to `latest()`:

| Field | Type | Notes |
|---|---|---|
| `values`, `errors` | `types.MappingProxyType` | indexing, `in`, `.items()` and `==` with a dict work; assignment raises `TypeError`. A value the driver returned whole (an array as a list) is not copied. |
| `nested` | `MappingProxyType` all the way down, arrays as **tuples** | `nested["GVL"]["Axes"][0]` works; a sparse array keeps `None` where nothing was read. A tuple does not compare equal to a list, and `isinstance(x, dict)` is false: test for `collections.abc.Mapping`. |

For a copy you can change, use `dict(sample.values)` or
`plc_bridge.thaw(sample.nested)` (plain dicts and lists). The message bus
already sends such a copy.

### Symbol errors

Each entry in `Sample.errors`, `WriteResult.errors` and a failed write's
`WriteHandle.error` is a `plc_bridge.SymbolError`: a `str` holding the
vendor's text, plus `.code`:

| Code | Meaning | Beckhoff (ADS) | B&R (OMJSON) |
|---|---|---|---|
| `missing` | the PLC does not know the symbol | ADS error 1808 (symbol not found) | `undefined`, not in the response, or a write to a symbol last read as missing |
| `rejected` | the PLC knows the symbol but refused the value | any other per-symbol ADS error on a write | a write the PLC answered with an error |
| `other` | anything else reported for one symbol | any other per-symbol ADS error on a read | any other per-symbol read error |

```python
from plc_bridge import ERROR_MISSING

for name, error in sample.errors.items():
    if error.code == ERROR_MISSING:
        ...                     # sample.missing lists exactly these
```

Because a SymbolError is a `str`, code that printed or compared the text keeps
working; new code reads `.code`.

## on_sample_main

```python
from loupe.simulation.bridge import on_sample_main

def on_plc(sample):
    jaw.GetAttribute("xformOp:rotateX").Set(sample.values["GVL.Axes[0].ActualPosition"])

remove = on_sample_main("PLC1", on_plc)    # before or after the stage is open
...
remove()
```

It runs on the main thread, once per `omni.kit.app` update, with the newest
sample the PLC produced since the previous update. **It drops the samples in
between**: at 15 fps with a 20 ms scan a callback sees one sample in three.
`Sample.seq` says how many were skipped (`seq - last_seq - 1`). Nothing is
called for a frame without a new sample. The registration is by PLC name and
survives stage reloads: the callback is attached when a PLC of that name
appears and detached when it goes.

Use it for everything that writes USD or drives widgets. Those are not safe
from any other thread.

## Worker-thread callbacks and latest()

```python
from loupe.simulation.bridge import get_plc

plc = get_plc("PLC1")                       # plc_bridge.PlcRuntime, or None
plc.on_sample(lambda s: log.append(s))      # every sample, worker thread
plc.on_problem(lambda p: ...)               # Problem(kind, text, symbols)
plc.on_connection(lambda state: ...)        # "Connecting" / "Connected" / "Disconnected"
s = plc.latest()                            # any thread
```

Rules for a worker-thread listener: do not touch the stage, `omni.ui`, or
anything else Kit owns; keep it short (it delays the next scan); do not block.
A listener that raises is logged and does not disturb the polling. To get the
data onto the main thread yourself, store it and read it from an
`omni.kit.app` update subscription, which is what `on_sample_main` does.

Branch on `Problem.kind` (`connect`, `read`, `write`, `ok`), `Problem.symbols`
and the error codes in `Sample.errors` / `WriteResult.errors` (`missing`,
`rejected`, `other`), never on `Problem.text` or the error text: it carries the driver's own
wording ("symbol not found" from ADS, "undefined" from OMJSON, exception
messages that change between library versions), so the same failure reads
differently on another vendor. The text is for people.

`latest()` is the pull form: cheap, thread-safe, returns the same object until
a new read lands. A physics callback at 240 Hz against a 50 Hz PLC reads the
same sample four or five times; compare `seq`.

## Writing

```python
handle = plc.queue_write("GVL.Command.Blend", 1.0)   # any thread
handle.wait(1.0); handle.ok; handle.error            # optional
```

Writes go out on the next scan, before that scan's read, so the following
sample reflects them. A later write of the same symbol replaces a pending one
(its handle resolves with `"superseded"`).

Writes are never held for a later connection. A write queued while the PLC is
not connected, or while it is disabled, resolves at once with
`handle.error == "not connected"` (`plc_bridge.WRITE_NOT_CONNECTED`), and
nothing is sent. A write still pending when the connection drops or the PLC is
disabled resolves the same way; one pending at `stop()` resolves with
`"stopped"`. A write the PLC failed carries a SymbolError with code `missing`
or `rejected`. `plc.on_write(cb)` reports every
flushed batch as a `WriteResult`. Spell the symbol as the PLC knows it
(`TestProg:lreal` on B&R).

## The message bus

The 0.2.x surface, kept for scripts that have it. Events are
`loupe.simulation.bridge.<KIND>.<plc>` with KIND one of `DATA_INIT`,
`DATA_READ`, `CONNECTION`, `STATUS`, `ENABLE`, `WRITE`, and the requests
`DATA_READ_REQ`, `DATA_WRITE_REQ`. While the setting
`/exts/loupe.simulation.bridge/legacyBusNames` is on (0.3 default) the
vendor names (`loupe.simulation.beckhoff_bridge.*`, `loupe.simulation.br_bridge.*`)
are pushed and accepted as well.

```python
from loupe.simulation.bridge import Manager

m = Manager("PLC1")
m.register_data_callback(lambda ev: print(ev.payload["data"]["GVL"]["Axes"][0]))
m.add_cyclic_read_variables(["GVL.Axes[0].ActualPosition"])
m.write_variable("GVL.Command.Blend", 1.0)
```

The pushes happen on the worker thread, so a bus subscriber runs there too
(the same rules as a worker-thread callback). `STATUS` carries the structured
`Problem` (`{"kind", "text", "symbols"}`) on the neutral name and the text on
a vendor name. As with `on_problem`, branch on `kind` and `symbols`; the text
differs between vendors.

## The System

`get_system()` returns the framework's System. Its public API is three
methods; everything else on it is internal and may change in any release.

| Method | What it does |
|---|---|
| `get_component(name)` | the Runtime adapter of one PLC (`.plc` is the `PlcRuntime`), or None |
| `get_component_names()` | the names of the PLCs that have a Runtime |
| `add_component(name, options, author_prim=True, driver=None)` | create a PLC from code; `options` is a dict of prim attribute names (`bridge:enable`, `bridge:periodMs`, `bridge:variables`, `beckhoff:amsNetId`, ...); `author_prim=False` keeps it off the stage |

For data, prefer `get_plc(name)` and `on_sample_main(name, cb)`.

## Thread rules, summarised

| Where you are | Safe to call |
|---|---|
| main thread | everything |
| worker thread (callbacks, bus subscribers) | `latest()`, `queue_write`, `read_variables`, your own data structures under your own lock; **not** USD, `omni.ui`, settings |
| another thread of yours | `latest()`, `queue_write`, `on_*` registration |

`enable_communication`, `refresh_rate`, `set_read_variables` and the option
setters are intended for the main thread (they are what the window and the
prims drive) but are safe to call from a worker listener.
