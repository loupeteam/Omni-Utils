# plc_bridge

The vendor-neutral half of Loupe's PLC bridges, as a plain Python package. It
imports nothing from Omniverse or Kit and has no dependencies.

It holds the two things every vendor bridge shares:

- **The driver contract**, `PlcDriver`: what a vendor library implements.
- **The polling runtime**, `PlcRuntime`: connects, reads a variable list at a
  fixed rate, flushes queued writes, and reports data, status and connection
  changes to listeners.

Because the contract lives here, below Kit, switching a simulation from one PLC
vendor to another is one import and one constructor:

```python
from plc_bridge import PlcRuntime
from beckhoff_bridge import AdsDriver      # or a B&R driver written to the same contract

plc = PlcRuntime(AdsDriver("10.20.30.40.1.1"), refresh_ms=20, enabled=True)
plc.set_read_variables(["GVL.Axes[0].ActualPosition"])
plc.on_data(lambda data: print(data["GVL"]["Axes"][0]["ActualPosition"]))
plc.on_status(print)
plc.start()
...
plc.queue_write("GVL.Command.Blend", 1.0)
...
plc.stop()
```

## Writing a driver

Subclass `PlcDriver` and implement `connect`, `disconnect`, `is_connected`,
`read(symbols) -> ReadResult` and `write(values)`. The rules, from the class
docstring:

| Rule | Meaning |
|---|---|
| Synchronous | Every method blocks. A driver on an async transport (websockets) owns its event loop on a thread of its own and hides it. |
| Bounded | Every method returns or raises within a timeout the driver chooses, a few seconds at most, and `disconnect` unblocks a call in flight. `stop()` only waits two seconds. |
| Two caller threads | `read` comes from the read thread, `write` from the write thread, possibly at once. `connect` comes from the read thread; `disconnect` from the read thread or from whoever calls `stop()`. The driver makes that safe: two connections, or a lock. |
| Stateless read list | The symbols arrive with each `read`, never empty. |
| Flat in, flat out | `ReadResult.values` is flat symbol name to value. The runtime nests it, so every vendor's data has the same shape. |
| Errors are not values | A symbol the PLC rejects goes in `ReadResult.errors`, or in the dict `write` returns. A failed connection or request raises; the runtime then asks `is_connected()` and reconnects if the transport is gone. |
| `symbol_separators` | `"."` for Beckhoff (`GVL.struct.member`), `":."` for B&R (`Program:struct.member`). |

## Data shape

`nest` turns flat symbols into nested data: `a.b.c` becomes nested dicts,
`arr[2]` a list padded with `None`, `arr[2].x` a dict inside that list. An index
it cannot represent (`a[0,1]`, `a[1][2]`) raises `ValueError` naming the symbol,
which the runtime reports as a status. This is the parser the Beckhoff and B&R
bridges each carried a copy of.

## Threads

Listeners run on the runtime's worker threads. A host with a main thread
marshals to it itself. A listener that raises is logged and does not stop the
polling. Both threads are daemons, and `stop()` returns within two seconds in
total even when a read is stuck; a worker that outlives that exits on its own
when the driver call returns, and a later `start()` is not confused by it.

`scan_read()` and `scan_write()` run one iteration each without threads, for
tests and for hosts that bring their own scheduling.

## Tests

```bash
pip install -e .[test]
pytest
```

## Where this sits

This package lives in Omni-Utils, which the vendor extensions still vendor as a
git submodule, so an extension can load it with a `[[python.module]]` path into
the submodule. The direction (framework extension, no submodule) is described in
`docs/ARCHITECTURE_PLAN.md` of the Beckhoff bridge repo.
