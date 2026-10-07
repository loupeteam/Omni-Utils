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
plc.on_sample(lambda s: print(s.seq, s.nested["GVL"]["Axes"][0]["ActualPosition"]))
plc.on_problem(print)
plc.start()
...
handle = plc.queue_write("GVL.Command.Blend", 1.0)
handle.wait(1.0); print(handle.ok)
...
plc.stop()
```

## What a consumer gets

| Call | What arrives |
|---|---|
| `on_sample(cb)` | a `Sample` per successful read: `seq` (counts up), `t` (monotonic), flat `values`, per-symbol `errors`, and `nested` (the values as dicts and lists, built on first use). |
| `on_data(cb)` | `Sample.nested` only; the 0.2.x shape. |
| `latest()` | the newest `Sample`, from any thread, for consumers that pull once per tick instead of taking every packet. Compare `seq` to see whether it is new. |
| `on_problem(cb)` | a `Problem` (`kind`, `text`, `symbols`) when a problem starts, every 2 s while it persists, and once (`kind == "ok"`) when a read problem clears. |
| `on_status(cb)` | `Problem.text` only; the 0.2.x shape. |
| `on_connection(cb)` | `Connecting`, `Connected`, `Disconnected`. |
| `queue_write(name, value)` | returns a `WriteHandle`; `wait()` blocks until the write went out, `ok` / `error` say how it went. `on_write(cb)` gets a `WriteResult` per flushed batch. |

Listeners run on the runtime's worker thread. A host with a main thread
marshals to it itself, or polls `latest()` from its own loop.

## Writing a driver

Subclass `PlcDriver` and implement `connect`, `disconnect`, `is_connected`,
`read(symbols) -> ReadResult` and `write(values) -> errors`. The rules, from
the class docstring:

| Rule | Meaning |
|---|---|
| Synchronous | Every method blocks. A driver on an async transport (websockets) owns its event loop on a thread of its own and hides it. |
| Bounded | Every method returns or raises within a timeout the driver chooses, a few seconds at most, and `disconnect` unblocks a call in flight (or the transport timeout is short enough to stand in for that). `stop()` waits two seconds, then closes the connection itself. |
| One caller at a time | `connect`, `read` and `write` come from the runtime's single worker, in turn: writes first, then the read, every period. Only `disconnect` may arrive from another thread, while a call is in flight. |
| Stateless read list | The symbols arrive with each `read`, never empty. |
| One call, any number of requests | The driver may split a batch as its transport requires. |
| Flat in, flat out | `ReadResult.values` is flat symbol name to value (a value may itself be a struct or list the vendor read whole). The runtime nests it, so every vendor's data has the same shape. |
| Errors are not values | A symbol the PLC rejects goes in `ReadResult.errors`, or in the dict `write` returns. A failed connection or request raises; the runtime then asks `is_connected()` and reconnects if the transport is gone. |
| `symbol_separators` | `"."` for Beckhoff (`GVL.struct.member`), `":."` for B&R (`Program:struct.member`). |

## Data shape

`nest` turns flat symbols into nested data: `a.b.c` becomes nested dicts,
`arr[2]` a list padded with `None`, `arr[2].x` a dict inside that list. An index
it cannot represent (`a[0,1]`, `a[1][2]`) raises `ValueError` naming the symbol,
which the runtime reports as a read problem. This is the parser the Beckhoff and
B&R bridges each carried a copy of.

## Threads

One worker thread per PLC. Every period it flushes the queued writes and then
reads, in that order, so the sample that follows a write reflects it; a queued
write wakes the loop so it goes out at once. The thread is a daemon, and
`stop()` returns within two seconds even when a driver call is stuck; a worker
that outlives that exits on its own when the call returns, and a later
`start()` is not confused by it.

`scan()`, `scan_write()` and `scan_read()` run one iteration without a thread,
for tests and for hosts that bring their own scheduling.

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
