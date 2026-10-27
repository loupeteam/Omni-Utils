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
plc.driver.close()   # once you are done with the driver for good
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
`read(symbols) -> ReadResult` and `write(values) -> errors`; override `close`
when the driver holds something for its whole life (an event-loop thread).
The rules, from the class docstring:

| Rule | Meaning |
|---|---|
| Synchronous | Every method blocks. A driver on an async transport (websockets) owns its event loop on a thread of its own and hides it. |
| Bounded | Every method returns or raises within a timeout the driver chooses, a few seconds at most, and `disconnect` unblocks a call in flight (or the transport timeout is short enough to stand in for that). `stop()` waits two seconds for the worker, then calls `disconnect` itself. |
| One caller at a time | `connect`, `read` and `write` come from the runtime's single worker, in turn: writes first, then the read, every period. Only `disconnect` may arrive from another thread, while a call is in flight. |
| Stateless read list | The symbols arrive with each `read`, never empty. |
| One call, any number of requests | The driver may split a batch as its transport requires. |
| Flat in, flat out | `ReadResult.values` is flat symbol name to value. The runtime nests it, so every vendor's data has the same shape. A value may itself be a list or dict when the vendor reads a whole array or struct as one symbol: B&R (OMJSON) returns structs and arrays whole; ADS returns arrays of a primitive type as a list, but a struct only when the driver was given a pyads `structure_def` for it, so with ADS read struct members one by one. |
| Errors are not values | A symbol the PLC rejects goes in `ReadResult.errors`, or in the dict `write` returns. A failed connection or request raises; the runtime then asks `is_connected()` and reconnects if the transport is gone. |
| Closed by the host | `disconnect` leaves the driver reusable; `close` (default: nothing) releases what it holds for good. The runtime never calls `close`, since it can be started again; whoever created the driver calls it after the last `stop()`. |
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
write wakes the loop so it goes out at once. The thread is a daemon.

`stop()` waits at most two seconds (`JOIN_TIMEOUT_SEC`) for the worker, then
calls `driver.disconnect()`, which closes the link and unblocks a stuck call.
Its worst case is therefore two seconds plus the driver's disconnect bound:
milliseconds for the ADS driver, up to the driver's `timeout` + 1 s (4 s by
default) for the B&R driver when the PLC stopped answering. A host that must
not block that long (a UI thread) calls `stop()` from a thread of its own; the
Omniverse framework extension does. A worker that outlives the join exits on
its own when the call returns, and a later `start()` is not confused by it.

`scan()`, `scan_write()` and `scan_read()` run one iteration without a thread,
for tests and for hosts that bring their own scheduling.

## Tests

```bash
pip install -e .[test]
pytest
```

## Where this sits

This package lives in Omni-Utils and is distributed as the `plc-bridge` wheel:
`.github/workflows/plc-bridge.yml` tests it on Python 3.10 and 3.12, builds the
wheel on every push, and publishes it to PyPI on a `plc-bridge-v<version>` tag
once a `PYPI_TOKEN` secret exists. The vendor extensions list it as a pip
requirement. The direction (framework extension, no submodule) is described in
`docs/ARCHITECTURE_PLAN.md` of the Beckhoff bridge repo.
