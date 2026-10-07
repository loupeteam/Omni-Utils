# Changelog

## [0.3.0-rc1] - 2026-10-07

First release of the framework extension. Until now the files in this folder
were vendored into each vendor extension as a git submodule
(`loupe/simulation/common`); they are now one extension loaded once, with the
vendor code behind a driver registry.

### Added
- Driver registry: `registry.register(name, driver_class, option_schema, defaults, ui_panel)`
  with `Option(key, kind, default, label, secret=False)`. A `secret` option
  holds an `env:NAME` or `setting:/path` reference on the prim, never the value.
- Neutral prim schema: `bridge:driver`, `bridge:Enable`, `bridge:RefreshRate`,
  `bridge:Variables` (a `string[]`), `bridge:MirrorToUsd`, `bridge:MirrorSymbols`,
  and the driver's options under its namespace (`beckhoff:AmsNetId`, `br:Host`).
  A PLC prim is recognised by `bridge:driver`; `/PLC/<name>` is the convention.
- One `Runtime` adapter over `plc_bridge.PlcRuntime` for every driver, in
  place of the vendor extensions' copies.
- `get_system()`, `get_plc(name)` and `on_sample_main(name, cb)`: the newest
  sample once per app update, on the main thread.
- The bus as a listener (`BusAdapter`) on the neutral names
  `loupe.simulation.bridge.<KIND>.<plc>`; the structured `Problem` goes out
  under `STATUS` as `{"kind", "text", "symbols"}`, and a `WRITE` event
  reports each flushed write batch.
- Setting `/exts/loupe.simulation.bridge/autoConnect` (default true): when
  false, enabled prims come up disabled and the window says why.
- Setting `/exts/loupe.simulation.bridge/legacyBusNames` (default true):
  also push the 0.2.x vendor bus names and accept requests on them.
- The window is vendor-neutral; a driver's `ui_panel` adds its options, or
  the framework builds a field per option of the schema.
- Kit tests (`omni.kit.test`) and the headless harness `tools/kit_check`
  with a mixed stage (a legacy Beckhoff prim and a neutral B&R prim).

### Changed
- The USD mirror is a registered component, created when `bridge:MirrorToUsd`
  is true (default on in 0.3, off from 0.4), fed by `on_sample_main` instead
  of the bus. It uses the sample's flat values directly: a write-back from a
  mirror prim goes out under the symbol the value was read with, so a B&R
  `TestProg:lreal` keeps its colon (0.2.x re-derived `TestProg.lreal`). Arrays
  read whole and `None`-padded arrays mirror as `_<index>` prims.
- `bridge:MirrorSymbols` narrows the mirror to a watch list.
- `plc-bridge` is a pip requirement pinned exactly by this extension; the
  wheel ships in `wheels/` until the package is on PyPI.

### Compatibility
- Do not enable a 0.2.x vendor extension next to this one: both own the same
  legacy prims (two runtimes and two ADS connections per PLC, every bus event
  twice, two mirrors, a `write:value` edit written twice). Phase 4 ships thin
  vendor extensions that depend on this one.
- `get_system().get_component(name)` keeps `enable_communication`,
  `refresh_rate` / `refresh_period_ms`, `read_variables`,
  `set_read_variables`, `queue_write`, `is_connected`, `plc`, `driver`,
  `name`. `options` now returns the neutral keys with `bridge:Variables` as a
  list; the vendor properties `ams_net_id`, `host`, `port` are gone (use
  `driver_options`, `set_driver_option`, or the driver object).

### Deprecated
- 0.2.x prims (`beckhoff_bridge:*`, `br_bridge:*`) are read as the matching
  driver with one warning per prim. 0.4 reads them only behind a setting;
  0.5 removes them.
- The vendor bus names, same schedule, behind `legacyBusNames`.

### Removed
- `Manager()` with no name (deprecated in 0.2.0).
- `RuntimeUsd` reading from the bus, `flatten_obj`, `get_options_from_prim`,
  `set_options_on_prim`.
- `RuntimeBase.py` is not part of this extension; it stays at the repo root
  for the vendor submodules until Phase 4 removes them.
