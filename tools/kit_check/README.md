# Headless Kit check

Runs `loupe.simulation.bridge` inside a built Kit app without a window, opens
a stage with two PLC prims, and checks: startup, driver registration, prim
discovery (one legacy Beckhoff prim, one neutral B&R prim) under one System,
one daemon worker per PLC, the options setter in both spellings, data
delivery on the legacy and neutral bus names, through `Manager`, `on_sample`
and `latest()`, `on_sample_main` on the main thread at the frame rate, the
mirror (nested value and array element for both PLCs), write-back under the
PLC's own symbol spelling (`TestProg:lreal` keeps its colon), write
acknowledgement, the `autoConnect` setting, and disconnect on disable.

Copied from the Beckhoff repo's `tools/kit_check` (Phase 1 of its
`docs/IMPLEMENTATION_PLAN.md`) and extended for the framework.

| File | Role |
|---|---|
| `kit_check.py` | the check, run inside Kit with `--exec`; reads `FIXCHECK_STAGE` and `FIXCHECK_MODE` |
| `fixcheck.kit.template` | a USD Composer app with the framework as a dependency; `${FIXCHECK_EXTS}` and `${FIXCHECK_KIT_ROOT}` are filled in |
| `run.sh`, `run.ps1` | generate the `.kit` in a temp folder, run `kit.exe`, print the check's lines, exit 0 on `OK` |
| `stages/mixed_test.usda` | `/PLC/PLC1` with 0.2.x `beckhoff_bridge:*` attributes at `127.0.0.1.1.1`, `/PLC/BR1` with `bridge:driver = "br"` |

## Running

You need a kit-app-template build (the folder holding `kit/kit.exe`, usually
`_build/windows-x86_64/release`) whose `extscache` has USD Composer's
extensions, and the vendor driver libraries in Kit's Python, since the check
registers them itself rather than loading the vendor extensions:

```
python tools/dev_link.py <kit build root> --driver <Beckhoff repo>/beckhoff_bridge --driver <B&R repo>/br_bridge
```

(or `pip install --no-deps -e` the two driver checkouts plus `pyads` and
`websockets` into `kit/python`, which leaves `plc_bridge` to the bundled wheel
and so exercises the registry layout.)

```powershell
tools\kit_check\run.ps1 -Kit D:\kit-app-template\_build\windows-x86_64\release
tools\kit_check\run.ps1 -Kit ... -Mode inject      # no PLC: synthetic data for PLC1
```

```bash
tools/kit_check/run.sh --kit D:/kit-app-template/_build/windows-x86_64/release --log live.log
```

Options: `--kit` / `-Kit` (required), `--exts` (default: this repo's `exts/`),
`--stage` (default: `stages/mixed_test.usda`), `--mode inject|live`, `--log`
(default: `kit_check.log` in the current folder). Each has an environment
variable fallback: `FIXCHECK_KIT_ROOT`, `FIXCHECK_EXTS`, `FIXCHECK_STAGE`,
`FIXCHECK_MODE`, `FIXCHECK_LOG`.

A run takes about 40 s and ends with `OK -- all fix checks passed` or
`FAIL -- ...`. Kit's exit code is 7 by design: the script quits the app and,
because `omni.kit.window.file` can cancel a headless quit on a dirty stage (the
check edits the prims), forces the exit after 15 s. The launchers exit 0 on `OK`.

Live mode expects the Beckhoff PLC program behind the stage's symbols
(`GVL_Moonlight.Command.*`, `GVL_Moonlight.Axes[i].ActualPosition`) at the
local TwinCAT runtime. The B&R side runs against a mock OMJSON server the
check starts in-process from `br_bridge/tests/mock_omjson.py` (found next to
the installed `br_bridge` checkout, or at `FIXCHECK_BR_TESTS`); the prim's
`br:Port` is overridden with the mock's port through the options setter.

## Kit tests

The extension's `omni.kit.test` suite (a fake driver, no PLC) runs with
`tools\kit_test.ps1 -Kit <kit build root>`.
