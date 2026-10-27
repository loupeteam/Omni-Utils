# Omni-Utils

Loupe's vendor-neutral PLC bridge for Omniverse, in two layers:

| Folder | What | Depends on |
|---|---|---|
| [`plc_bridge/`](plc_bridge/README.md) | plain-Python package `plc-bridge`: the `PlcDriver` contract every vendor implements and the `PlcRuntime` that polls it. No Omniverse imports. | nothing |
| [`exts/loupe.simulation.bridge/`](exts/loupe.simulation.bridge/docs/README.md) | the Kit extension: `/PLC` prims, the driver registry, main-thread delivery, the message bus, the USD mirror, the window. | `plc-bridge` (pip), Kit |

Vendor extensions ([Beckhoff](https://github.com/loupeteam/Omniverse_Beckhoff_Bridge_Extension),
[B&R](https://github.com/loupeteam/Omniverse_BnR_Bridge_Extension)) depend on
the Kit extension and register a driver with it; a simulation depends on the
Kit extension only. The design and the decisions are in the Beckhoff repo's
`docs/ARCHITECTURE_PLAN.md`; the work, in phases, in its
`docs/IMPLEMENTATION_PLAN.md`. How to get data out of a PLC in Kit:
[docs/CONSUMING.md](docs/CONSUMING.md).

`RuntimeBase.py` and `Global.py` at the root are what the 0.2.x vendor
extensions still vendor through a git submodule. The 0.3.0 vendor
extensions no longer use them; they go once no supported 0.2.x release needs
them.

## Installing the extension

`loupe.simulation.bridge` lists `plc-bridge==<version>` as a pip requirement
that Kit installs before the extension starts. Two ways to make it available:

**From a registry or a packaged extension.** Until `plc-bridge` is on PyPI its
wheel ships inside the extension, in `exts/loupe.simulation.bridge/wheels/`
(git-ignored). Fill that folder before packaging:

```
python tools/build_wheels.py
```

On first start Kit installs from it with `--no-index` into the app's pip
environment; on later starts it finds the package importable and does nothing.

**From a clone.** Install the checkout editable into the Kit app's own Python:

```
python tools/dev_link.py <kit build root>      # the folder holding kit/kit.exe
```

Edits under `plc_bridge/src` are picked up on the next start. Add
`--driver <path>` for each vendor driver checkout to link (`beckhoff_bridge/`,
`br_bridge/` in their repos), which this repo's Kit tests and harness need:
they register the drivers from the libraries themselves instead of loading the
vendor extensions. `--uninstall` removes
them again (it leaves their dependencies, `pyads` and `websockets`, behind).
`dev_link.py` needs Python 3.11 or newer to run (it reads `pyproject.toml`
with `tomllib`); the Kit Python it installs into is 3.10+ as usual.

One of the two is required. A clean clone with neither the wheel built nor
the dev link falls through to PyPI, where `plc-bridge==0.3.0` does not
exist, and the extension fails to start; CI and registry packaging must run
`tools/build_wheels.py` first.

What Kit's pipapi does with a requirement that is already importable, and the
working-directory trap for a bare package folder at the repo root, is recorded
in the Beckhoff repo's README ("What Kit's pipapi does"); the extension's
`__init__.py` and the launchers under `tools/` guard against it.

## Verification

| Check | How |
|---|---|
| `plc_bridge` unit tests | `pip install -e "./plc_bridge[test]" && pytest plc_bridge` (CI: `.github/workflows/plc-bridge.yml`, Python 3.10 and 3.12) |
| Kit tests of the extension | `tools\kit_test.ps1 -Kit <kit build root>` (omni.kit.test, a fake driver, no PLC) |
| Headless harness | `tools\kit_check\run.ps1 -Kit <kit build root>`: a legacy Beckhoff prim live against TwinCAT and a neutral B&R prim against a mock OMJSON server, see [tools/kit_check/README.md](tools/kit_check/README.md) |
| No Kit imports in the library | `git grep -l "import omni\|import carb" plc_bridge/` returns nothing (also a test) |

## Licensing

Everything under `plc_bridge/`, `tools/` and `docs/`, and `registry.py`,
`schema.py`, `Runtime.py`, `delivery.py`, `bus.py`, `System.py`,
`UsdManager.py`, `BridgeManager.py` in the extension are Loupe's, under the
[MIT License](LICENSE). `extension.py`, `SystemUI.py` and `__init__.py` in the
extension derive from NVIDIA's extension template and carry its header
(NVIDIA Omniverse License Agreement and MIT, whichever is most restrictive).
