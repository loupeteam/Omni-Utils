"""Build the plc-bridge wheel the framework extension bundles until it is on PyPI.

loupe.simulation.bridge lists `plc-bridge==<version>` as a pip requirement and
points pipapi's `archiveDirs` at `exts/loupe.simulation.bridge/wheels/`. This
script fills that folder from `plc_bridge/` at the repo root. plc_bridge has
no dependencies, so the wheel alone is the whole closure pipapi needs
(`pip --target --no-index` ignores anything already installed and fails on a
missing dependency).

Run it before packaging the extension for a registry, and again after a
version bump. A plc_bridge wheel already in the folder is removed first, so
a stale version cannot shadow the new one.

Usage:
    python tools/build_wheels.py [--out DIR] [--python EXE]
"""

import argparse
import glob
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXT = os.path.join(ROOT, "exts", "loupe.simulation.bridge")
SRC = os.path.join(ROOT, "plc_bridge")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=os.path.join(EXT, "wheels"),
                    help="archive folder (default: the extension's wheels/)")
    ap.add_argument("--python", default=sys.executable, help="interpreter whose pip builds the wheel")
    args = ap.parse_args(argv)

    if not os.path.isfile(os.path.join(SRC, "pyproject.toml")):
        sys.exit("no pyproject.toml at {}".format(SRC))
    os.makedirs(args.out, exist_ok=True)
    for old in glob.glob(os.path.join(args.out, "plc_bridge-*.whl")):
        os.remove(old)
        print("removed  {}".format(os.path.basename(old)))

    print("building plc_bridge from {}".format(os.path.relpath(SRC, ROOT)))
    subprocess.run([args.python, "-m", "pip", "wheel", "--no-deps", "--wheel-dir", args.out, SRC],
                   check=True, stdout=subprocess.DEVNULL)

    print("\n{}:".format(os.path.relpath(args.out, ROOT)))
    for whl in sorted(glob.glob(os.path.join(args.out, "*.whl"))):
        print("  " + os.path.basename(whl))
    return 0


if __name__ == "__main__":
    sys.exit(main())
