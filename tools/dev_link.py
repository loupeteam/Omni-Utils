"""Run the framework extension from a git clone without building a wheel.

The extension declares `plc-bridge` as a pip requirement. Kit's pipapi tries
to import `plc_bridge.runtime` before it calls pip and skips the install when
the import works, so installing the checkout editable into Kit's own Python
makes Kit use the working tree directly: edit `plc_bridge/src` and restart
the app, no wheel build.

Vendor driver checkouts can be linked in the same call, which is how the
tests and `tools/kit_check` get the Beckhoff and B&R drivers without loading
the vendor extensions:

    python tools/dev_link.py <kit> --driver <path to beckhoff_bridge> --driver <path to br_bridge>

A driver checkout is installed with its dependencies (pyads, websockets);
plc-bridge resolves to the editable checkout in the same pip call, so pip
never looks for it on an index.

Usage:
    python tools/dev_link.py <kit build root | kit/python/python.exe> [--driver DIR ...]
    python tools/dev_link.py <...> --uninstall [--driver DIR ...]

The first form takes the folder that holds `kit/kit.exe` (a kit-app-template
`_build/<platform>/release`), a Kit SDK root, or the interpreter itself.
"""

import argparse
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLC_BRIDGE = os.path.join(ROOT, "plc_bridge")
EXE = "python.exe" if sys.platform == "win32" else "python3"


def kit_python(path):
    """Resolve a Kit build root, Kit SDK root or interpreter path to the interpreter."""
    path = os.path.abspath(path)
    if os.path.isfile(path):
        return path
    for candidate in (
        os.path.join(path, "kit", "python", EXE),          # kit-app-template build
        os.path.join(path, "kit", "python", "bin", EXE),   # same, Linux
        os.path.join(path, "python", EXE),                 # Kit SDK
        os.path.join(path, "python", "bin", EXE),
    ):
        if os.path.isfile(candidate):
            return candidate
    sys.exit("no Kit Python under {}: expected kit/python/{} or python/{}".format(path, EXE, EXE))


def dist_name(src):
    """The pip name of a checkout, from its pyproject.toml."""
    import tomllib
    with open(os.path.join(src, "pyproject.toml"), "rb") as f:
        return tomllib.load(f)["project"]["name"]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("kit", help="Kit build root, Kit SDK root, or its python executable")
    ap.add_argument("--driver", action="append", default=[], metavar="DIR",
                    help="a vendor driver checkout (folder with pyproject.toml) to link as well")
    ap.add_argument("--uninstall", action="store_true", help="remove the editable installs again")
    args = ap.parse_args(argv)

    python = kit_python(args.kit)
    print("Kit Python: {}".format(python))
    sources = [PLC_BRIDGE] + [os.path.abspath(d) for d in args.driver]
    for src in sources:
        if not os.path.isfile(os.path.join(src, "pyproject.toml")):
            sys.exit("no pyproject.toml at {}".format(src))

    if args.uninstall:
        cmd = [python, "-m", "pip", "uninstall", "-y"] + [dist_name(src) for src in sources]
    else:
        cmd = [python, "-m", "pip", "install"]
        for src in sources:
            cmd += ["-e", src]
    print(" ".join(cmd))
    subprocess.run(cmd, check=True)

    if not args.uninstall:
        check = [python, "-c", "import plc_bridge; print(plc_bridge.__file__)"]
        out = subprocess.run(check, check=True, capture_output=True, text=True).stdout
        print("\nKit's Python now imports plc_bridge from:\n" + out)
        print("The extension's wheels/ folder is ignored while this is installed;\n"
              "run again with --uninstall to go back to the bundled wheel.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
