"""
Startup check: are the pip packages Kit loaded the versions this extension
asked for?

Copyright (c) 2024 Loupe, https://loupe.team
Part of Omni-Utils, licensed under the MIT License.

Kit's pip installer (omni.kit.pipapi) decides whether a requirement is met by
importing the module named in `[python.pipapi] modules`. It never compares
versions. A package left in the app's pip folder by an earlier install (a
release candidate, the previous release) therefore keeps being used after an
upgrade, silently, and the bundled wheel of the new version is never
installed. This module compares `importlib.metadata.version()` of each pinned
requirement with its pin and logs an error that names the folder to clear.

The framework checks its own requirements at startup; a vendor extension calls
`check_extension_requirements(ext_id)` from its `on_startup`.
"""

import importlib.metadata
import logging
import os
import re
from typing import Callable, Iterable, List, Optional

logger = logging.getLogger(__name__)

# Indirections the tests replace to fake installed metadata.
_installed_version = importlib.metadata.version


def _installed_location(dist: str) -> Optional[str]:
    try:
        return str(importlib.metadata.distribution(dist).locate_file(""))
    except Exception:
        return None


_REQUIREMENT = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)\s*(?:\[[^\]]*\])?\s*([^;@]*?)\s*(?:;.*)?$")
_CLAUSE = re.compile(r"^\s*(===|==|!=|~=|>=|<=|>|<)\s*(\S+)\s*$")
_VERSION = re.compile(
    r"^\s*v?(\d+(?:\.\d+)*)"
    r"(?:[-_.]?(a|alpha|b|beta|c|rc|pre|preview)[-_.]?(\d*))?"
    r"(?:[-_.]?(post|rev|r)[-_.]?(\d*))?"
    r"(?:[-_.]?(dev)[-_.]?(\d*))?"
    r"(?:\+.*)?\s*$",
    re.IGNORECASE)
_PRE_RANK = {"a": 0, "alpha": 0, "b": 1, "beta": 1, "c": 2, "rc": 2, "pre": 2, "preview": 2}


def _release(text: str) -> tuple:
    parts = [int(p) for p in text.split(".")]
    while len(parts) > 1 and parts[-1] == 0:
        parts.pop()
    return tuple(parts)


def parse_version(text: str) -> tuple:
    """
    A sortable key for a PEP 440 version, enough for the pins used here:
    release numbers, a/b/rc pre-releases, post and dev releases.
    0.3.0rc1 < 0.3.0 < 0.3.0.post1, and 0.3 == 0.3.0.
    """
    m = _VERSION.match(text)
    if m is None:
        raise ValueError(f"not a version: {text!r}")
    release = _release(m.group(1))
    if m.group(2):
        pre = (0, _PRE_RANK[m.group(2).lower()], int(m.group(3) or 0))
    elif m.group(6) and not m.group(4):
        pre = (-1, 0, 0)  # 1.0.dev1 sorts before 1.0a1
    else:
        pre = (1, 0, 0)
    post = int(m.group(5) or 0) if m.group(4) else -1
    dev = int(m.group(7) or 0) if m.group(6) else float("inf")
    return (release, pre, post, dev)


def _matches(installed: str, op: str, wanted: str) -> bool:
    if op in ("==", "!=") and wanted.endswith(".*"):
        prefix = tuple(int(p) for p in wanted[:-2].split("."))
        have = tuple(int(p) for p in _VERSION.match(installed).group(1).split(".")) + (0,) * len(prefix)
        hit = have[:len(prefix)] == prefix
        return hit if op == "==" else not hit
    if op == "===":
        return installed.strip() == wanted.strip()
    have, want = parse_version(installed), parse_version(wanted)
    if op == "==":
        return have == want
    if op == "!=":
        return have != want
    if op == ">=":
        return have >= want
    if op == "<=":
        return have <= want
    if op == ">":
        return have > want
    if op == "<":
        return have < want
    if op == "~=":
        # Compatible release: the clause's own digits, trailing zeros kept
        # (~=0.3.0 means >=0.3.0 and ==0.3.*).
        release = tuple(int(p) for p in _VERSION.match(wanted).group(1).split("."))
        prefix = release[:-1] if len(release) > 1 else release
        mine = tuple(int(p) for p in _VERSION.match(installed).group(1).split(".")) + (0,) * len(prefix)
        return have >= want and mine[:len(prefix)] == prefix
    raise ValueError(f"unknown operator {op!r}")


def satisfies(installed: str, specifier: str) -> bool:
    """True when the version meets every comma-separated clause (">=0.3.0,<0.4")."""
    for clause in filter(None, (c.strip() for c in specifier.split(","))):
        m = _CLAUSE.match(clause)
        if m is None:
            raise ValueError(f"cannot read the version clause {clause!r}")
        if not _matches(installed, m.group(1), m.group(2)):
            return False
    return True


def _how_to_clear(dist: str, location: Optional[str]) -> str:
    if location:
        parts = os.path.normpath(location).split(os.sep)
        lowered = [p.lower() for p in parts]
        if "pip3-envs" in lowered:
            # pipapi installs into <app data>/pip3-envs/<env>; deleting that
            # folder makes Kit install the bundled wheels again on next start.
            end = min(lowered.index("pip3-envs") + 2, len(parts))
            folder = os.sep.join(parts[:end])
            return (f"Quit Kit and delete the folder {folder}; Kit installs the bundled versions again "
                    "on the next start.")
    return (f"Uninstall it from the Python that provides it ({location or 'unknown location'}) with "
            f"`python -m pip uninstall {dist}`, or install the version the extension needs.")


def check_requirements(requirements: Iterable[str], owner: str,
                       log: Optional[Callable[[str], None]] = None) -> List[str]:
    """
    Compare installed versions with the requirements' pins.

    Args:
        requirements: pip requirement strings, as in `[python.pipapi]
            requirements` ("plc-bridge==0.3.0", "pyads"). Those without a
            version clause are skipped.
        owner: who asked for them (the extension name), for the message.
        log: where each problem goes; default logger.error.

    Returns:
        One message per requirement whose installed version does not match.
        A package without installed metadata (a source folder on sys.path)
        cannot be checked and is skipped.
    """
    log = log or logger.error
    problems = []
    for requirement in requirements or ():
        m = _REQUIREMENT.match(str(requirement))
        if m is None or not m.group(2):
            continue
        dist, specifier = m.group(1), m.group(2)
        try:
            installed = _installed_version(dist)
        except importlib.metadata.PackageNotFoundError:
            logger.info("%s: %s has no installed metadata; version not checked", owner, dist)
            continue
        try:
            ok = satisfies(installed, specifier)
        except ValueError as e:
            logger.warning("%s: cannot check %s %s against %r: %s", owner, dist, installed, specifier, e)
            continue
        if ok:
            continue
        location = _installed_location(dist)
        message = (
            f"{owner} needs {dist}{specifier}, but Kit loaded {dist} {installed} from "
            f"{location or 'an unknown location'}. Kit's pip installer only checks that a module imports, "
            f"never its version, so a package left there by an earlier install (a release candidate, an older "
            f"release) is used instead of the bundled one. {_how_to_clear(dist, location)}")
        log(message)
        problems.append(message)
    return problems


def extension_requirements(ext_id: str) -> List[str]:
    """The `[python.pipapi] requirements` of an enabled extension, from Kit's extension manager."""
    import omni.kit.app

    manager = omni.kit.app.get_app().get_extension_manager()
    data = manager.get_extension_dict(ext_id)
    if data is None:
        return []
    if hasattr(data, "get_dict"):
        data = data.get_dict()
    python = (data or {}).get("python") or {}
    pipapi = python.get("pipapi") or {}
    return list(pipapi.get("requirements") or [])


def check_extension_requirements(ext_id: str, log: Optional[Callable[[str], None]] = None) -> List[str]:
    """
    Check an extension's pinned pip requirements against what is installed;
    log an error per mismatch. Vendor extensions call this from on_startup
    with their ext_id. Never raises.

    Returns:
        The messages logged; empty when everything matches.
    """
    owner = ext_id.split("-")[0] if ext_id else "extension"
    try:
        requirements = extension_requirements(ext_id)
    except Exception:
        logger.debug("%s: pip requirements not readable; version check skipped", owner, exc_info=True)
        return []
    try:
        return check_requirements(requirements, owner, log)
    except Exception:
        logger.debug("%s: version check failed", owner, exc_info=True)
        return []
