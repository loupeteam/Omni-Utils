"""
The PLC prim schema: how a PLC is described on a USD prim, and how the
framework reads it back.

Copyright (c) 2024 Loupe, https://loupe.team
Part of Omni-Utils, licensed under the MIT License.

A PLC prim is any prim that carries `bridge:driver`; `/PLC/<name>` is the
documented place for them, and the prim path is the identity. The neutral
attributes are the same for every vendor:

    custom string   bridge:driver        = "beckhoff"
    custom bool     bridge:Enable        = false
    custom int      bridge:RefreshRate   = 20            # milliseconds
    custom string[] bridge:Variables     = ["GVL.Axes[0].ActualPosition"]
    custom bool     bridge:MirrorToUsd   = true          # absent means true in 0.3
    custom string[] bridge:MirrorSymbols = []            # empty means every variable

and the driver's own options sit under its namespace:

    custom string   beckhoff:AmsNetId    = "127.0.0.1.1.1"
    custom string   br:Host              = "127.0.0.1"
    custom int      br:Port              = 8000

Legacy rule (0.3 only, warned once per prim): a prim with no `bridge:driver`
but with `<legacy_namespace>:*` attributes of a registered driver
(`beckhoff_bridge:AmsNetId`, `br_bridge:Host`) is treated as that driver;
`<legacy_namespace>:Enable / RefreshRate / Variables` stand in for the neutral
attributes, `Variables` being the 0.2.x comma-separated string.

A `secret` option is never stored on a prim: its attribute holds a reference,
`env:NAME` or `setting:/path`, resolved right before the driver is built.
"""

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Optional

from pxr import Sdf, Usd

from . import registry
from .registry import DriverSpec, Option

logger = logging.getLogger(__name__)

ATTR_DRIVER = "bridge:driver"
ATTR_ENABLE = "bridge:Enable"
ATTR_REFRESH = "bridge:RefreshRate"
ATTR_VARIABLES = "bridge:Variables"
ATTR_MIRROR = "bridge:MirrorToUsd"
ATTR_MIRROR_SYMBOLS = "bridge:MirrorSymbols"

#: The neutral attributes with their defaults, in the order the UI shows them.
NEUTRAL_DEFAULTS = {
    ATTR_ENABLE: False,
    ATTR_REFRESH: 20,
    ATTR_VARIABLES: [],
    ATTR_MIRROR: True,
    ATTR_MIRROR_SYMBOLS: [],
}

#: Where PLC prims live by convention.
DEFAULT_ROOT = "/PLC/"

#: The three legacy attributes that stand in for neutral ones, by bare key.
_LEGACY_NEUTRAL = {"Enable": ATTR_ENABLE, "RefreshRate": ATTR_REFRESH, "Variables": ATTR_VARIABLES}

SECRET_ENV = "env:"
SECRET_SETTING = "setting:"


@dataclass
class PlcConfig:
    """
    One PLC prim, read. `options` holds the driver options by bare key, with
    secrets still as references; `legacy` says the prim used the 0.2.x
    attributes.
    """

    name: str
    path: str
    driver: str
    enabled: bool = False
    refresh_ms: Any = 20
    variables: list = field(default_factory=list)
    mirror: bool = True
    mirror_symbols: list = field(default_factory=list)
    options: dict = field(default_factory=dict)
    legacy: bool = False

    def to_options(self, spec: Optional[DriverSpec] = None) -> dict:
        """The flat option dict the Runtime adapter and the UI work with (neutral keys)."""
        options = {
            ATTR_ENABLE: self.enabled,
            ATTR_REFRESH: self.refresh_ms,
            ATTR_VARIABLES: list(self.variables),
            ATTR_MIRROR: self.mirror,
            ATTR_MIRROR_SYMBOLS: list(self.mirror_symbols),
        }
        spec = spec or registry.get(self.driver)
        if spec is not None:
            for key, value in self.options.items():
                options[spec.attribute(key)] = value
        return options


# region - Secrets

def is_secret_reference(value: Any) -> bool:
    return isinstance(value, str) and (value.startswith(SECRET_ENV) or value.startswith(SECRET_SETTING))


def resolve_secret(value: Any, settings=None) -> Any:
    """
    Turn a secret reference into its value: `env:NAME` reads the environment,
    `setting:/path` reads a carb setting. Anything else is returned as given,
    with a warning, since a secret written in clear on a prim is what the
    reference form exists to avoid. A reference that resolves to nothing is
    an error: the driver would otherwise silently get an empty credential.
    """
    if not isinstance(value, str):
        return value
    if value.startswith(SECRET_ENV):
        name = value[len(SECRET_ENV):]
        resolved = os.environ.get(name)
        if resolved is None:
            raise LookupError(f"secret {value!r}: environment variable {name!r} is not set")
        return resolved
    if value.startswith(SECRET_SETTING):
        path = value[len(SECRET_SETTING):]
        if settings is None:
            import carb.settings
            settings = carb.settings.get_settings()
        resolved = settings.get(path)
        if resolved is None:
            raise LookupError(f"secret {value!r}: setting {path!r} is not set")
        return resolved
    logger.warning("a secret option holds a plain value instead of an env:NAME or setting:/path reference")
    return value


def resolve_secrets(spec: DriverSpec, options: dict, settings=None) -> dict:
    """The options with every secret reference replaced by its value. The input is not changed."""
    resolved = dict(options)
    for option in spec.options:
        if option.secret and option.key in resolved and resolved[option.key] is not None:
            resolved[option.key] = resolve_secret(resolved[option.key], settings)
    return resolved


# endregion
# region - Reading prims

def _get(prim: Usd.Prim, name: str, default=None):
    attr = prim.GetAttribute(name)
    if not attr.IsValid():
        return default
    value = attr.Get()
    return default if value is None else value


def _as_list(value) -> list:
    """A string[] attribute, or the 0.2.x comma-separated string, as a clean list."""
    if value is None:
        return []
    if isinstance(value, str):
        value = value.split(",")
    return [str(v).strip() for v in value if str(v).strip()]


def component_name(path: str, root: str = DEFAULT_ROOT) -> str:
    """'/PLC/PLC1' -> 'PLC1' under the root; any other path is its own name."""
    if path.startswith(root):
        return path[len(root):]
    return path


def classify(prim: Usd.Prim):
    """
    Decide whether a prim is a PLC prim and which driver it names.

    Returns:
        (driver_name, spec_or_None, legacy): spec is None when the driver is
        not registered; or None when the prim is not a PLC prim at all.
    """
    driver = _get(prim, ATTR_DRIVER)
    if driver is not None:
        driver = str(driver).strip()
        return driver, registry.get(driver), False
    # No marker: a 0.2.x prim carries the vendor attributes under its namespace.
    for spec in registry.specs():
        if spec.legacy_namespace is None:
            continue
        prefix = spec.legacy_namespace + ":"
        if any(prop.GetName().startswith(prefix) for prop in prim.GetAttributes()):
            return spec.name, spec, True
    return None


def read_config(prim: Usd.Prim, spec: DriverSpec, legacy: bool, root: str = DEFAULT_ROOT) -> PlcConfig:
    """Read one PLC prim into a PlcConfig. Attributes the prim lacks keep their defaults."""
    path = prim.GetPath().pathString
    config = PlcConfig(name=component_name(path, root), path=path, driver=spec.name, legacy=legacy)
    if legacy:
        ns = spec.legacy_namespace + ":"
        config.enabled = bool(_get(prim, ns + "Enable", False))
        config.refresh_ms = _get(prim, ns + "RefreshRate", NEUTRAL_DEFAULTS[ATTR_REFRESH])
        config.variables = _as_list(_get(prim, ns + "Variables"))
        for option in spec.options:
            config.options[option.key] = option.coerce(_get(prim, ns + option.key))
    else:
        config.enabled = bool(_get(prim, ATTR_ENABLE, False))
        config.refresh_ms = _get(prim, ATTR_REFRESH, NEUTRAL_DEFAULTS[ATTR_REFRESH])
        config.variables = _as_list(_get(prim, ATTR_VARIABLES))
        for option in spec.options:
            config.options[option.key] = option.coerce(_get(prim, spec.attribute(option.key)))
    # Read for both forms: a legacy prim may already carry the opt-out.
    config.mirror = bool(_get(prim, ATTR_MIRROR, True))
    config.mirror_symbols = _as_list(_get(prim, ATTR_MIRROR_SYMBOLS))
    for key, value in list(config.options.items()):
        if value is None:
            config.options[key] = spec.defaults[key]
    return config


def discover(stage: Usd.Stage, root: str = DEFAULT_ROOT, warned: Optional[set] = None):
    """
    Find every PLC prim in the stage.

    Args:
        stage: the stage to scan.
        root: the prefix stripped from a prim path to get the component name.
        warned: prim paths already warned about for the legacy form; updated in
            place so the warning is logged once per prim.

    Returns:
        (configs, unresolved): the PlcConfigs in stage order, and
        path -> driver name for prims naming a driver nobody has registered.
    """
    configs = []
    unresolved = {}
    if stage is None:
        return configs, unresolved
    for prim in stage.Traverse():
        found = classify(prim)
        if found is None:
            continue
        driver, spec, legacy = found
        path = prim.GetPath().pathString
        if spec is None:
            unresolved[path] = driver
            continue
        if legacy and warned is not None and path not in warned:
            warned.add(path)
            logger.warning(
                "%s uses the 0.2.x '%s:*' attributes, which are DEPRECATED (0.3 reads them, "
                "0.4 only behind a setting, 0.5 not at all). Set '%s = \"%s\"', move the "
                "vendor options to '%s:*' and use %s / %s / %s (a string[]) instead.",
                path, spec.legacy_namespace, ATTR_DRIVER, spec.name, spec.namespace,
                ATTR_ENABLE, ATTR_REFRESH, ATTR_VARIABLES)
        configs.append(read_config(prim, spec, legacy, root))
    return configs, unresolved


# endregion
# region - Writing prims

def _type_for(value) -> Sdf.ValueTypeName:
    if isinstance(value, bool):
        return Sdf.ValueTypeNames.Bool
    if isinstance(value, int):
        return Sdf.ValueTypeNames.Int
    if isinstance(value, float):
        return Sdf.ValueTypeNames.Double
    if isinstance(value, (list, tuple)):
        if value and isinstance(value[0], float):
            return Sdf.ValueTypeNames.DoubleArray
        if value and isinstance(value[0], int) and not isinstance(value[0], bool):
            return Sdf.ValueTypeNames.IntArray
        return Sdf.ValueTypeNames.StringArray
    return Sdf.ValueTypeNames.String


def _option_type(option: Option) -> Sdf.ValueTypeName:
    return {
        "str": Sdf.ValueTypeNames.String,
        "int": Sdf.ValueTypeNames.Int,
        "float": Sdf.ValueTypeNames.Double,
        "bool": Sdf.ValueTypeNames.Bool,
        "str_list": Sdf.ValueTypeNames.StringArray,
    }[option.kind]


def set_attr(prim: Usd.Prim, name: str, value, type_name: Optional[Sdf.ValueTypeName] = None):
    attr = prim.GetAttribute(name)
    if not attr.IsValid():
        attr = prim.CreateAttribute(name, type_name or _type_for(value), custom=True)
    if isinstance(value, tuple):
        value = list(value)
    attr.Set(value)


def author_config(prim: Usd.Prim, config: PlcConfig, spec: DriverSpec, secret_refs: Optional[dict] = None):
    """
    Write a PlcConfig onto a prim in the neutral form. A secret option is
    written as the reference it was read with (`secret_refs`, key -> ref),
    never as its value; with no reference known it is left off the prim.
    """
    set_attr(prim, ATTR_DRIVER, spec.name, Sdf.ValueTypeNames.String)
    set_attr(prim, ATTR_ENABLE, bool(config.enabled), Sdf.ValueTypeNames.Bool)
    set_attr(prim, ATTR_REFRESH, int(config.refresh_ms), Sdf.ValueTypeNames.Int)
    set_attr(prim, ATTR_VARIABLES, list(config.variables), Sdf.ValueTypeNames.StringArray)
    set_attr(prim, ATTR_MIRROR, bool(config.mirror), Sdf.ValueTypeNames.Bool)
    set_attr(prim, ATTR_MIRROR_SYMBOLS, list(config.mirror_symbols), Sdf.ValueTypeNames.StringArray)
    for option in spec.options:
        value = config.options.get(option.key, spec.defaults[option.key])
        if option.secret:
            # The reference the runtime was configured with, or the one still in
            # the config (a prim being created from options); never a value.
            value = (secret_refs or {}).get(option.key)
            if value is None and is_secret_reference(config.options.get(option.key)):
                value = config.options[option.key]
            if value is None:
                continue
        if value is None:
            continue
        set_attr(prim, spec.attribute(option.key), value, _option_type(option))


# endregion
