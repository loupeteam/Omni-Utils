"""
The driver registry: where a vendor extension tells the framework how to build
its PlcDriver from the options on a PLC prim.

Copyright (c) 2024 Loupe, https://loupe.team
Part of Omni-Utils, licensed under the MIT License.

A vendor extension registers once, in its on_startup:

    from loupe.simulation.bridge import registry, Option
    from beckhoff_bridge import AdsDriver

    registry.register(
        "beckhoff", AdsDriver,
        [Option("AmsNetId", "str", "127.0.0.1.1.1", "PLC AMS Net Id")],
        legacy_namespace="beckhoff_bridge",
    )

and unregisters in on_shutdown. The framework then recognises PLC prims with
`bridge:driver = "beckhoff"`, reads the option `beckhoff:AmsNetId` from them,
builds `AdsDriver(ams_net_id=...)`, and (because of `legacy_namespace`) also
accepts 0.2.x prims that carry `beckhoff_bridge:AmsNetId` and pushes the
0.2.x bus names.

This module imports nothing from Kit so the schema can be tested anywhere.
"""

import logging
import re
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional

logger = logging.getLogger(__name__)

#: The option kinds a driver may declare, and the USD attribute each maps to.
KINDS = ("str", "int", "float", "bool", "str_list")

_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


def snake_case(name: str) -> str:
    """'AmsNetId' -> 'ams_net_id', 'Host' -> 'host', 'URL' -> 'url'."""
    return _CAMEL.sub("_", name).lower()


@dataclass(frozen=True)
class Option:
    """
    One entry of a driver's option schema.

    Attributes:
        key: the attribute name under the driver's prim namespace
            ("AmsNetId" -> `beckhoff:AmsNetId`) and the key in the options dict.
        kind: one of KINDS.
        default: the value when the prim does not carry the attribute.
        label: what the UI shows next to the field.
        secret: never stored on a prim. The prim (or the options dict) holds a
            reference, `env:NAME` or `setting:/path`, which the framework
            resolves right before the driver is built; the resolved value never
            goes back to the stage.
        arg: the driver constructor's keyword for this option (and the driver
            attribute the framework assigns when the option changes at
            runtime). Defaults to the key in snake_case.
    """

    key: str
    kind: str
    default: Any
    label: str = ""
    secret: bool = False
    arg: Optional[str] = None

    def __post_init__(self):
        if self.kind not in KINDS:
            raise ValueError(f"option {self.key!r}: kind must be one of {KINDS}, got {self.kind!r}")
        if not self.key or ":" in self.key:
            raise ValueError(f"option key must be a bare name, got {self.key!r}")

    @property
    def arg_name(self) -> str:
        return self.arg or snake_case(self.key)

    def coerce(self, value: Any) -> Any:
        """
        Bring a value read from a prim (or typed into the UI) to the option's
        kind. None stays None, so a missing attribute keeps the default.
        """
        if value is None:
            return None
        if self.kind == "str":
            return str(value)
        if self.kind == "int":
            return int(value)
        if self.kind == "float":
            return float(value)
        if self.kind == "bool":
            if isinstance(value, str):
                return value.strip().lower() in ("1", "true", "yes", "on")
            return bool(value)
        if self.kind == "str_list":
            if isinstance(value, str):
                # a comma-separated string is the 0.2.x spelling of a list
                value = value.split(",")
            return [str(v).strip() for v in value if str(v).strip()]
        return value


@dataclass(frozen=True)
class DriverSpec:
    """
    What the registry holds for one driver name.

    Attributes:
        name: the value of `bridge:driver` that selects this driver.
        driver_class: a plc_bridge.PlcDriver subclass.
        options: the option schema, in UI order.
        defaults: key -> default, the schema defaults with the registration's
            overrides applied.
        ui_panel: optional `callback(runtime, spec)` that builds extra omni.ui
            widgets under the neutral panel. None: the framework builds a field
            per option from the schema.
        namespace: the prim attribute namespace for the options
            (`<namespace>:<key>`). Defaults to the driver name.
        legacy_namespace: the 0.2.x attribute namespace (`beckhoff_bridge`)
            and bus name. A prim carrying `<legacy_namespace>:*` attributes
            and no `bridge:driver` is treated as this driver with a
            deprecation warning; the bus adapter also pushes
            `loupe.simulation.<legacy_namespace>.*` while the legacyBusNames
            setting is on. None: no legacy surface.
        title: what the UI calls the driver.
    """

    name: str
    driver_class: type
    options: tuple = ()
    defaults: dict = field(default_factory=dict)
    ui_panel: Optional[Callable] = None
    namespace: str = ""
    legacy_namespace: Optional[str] = None
    title: str = ""

    def option(self, key: str) -> Optional[Option]:
        for option in self.options:
            if option.key == key:
                return option
        return None

    def attribute(self, key: str) -> str:
        """The prim attribute name of an option: `<namespace>:<key>`."""
        return f"{self.namespace}:{key}"

    def legacy_attribute(self, key: str) -> Optional[str]:
        """The 0.2.x prim attribute name of an option, or None without a legacy namespace."""
        if self.legacy_namespace is None:
            return None
        return f"{self.legacy_namespace}:{key}"

    def resolve(self, options: Optional[dict]) -> dict:
        """key -> value for every option: the given value, coerced, else the default."""
        options = options or {}
        resolved = {}
        for option in self.options:
            value = option.coerce(options.get(option.key))
            resolved[option.key] = self.defaults[option.key] if value is None else value
        return resolved

    def create_driver(self, options: Optional[dict]):
        """Build the driver from a full option dict (see `resolve`)."""
        kwargs = {option.arg_name: value for option, value in zip(self.options, self.resolve(options).values())}
        return self.driver_class(**kwargs)

    def apply_option(self, driver, key: str, value: Any) -> bool:
        """
        Assign a changed option to a live driver, so a prim edit (or the UI)
        takes effect without rebuilding the runtime. The caller reconnects.

        Returns:
            True when the driver had the attribute and its value changed.
        """
        option = self.option(key)
        if option is None:
            return False
        value = option.coerce(value)
        if value is None or not hasattr(driver, option.arg_name):
            return False
        if getattr(driver, option.arg_name) == value:
            return False
        setattr(driver, option.arg_name, value)
        return True


_lock = threading.RLock()
_drivers: dict = {}
_listeners: list = []

EVENT_REGISTERED = "registered"
EVENT_UNREGISTERED = "unregistered"


def register(name: str, driver_class: type, option_schema: Iterable[Option] = (),
             defaults: Optional[dict] = None, ui_panel: Optional[Callable] = None, *,
             namespace: Optional[str] = None, legacy_namespace: Optional[str] = None,
             title: Optional[str] = None) -> DriverSpec:
    """
    Register a driver under `name`. Registering a name again replaces the
    previous entry (an extension reload does that) and tells the listeners,
    so the System rebuilds the components that use it.

    Args:
        name: the `bridge:driver` value; also the default prim namespace.
        driver_class: a plc_bridge.PlcDriver subclass whose constructor takes
            one keyword per option (`Option.arg_name`).
        option_schema: the Options, in UI order.
        defaults: overrides for the schema defaults, key -> value.
        ui_panel: see DriverSpec.ui_panel.
        namespace: see DriverSpec.namespace.
        legacy_namespace: see DriverSpec.legacy_namespace.
        title: see DriverSpec.title.
    """
    if not name or ":" in name or "/" in name:
        raise ValueError(f"driver name must be a bare identifier, got {name!r}")
    options = tuple(option_schema)
    keys = [option.key for option in options]
    if len(set(keys)) != len(keys):
        raise ValueError(f"driver {name!r}: duplicate option keys in {keys}")
    merged = {option.key: option.default for option in options}
    for key, value in (defaults or {}).items():
        if key not in merged:
            raise ValueError(f"driver {name!r}: default for unknown option {key!r}")
        merged[key] = value
    spec = DriverSpec(
        name=name, driver_class=driver_class, options=options, defaults=merged, ui_panel=ui_panel,
        namespace=namespace or name, legacy_namespace=legacy_namespace, title=title or name,
    )
    with _lock:
        replaced = _drivers.get(name)
        _drivers[name] = spec
    if replaced is not None:
        logger.info("driver %r re-registered (%s -> %s)", name,
                    replaced.driver_class.__name__, driver_class.__name__)
    _notify(EVENT_REGISTERED, spec)
    return spec


def unregister(name: str) -> Optional[DriverSpec]:
    """Remove a driver. The System stops the components that use it. Returns the spec, or None."""
    with _lock:
        spec = _drivers.pop(name, None)
    if spec is not None:
        _notify(EVENT_UNREGISTERED, spec)
    return spec


def get(name: str) -> Optional[DriverSpec]:
    with _lock:
        return _drivers.get(name)


def names() -> list:
    """The registered driver names, in registration order."""
    with _lock:
        return list(_drivers)


def specs() -> list:
    with _lock:
        return list(_drivers.values())


def by_legacy_namespace(namespace: str) -> Optional[DriverSpec]:
    """The driver that claims a 0.2.x attribute namespace (`beckhoff_bridge`), if any."""
    with _lock:
        for spec in _drivers.values():
            if spec.legacy_namespace == namespace:
                return spec
    return None


def add_listener(callback: Callable[[str, DriverSpec], None]) -> Callable[[], None]:
    """callback(event, spec) on EVENT_REGISTERED / EVENT_UNREGISTERED. Returns a remover."""
    with _lock:
        _listeners.append(callback)

    def remove():
        with _lock:
            if callback in _listeners:
                _listeners.remove(callback)

    return remove


def _notify(event: str, spec: DriverSpec):
    with _lock:
        listeners = list(_listeners)
    for callback in listeners:
        try:
            callback(event, spec)
        except Exception:
            logger.exception("a registry listener raised on %s %r", event, spec.name)


def clear():
    """Forget every driver. For tests."""
    for name in names():
        unregister(name)
