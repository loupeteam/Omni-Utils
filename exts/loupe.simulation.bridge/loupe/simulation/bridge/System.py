"""
The System: one object that owns a Runtime per PLC prim in the stage, plus
the components built on each (the bus adapter, the USD mirror).

Copyright (c) 2024 Loupe, https://loupe.team
Part of Omni-Utils, licensed under the MIT License.

The extension creates one System at startup and keeps it in step with the
stage on every open and close. Vendor extensions never touch it: they
register a driver, and the System builds a Runtime for every prim that names
it. Components are registered the same way (`register_component`): a factory
that gets the runtime and its config and returns an object with `cleanup()`,
or None when it does not apply to that PLC.
"""

import logging
from typing import Callable, Optional

import carb.settings
import omni.usd

from . import registry
from .delivery import MainThreadDelivery
from .registry import DriverSpec
from .Runtime import Runtime
from .schema import DEFAULT_ROOT, PlcConfig, author_config, classify, component_name, discover, read_config

logger = logging.getLogger(__name__)

SETTING_AUTO_CONNECT = "/exts/loupe.simulation.bridge/autoConnect"


class Component:
    """One PLC: its Runtime and the registered components built on it, by kind."""

    def __init__(self, runtime: Runtime, config: PlcConfig):
        self.runtime = runtime
        self.config = config
        self.parts = {}

    # 0.2.x name for the mirror.
    usd = property(lambda self: self.parts.get("mirror"))
    bus = property(lambda self: self.parts.get("bus"))

    def cleanup(self):
        for kind, part in list(self.parts.items()):
            try:
                part.cleanup()
            except Exception:
                logger.exception("%s: %s component cleanup failed", self.runtime.name, kind)
        self.parts.clear()
        self.runtime.cleanup()


class System:
    """
    Manages the PLC components of the open stage.

    Args:
        system_root: the prim path prefix a component name is relative to.
        auto_connect: None reads the setting /exts/loupe.simulation.bridge/autoConnect
            at every component creation; a bool fixes it (tests).
    """

    def __init__(self, system_root: str = DEFAULT_ROOT, auto_connect: Optional[bool] = None):
        self._system_root = system_root
        self._auto_connect = auto_connect
        self._components = {}
        self._factories = {}
        self._warned_legacy = set()
        self._unresolved = {}
        self.delivery = MainThreadDelivery()
        self._registry_remove = registry.add_listener(self._on_registry_event)

    system_root = property(lambda self: self._system_root)
    unresolved = property(lambda self: dict(self._unresolved),
                          doc="prim path -> driver name, for prims naming a driver nobody registered.")

    @property
    def auto_connect(self) -> bool:
        if self._auto_connect is not None:
            return self._auto_connect
        value = carb.settings.get_settings().get(SETTING_AUTO_CONNECT)
        return True if value is None else bool(value)

    def get_normalize_prim_name(self, name: str) -> str:
        if name.startswith("/"):
            return name
        return self._system_root + name

    def dispose(self):
        """Release everything, including the delivery and the registry listener. The extension's shutdown."""
        self.cleanup()
        if self._registry_remove is not None:
            self._registry_remove()
            self._registry_remove = None
        self.delivery.cleanup()

    def cleanup(self):
        """Remove every component. The stage is about to change or go away."""
        for name, component in list(self._components.items()):
            self.delivery.detach(name)
            component.cleanup()
        self._components.clear()
        self._unresolved = {}

    # region - Registered components

    def register_component(self, kind: str, factory: Callable):
        """
        Register `factory(system, runtime, config) -> object | None` to be
        called for every PLC. The object needs a `cleanup()`. Registering a
        kind again replaces it for PLCs created from now on.
        """
        self._factories[kind] = factory

    def unregister_component(self, kind: str):
        self._factories.pop(kind, None)

    def install_default_components(self):
        """The framework's own components: the bus adapter and the USD mirror."""
        self.register_component("bus", bus_component)
        self.register_component("mirror", mirror_component)

    def get_part(self, name: str, kind: str):
        component = self._components.get(name)
        return None if component is None else component.parts.get(kind)

    # endregion
    # region - Stage discovery

    def _stage(self):
        context = omni.usd.get_context()
        return None if context is None else context.get_stage()

    def find_components(self) -> Optional[dict]:
        """
        The PLC prims in the stage, name -> PlcConfig; None when no stage is
        open. Prims naming an unregistered driver are left out and listed in
        `unresolved`.
        """
        stage = self._stage()
        if stage is None:
            return None
        configs, self._unresolved = discover(stage, self._system_root, self._warned_legacy)
        for path, driver in self._unresolved.items():
            logger.warning("%s: no driver named %r is registered; is its extension enabled?", path, driver)
        return {config.name: config for config in configs}

    def find_and_create_components(self) -> Optional[list]:
        """Create a component for every PLC prim that has none, and drop those whose prim is gone."""
        found = self.find_components()
        if found is None:
            return None
        # The prim already exists and its options were just read from it, so it
        # is not authored back: that would dirty the stage merely by opening it.
        for name, config in found.items():
            if name not in self._components:
                try:
                    self.add_component(name, config, author_prim=False)
                except Exception:
                    logger.exception("%s: could not create the PLC component", config.path)
        for name in list(self._components):
            if name not in found:
                self.remove_component(name)
        return self.get_component_names()

    # endregion
    # region - Components

    def get_component(self, name: str) -> Optional[Runtime]:
        component = self._components.get(name)
        return None if component is None else component.runtime

    def get_component_names(self) -> list:
        return list(self._components)

    def get_config(self, name: str) -> Optional[PlcConfig]:
        component = self._components.get(name)
        return None if component is None else component.config

    def add_component(self, name: str, options=None, author_prim: bool = True, driver: Optional[str] = None) -> Runtime:
        """
        Create the Runtime (and the registered components) for one PLC.

        Args:
            name: the component name; the prim is `<system_root><name>` unless
                the name is already a path.
            options: a PlcConfig, or a dict of prim attribute names to values
                (`bridge:driver`, `bridge:Enable`, `beckhoff:AmsNetId`, ...).
            author_prim: define the prim and write the options onto it. False
                for a prim that already exists and was just read.
            driver: the driver name when `options` does not carry one; default
                the first registered driver.
        """
        if name in self._components:
            return self._components[name].runtime
        path = self.get_normalize_prim_name(name)
        if isinstance(options, PlcConfig):
            config = options
        else:
            config = self._config_from_options(name, path, options or {}, driver)
        spec = registry.get(config.driver)
        if spec is None:
            raise LookupError(f"{path}: no driver named {config.driver!r} is registered")
        if author_prim:
            self.create_component_prim(name, config, spec)
        runtime = Runtime(name, config, spec, auto_connect=self.auto_connect)
        component = Component(runtime, config)
        self._components[name] = component
        self.delivery.attach(name, runtime.plc)
        for kind, factory in list(self._factories.items()):
            try:
                part = factory(self, runtime, config)
            except Exception:
                logger.exception("%s: %s component failed to build", name, kind)
                continue
            if part is not None:
                component.parts[kind] = part
        return runtime

    def remove_component(self, name: str):
        component = self._components.pop(name, None)
        if component is None:
            return
        self.delivery.detach(name)
        component.cleanup()

    def _config_from_options(self, name: str, path: str, options: dict, driver: Optional[str]) -> PlcConfig:
        from .schema import ATTR_DRIVER
        driver = options.get(ATTR_DRIVER) or driver
        if driver is None:
            names = registry.names()
            if not names:
                raise LookupError("no driver is registered")
            driver = names[0]
        spec = registry.get(driver)
        if spec is None:
            raise LookupError(f"no driver named {driver!r} is registered")
        config = PlcConfig(name=name, path=path, driver=driver, options=dict(spec.defaults))
        # Route the rest through the adapter's option normaliser by building a
        # throwaway view: simpler to apply the dict after construction.
        self._apply_option_dict(config, spec, options)
        return config

    @staticmethod
    def _apply_option_dict(config: PlcConfig, spec: DriverSpec, options: dict):
        from .schema import ATTR_ENABLE, ATTR_MIRROR, ATTR_MIRROR_SYMBOLS, ATTR_REFRESH, ATTR_VARIABLES, _as_list
        legacy = (spec.legacy_namespace + ":") if spec.legacy_namespace else None
        for key, value in options.items():
            if value is None:
                continue
            if key == ATTR_ENABLE or (legacy and key == legacy + "Enable"):
                config.enabled = bool(value)
            elif key == ATTR_REFRESH or (legacy and key == legacy + "RefreshRate"):
                config.refresh_ms = value
            elif key == ATTR_VARIABLES or (legacy and key == legacy + "Variables"):
                config.variables = _as_list(value)
            elif key == ATTR_MIRROR:
                config.mirror = bool(value)
            elif key == ATTR_MIRROR_SYMBOLS:
                config.mirror_symbols = _as_list(value)
            else:
                bare = key
                for prefix in (spec.namespace + ":", legacy):
                    if prefix and key.startswith(prefix):
                        bare = key[len(prefix):]
                option = spec.option(bare)
                if option is not None:
                    config.options[bare] = option.coerce(value)

    def create_component_prim(self, name: str, config: PlcConfig, spec: Optional[DriverSpec] = None) -> str:
        """Define the PLC prim and write the config onto it in the neutral form. Returns the prim path."""
        spec = spec or registry.get(config.driver)
        path = self.get_normalize_prim_name(name)
        prim = self._stage().DefinePrim(path)
        author_config(prim, config, spec)
        return path

    def write_options_to_stage(self, name: str):
        """Write a component's current configuration onto its prim (neutral attributes; secrets as references)."""
        runtime = self.get_component(name)
        if runtime is None:
            return
        stage = self._stage()
        prim = stage.GetPrimAtPath(runtime.path) if stage is not None else None
        if prim is None or not prim.IsValid():
            return
        author_config(prim, runtime.to_config(), runtime.spec, runtime.secret_refs)

    def read_options_from_stage(self, name: str):
        """Re-read a component's prim and apply the options to the runtime."""
        runtime = self.get_component(name)
        if runtime is None:
            return
        stage = self._stage()
        prim = stage.GetPrimAtPath(runtime.path) if stage is not None else None
        if prim is None or not prim.IsValid():
            return
        found = classify(prim)
        if found is None or found[1] is None:
            return
        config = read_config(prim, found[1], found[2], self._system_root)
        runtime.options = config.to_options(found[1])
        self._components[name].config = config

    # endregion
    # region - Registry events

    def _on_registry_event(self, event, spec: DriverSpec):
        if event == registry.EVENT_REGISTERED:
            # Prims that named this driver before it existed can be built now;
            # components built on a previous registration of the name are
            # rebuilt on the new class.
            for name, component in list(self._components.items()):
                if component.runtime.driver_name == spec.name and component.runtime.spec is not spec:
                    self.remove_component(name)
            if self._stage() is not None:
                self.find_and_create_components()
        elif event == registry.EVENT_UNREGISTERED:
            for name, component in list(self._components.items()):
                if component.runtime.driver_name == spec.name:
                    self.remove_component(name)

    # endregion


def bus_component(system: System, runtime: Runtime, config: PlcConfig):
    """The message bus adapter: neutral names, plus the driver's legacy names while the setting allows."""
    from .bus import BUS_NAMESPACE, BusAdapter, legacy_bus_names_enabled
    namespaces = [BUS_NAMESPACE]
    if runtime.spec.legacy_namespace and legacy_bus_names_enabled():
        namespaces.append(runtime.spec.legacy_namespace)
    return BusAdapter(runtime, namespaces)


def mirror_component(system: System, runtime: Runtime, config: PlcConfig):
    """The USD mirror, for PLCs whose prim has bridge:MirrorToUsd true (the default)."""
    if not runtime.mirror:
        return None
    from .UsdManager import RuntimeUsd
    return RuntimeUsd(runtime.path, runtime, system.delivery, runtime.mirror_symbols)
