"""
The Kit side of one PLC: a plc_bridge.PlcRuntime driving the registered
driver, plus the glue that maps the prim's options onto it.

Copyright (c) 2024 Loupe, https://loupe.team
Part of Omni-Utils, licensed under the MIT License.

This is the one adapter for every vendor; it replaces the Runtime classes the
0.2.x vendor extensions each carried. It knows nothing vendor-specific: the
DriverSpec from the registry says how to build and re-configure the driver.
The message bus and the USD mirror are separate components built on top of it
(see System.py), not part of it.
"""

import logging
import threading
import time
from typing import Optional

from plc_bridge import PlcRuntime

from .registry import DriverSpec
from .schema import (
    ATTR_ENABLE,
    ATTR_MIRROR,
    ATTR_MIRROR_SYMBOLS,
    ATTR_REFRESH,
    ATTR_VARIABLES,
    PlcConfig,
    is_secret_reference,
    resolve_secrets,
)

logger = logging.getLogger(__name__)

# Why a runtime that the prim asked to enable is sitting disabled.
HELD_BY_AUTO_CONNECT = (
    "the prim has bridge:Enable = true, but the app setting "
    "/exts/loupe.simulation.bridge/autoConnect is false; enable it here or in code"
)


# Closes running on threads of their own (Runtime.cleanup(wait=False)), so
# the extension's shutdown can give the healthy ones a moment to finish.
_closers = set()
_closers_lock = threading.Lock()


def wait_for_closers(timeout: float) -> bool:
    """
    Wait up to `timeout` seconds in total for background closes to finish.

    Returns:
        True when none is left running.
    """
    deadline = time.monotonic() + timeout
    with _closers_lock:
        pending = list(_closers)
    for thread in pending:
        thread.join(max(0.0, deadline - time.monotonic()))
    with _closers_lock:
        return not any(t.is_alive() for t in _closers)


def _option(options: dict, key: str, default):
    """
    Read an option, falling back to the default when the key is missing or
    None (a prim attribute with no value arrives as None). Not
    `options.get(key) or default`: that turns a legitimate False or 0 into
    the default, so the option could never be switched off.
    """
    value = options.get(key)
    return default if value is None else value


class Runtime:
    """
    One PLC. Construct it from a PlcConfig (read from the prim) and the
    driver's spec; it builds the driver and the PlcRuntime, idle. The System
    calls `start()` once the listeners (delivery, bus adapter, mirror) are
    attached, so the first CONNECTING, the first sample and the first problem
    reach them; nothing is polled before that.

    What a 0.2.x script that reached a vendor Runtime through
    `get_system().get_component(name)` still finds: `enable_communication`,
    `refresh_rate` / `refresh_period_ms`, `read_variables`,
    `set_read_variables`, `queue_write`, `is_connected`, `plc`, `driver`,
    `name`. Narrower than 0.2.x: `options` returns the neutral keys with
    `bridge:Variables` as a list, and the vendor properties (`ams_net_id`,
    `host`, `port`) are gone; use `driver_options` / `set_driver_option` or
    the driver object itself.
    """

    def __init__(self, name: str, config: PlcConfig, spec: DriverSpec, *,
                 auto_connect: bool = True, settings=None):
        self._name = name
        self._path = config.path
        self._spec = spec
        self._legacy = config.legacy
        # Secret options stay as references here (what the prim holds and what
        # goes back to it); only the driver sees the values.
        self._options = spec.resolve(config.options)
        self._secret_refs = {
            key: value for key, value in self._options.items()
            if (spec.option(key) is not None and spec.option(key).secret and is_secret_reference(value))
        }
        self._driver = spec.create_driver(resolve_secrets(spec, self._options, settings))

        self._held = None
        enabled = bool(config.enabled)
        if enabled and not auto_connect:
            enabled = False
            self._held = HELD_BY_AUTO_CONNECT
            logger.warning("%s: %s", self._path, HELD_BY_AUTO_CONNECT)

        self._plc = PlcRuntime(self._driver, name=name, refresh_ms=config.refresh_ms, enabled=enabled)
        self._plc.set_read_variables(config.variables)
        self._mirror = bool(config.mirror)
        self._mirror_symbols = list(config.mirror_symbols)
        # Set by the System: called when the mirror settings change so the
        # mirror component can be rebuilt.
        self.on_mirror_changed = None

    def start(self):
        """Start polling. The System calls this after every listener is attached. Idempotent."""
        self._plc.start()

    def __del__(self):
        # Synchronous: a thread started from a finaliser can outlive the
        # interpreter. A runtime the System cleaned up is already done.
        self.cleanup()

    def cleanup(self, wait: bool = True) -> Optional[threading.Thread]:
        """
        Stop polling, then close the driver (`PlcDriver.close()`: the
        framework created it, so the framework closes it). Safe to call more
        than once; only the first call does anything.

        Args:
            wait: True does it on the calling thread. That can take
                plc_bridge's JOIN_TIMEOUT_SEC (2 s) plus the driver's
                disconnect bound when the PLC stopped answering: 5 s for a B&R
                PLC in the worst case. False does it on a daemon thread and
                returns at once; the System uses that on stage changes, so a
                silent PLC does not freeze the app.

        Returns:
            The closing thread when wait is False and there was something to
            close, else None.
        """
        if getattr(self, "_closing", False):
            return None
        self._closing = True
        if wait:
            self._close()
            return None
        thread = threading.Thread(target=self._close_in_background,
                                  name=f"{self._name}-close", daemon=True)
        with _closers_lock:
            _closers.add(thread)
        thread.start()
        return thread

    def _close_in_background(self):
        try:
            self._close()
        finally:
            with _closers_lock:
                _closers.discard(threading.current_thread())

    def _close(self):
        plc = getattr(self, "_plc", None)  # __init__ may have failed before it existed
        if plc is not None:
            try:
                plc.stop()
            except Exception:
                logger.exception("%s: stop failed", self._name)
        driver = getattr(self, "_driver", None)
        # stop() only disconnects; a driver on an async transport also owns an
        # event-loop thread that close() ends, so a runtime torn down on every
        # stage open/close does not leave one behind. Checked with getattr: a
        # registered class need not derive from PlcDriver.
        close = getattr(driver, "close", None)
        if close is not None:
            try:
                close()
            except Exception:
                logger.exception("%s: driver close failed", self._name)

    # region - Properties
    name = property(lambda self: self._name)
    path = property(lambda self: self._path, doc="The PLC prim's path: the component's identity.")
    spec = property(lambda self: self._spec, doc="The registry entry of the driver in use.")
    driver_name = property(lambda self: self._spec.name)
    legacy = property(lambda self: self._legacy, doc="True when the prim used the 0.2.x attributes.")
    plc = property(lambda self: self._plc, doc="The plc_bridge.PlcRuntime doing the polling.")
    driver = property(lambda self: self._driver, doc="The vendor's PlcDriver.")
    is_connected = property(lambda self: self._plc.is_connected)
    read_variables = property(lambda self: self._plc.read_variables)
    held = property(lambda self: self._held,
                    doc="Why the runtime is disabled although the prim asked for it, or None.")

    @property
    def enable_communication(self) -> bool:
        return self._plc.enabled

    @enable_communication.setter
    def enable_communication(self, value: bool):
        value = bool(value)
        if value:
            self._held = None
        self._plc.enabled = value

    @property
    def refresh_period_ms(self):
        return self._plc.refresh_ms

    @refresh_period_ms.setter
    def refresh_period_ms(self, value):
        self._plc.refresh_ms = value

    # Two names for one value; the prim attribute is called RefreshRate.
    refresh_rate = refresh_period_ms

    write_sleep_time = property(
        lambda self: self._plc.write_sleep,
        lambda self, value: setattr(self._plc, "write_sleep", value),
    )

    @property
    def mirror(self) -> bool:
        return self._mirror

    @property
    def mirror_symbols(self) -> list:
        return list(self._mirror_symbols)

    @property
    def driver_options(self) -> dict:
        """The driver's options by bare key, secrets as their references."""
        return dict(self._options)

    def set_driver_option(self, key: str, value) -> bool:
        """
        Change one driver option. A changed value is assigned to the live
        driver (`DriverSpec.apply_option`) and the connection is reopened.

        Returns:
            True when something changed.
        """
        option = self._spec.option(key)
        if option is None:
            raise KeyError(f"{self._spec.name!r} has no option {key!r}")
        value = option.coerce(value)
        if value is None or value == self._options.get(key):
            return False
        driver_value = value
        if option.secret:
            # Resolve before anything is stored: a reference that does not
            # resolve raises here and leaves the runtime (and what "Write To
            # USD" would author) as it was.
            driver_value = resolve_secrets(self._spec, {key: value})[key]
        self._options[key] = value
        if option.secret:
            if is_secret_reference(value):
                self._secret_refs[key] = value
            else:
                # A plain value replaces the reference: "Write To USD" must not
                # author the old reference back, nor the value (left off the prim).
                self._secret_refs.pop(key, None)
        if self._spec.apply_option(self._driver, key, driver_value):
            self._plc.reconnect()
        return True

    @property
    def options(self) -> dict:
        """
        The configuration as a flat dict keyed by prim attribute name:
        `bridge:Enable`, `bridge:RefreshRate`, `bridge:Variables` (a list),
        `bridge:MirrorToUsd`, `bridge:MirrorSymbols` and `<namespace>:<key>`
        for the driver's options. Secrets appear as their references.
        """
        options = {
            ATTR_ENABLE: self.enable_communication,
            ATTR_REFRESH: self.refresh_rate,
            ATTR_VARIABLES: self._plc.read_variables,
            ATTR_MIRROR: self._mirror,
            ATTR_MIRROR_SYMBOLS: list(self._mirror_symbols),
        }
        for key, value in self._options.items():
            options[self._spec.attribute(key)] = value
        return options

    @options.setter
    def options(self, value: dict):
        """
        Apply a partial update. Keys may be neutral (`bridge:Enable`,
        `beckhoff:AmsNetId`), 0.2.x (`beckhoff_bridge:Enable`,
        `beckhoff_bridge:Variables` as a comma-separated string) or bare option
        keys (`AmsNetId`). Missing keys leave their setting alone. `Enable`
        is always re-assigned, as in 0.2.x: the assignment pushes the ENABLE
        event that tells listeners the options were (re)applied.
        """
        value = self._normalise(value or {})
        for key, item in value.items():
            if key.startswith("driver:"):
                self.set_driver_option(key[len("driver:"):], item)
        self.enable_communication = _option(value, ATTR_ENABLE, self.enable_communication)
        self.refresh_rate = _option(value, ATTR_REFRESH, self.refresh_rate)
        if ATTR_VARIABLES in value:
            # Replaces the cyclic read list, so a variable removed from the
            # prim stops being read.
            self.set_read_variables(value[ATTR_VARIABLES] or [])
        mirror_changed = False
        if ATTR_MIRROR in value and value[ATTR_MIRROR] is not None:
            mirror_changed |= self._mirror != bool(value[ATTR_MIRROR])
            self._mirror = bool(value[ATTR_MIRROR])
        if ATTR_MIRROR_SYMBOLS in value:
            symbols = _as_list(value[ATTR_MIRROR_SYMBOLS])
            mirror_changed |= symbols != self._mirror_symbols
            self._mirror_symbols = symbols
        if mirror_changed and self.on_mirror_changed is not None:
            self.on_mirror_changed()

    def _normalise(self, value: dict) -> dict:
        """Bring any accepted key spelling to the neutral one; driver options become `driver:<key>`."""
        out = {}
        legacy = (self._spec.legacy_namespace + ":") if self._spec.legacy_namespace else None
        neutral = self._spec.namespace + ":"
        for key, item in value.items():
            if key in (ATTR_ENABLE, ATTR_REFRESH, ATTR_MIRROR, ATTR_MIRROR_SYMBOLS):
                out[key] = item
            elif key == ATTR_VARIABLES:
                out[key] = _as_list(item)
            elif legacy and key.startswith(legacy):
                bare = key[len(legacy):]
                if bare == "Variables":
                    out[ATTR_VARIABLES] = _as_list(item)
                elif bare == "Enable":
                    out[ATTR_ENABLE] = item
                elif bare == "RefreshRate":
                    out[ATTR_REFRESH] = item
                elif self._spec.option(bare) is not None:
                    out["driver:" + bare] = item
                else:
                    logger.warning("%s: unknown option %r ignored", self._name, key)
            elif key.startswith(neutral) and self._spec.option(key[len(neutral):]) is not None:
                out["driver:" + key[len(neutral):]] = item
            elif self._spec.option(key) is not None:
                out["driver:" + key] = item
            else:
                logger.warning("%s: unknown option %r ignored", self._name, key)
        return out

    def to_config(self) -> PlcConfig:
        """The current configuration as a PlcConfig, for writing back to the prim."""
        return PlcConfig(
            name=self._name, path=self._path, driver=self._spec.name,
            enabled=self.enable_communication, refresh_ms=self.refresh_rate,
            variables=self._plc.read_variables, mirror=self._mirror,
            mirror_symbols=list(self._mirror_symbols), options=dict(self._options), legacy=self._legacy,
        )

    secret_refs = property(lambda self: dict(self._secret_refs),
                           doc="Secret option key -> the reference it was configured with.")

    # endregion
    # region - External API

    def set_read_variables(self, variables):
        """
        Replace the cyclic read list. Blank entries are dropped and whitespace is
        stripped (see PlcRuntime.set_read_variables).
        """
        self._plc.set_read_variables(_as_list(variables))

    def add_read_variables(self, variables):
        self._plc.add_read_variables(_as_list(variables))

    def queue_write(self, name, value):
        """Queue one write; returns the plc_bridge.WriteHandle."""
        return self._plc.queue_write(name, value)

    # endregion


def _as_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, str):
        value = value.split(",")
    return [str(v).strip() for v in value if str(v).strip()]
