"""
The carb message bus as one listener among others.

Copyright (c) 2024 Loupe, https://loupe.team
Part of Omni-Utils, licensed under the MIT License.

`BusAdapter` subscribes to a runtime's events and pushes them on the bus under
the neutral names `loupe.simulation.bridge.<KIND>.<plc>`; while the setting
`/exts/loupe.simulation.bridge/legacyBusNames` is on (the 0.3 default) it
also pushes the driver's 0.2.x names (`loupe.simulation.beckhoff_bridge.*`,
`loupe.simulation.br_bridge.*`) and accepts requests on both.

`Manager` is the 0.2.x script API on top of the bus, on the neutral names.
The vendor modules re-export it in Phase 4.

Bus payloads (`event.payload`):

| KIND            | payload                                                    |
|-----------------|------------------------------------------------------------|
| DATA_INIT       | `{"meta": {"name"}}` once per runtime, when it is created   |
| DATA_READ       | `{"meta", "data": <nested values>}` per sample, worker thread |
| CONNECTION      | `{"meta", "status": "Connecting" / "Connected" / "Disconnected"}` |
| STATUS          | neutral: `{"meta", "status": {"kind", "text", "symbols"}}`; legacy: `status` is the text |
| ENABLE          | `{"meta", "status": {"enabled": bool}}`                    |
| WRITE           | neutral only: `{"meta", "values", "errors", "error"}` per flushed batch |
| DATA_READ_REQ   | request in: `{"variables": [names]}`                        |
| DATA_WRITE_REQ  | request in: `{"variables": [{"name", "value"}]}`             |

The pushes happen on the runtime's worker thread, as they did in 0.2.x; a
subscriber that touches the stage or the UI marshals to the main thread
itself, or uses `on_sample_main` instead.
"""

import logging
from typing import Any, Callable, Optional

import carb.events
import carb.settings
import omni.kit.app

from .BridgeManager import BridgeManager, Manager_Events

logger = logging.getLogger(__name__)

#: The neutral bus namespace: events are `loupe.simulation.bridge.<KIND>.<plc>`.
BUS_NAMESPACE = "bridge"
SETTING_LEGACY_BUS_NAMES = "/exts/loupe.simulation.bridge/legacyBusNames"

Events = Manager_Events(BUS_NAMESPACE)
EVENT_TYPE_DATA_INIT = Events.EVENT_TYPE_DATA_INIT
EVENT_TYPE_DATA_READ = Events.EVENT_TYPE_DATA_READ
EVENT_TYPE_DATA_READ_REQ = Events.EVENT_TYPE_DATA_READ_REQ
EVENT_TYPE_DATA_WRITE_REQ = Events.EVENT_TYPE_DATA_WRITE_REQ
EVENT_TYPE_CONNECTION = Events.EVENT_TYPE_CONNECTION
EVENT_TYPE_STATUS = Events.EVENT_TYPE_STATUS
EVENT_TYPE_ENABLE = Events.EVENT_TYPE_ENABLE
EVENT_TYPE_WRITE = f"loupe.simulation.{BUS_NAMESPACE}.WRITE"


def get_stream_name(msg_type: str, name: str) -> int:
    """The carb event type of `<msg_type>.<plc name>`."""
    return carb.events.type_from_string(f"{msg_type}.{name}")


def legacy_bus_names_enabled() -> bool:
    value = carb.settings.get_settings().get(SETTING_LEGACY_BUS_NAMES)
    return True if value is None else bool(value)


class BusAdapter:
    """
    Pushes one runtime's events on the bus and feeds bus requests back into it.

    Args:
        runtime: the framework Runtime (its `plc` is listened to).
        namespaces: the bus namespaces to serve. The first is the neutral
            one; any other is a legacy name that gets the 0.2.x payload
            shapes (STATUS as text).
    """

    def __init__(self, runtime, namespaces):
        self._runtime = runtime
        self._name = runtime.name
        self._namespaces = list(namespaces)
        self._events = [Manager_Events(ns) for ns in self._namespaces]
        self._stream = omni.kit.app.get_app().get_message_bus_event_stream()
        plc = runtime.plc
        self._removers = [
            plc.on_data(self._on_data),
            plc.on_problem(self._on_problem),
            plc.on_connection(self._on_connection),
            plc.on_enabled(self._on_enabled),
            plc.on_write(self._on_write),
        ]
        self._subscriptions = []
        for events in self._events:
            self._subscriptions.append(self._stream.create_subscription_to_push_by_type(
                get_stream_name(events.EVENT_TYPE_DATA_READ_REQ, self._name), self._on_read_req))
            self._subscriptions.append(self._stream.create_subscription_to_push_by_type(
                get_stream_name(events.EVENT_TYPE_DATA_WRITE_REQ, self._name), self._on_write_req))
        for events in self._events:
            self._push(events.EVENT_TYPE_DATA_INIT, {})

    namespaces = property(lambda self: list(self._namespaces))

    def cleanup(self):
        for remove in self._removers:
            remove()
        self._removers = []
        for subscription in self._subscriptions:
            subscription.unsubscribe()
        self._subscriptions = []

    # region - Runtime -> bus

    def _push(self, event_type: str, extra: dict):
        message = {"meta": {"name": self._name}}
        message.update(extra)
        try:
            self._stream.push(event_type=get_stream_name(event_type, self._name), payload=message)
        except Exception as e:
            logger.error("%s: error pushing %s: %s", self._name, event_type, e)

    def _on_data(self, data):
        for events in self._events:
            self._push(events.EVENT_TYPE_DATA_READ, {"data": data})

    def _on_problem(self, problem):
        structured = {"kind": problem.kind, "text": problem.text, "symbols": list(problem.symbols)}
        for index, events in enumerate(self._events):
            # The neutral name carries the structured Problem; a legacy name the
            # 0.2.x text, which is what a 0.2.x status display expects.
            self._push(events.EVENT_TYPE_STATUS, {"status": structured if index == 0 else problem.text})

    def _on_connection(self, state):
        for events in self._events:
            self._push(events.EVENT_TYPE_CONNECTION, {"status": state})

    def _on_enabled(self, enabled):
        for events in self._events:
            self._push(events.EVENT_TYPE_ENABLE, {"status": {"enabled": enabled}})

    def _on_write(self, result):
        self._push(EVENT_TYPE_WRITE, {"values": dict(result.values), "errors": dict(result.errors),
                                      "error": result.error})

    # endregion
    # region - Bus -> runtime

    def _on_read_req(self, event):
        self._runtime.add_read_variables(event.payload["variables"])

    def _on_write_req(self, event):
        for variable in event.payload["variables"]:
            self._runtime.queue_write(variable["name"], variable["value"])

    # endregion


class Manager(BridgeManager):
    """
    The 0.2.x bus API: subscribe to a PLC's data and send it requests, with
    no reference to the runtime object. New code should prefer `get_plc()`
    and `on_sample_main()`; this stays for scripts written against 0.2.x.

    Args:
        name: the PLC name (`/PLC/<name>`).
        namespace: the bus namespace to talk on. Default the neutral one;
            a vendor module passes its legacy namespace.
    """

    def __init__(self, name: str, namespace: str = BUS_NAMESPACE):
        # First, so cleanup() from __del__ works even when the checks below raise
        self._callbacks = []
        if not name:
            raise ValueError("Manager() needs the PLC name; the no-name form was removed in 0.3.0")
        self._plc_name = name
        self._events = Manager_Events(namespace)
        self._event_stream = omni.kit.app.get_app().get_message_bus_event_stream()
        system = get_system()
        if system is not None and system.get_component(name) is None:
            logger.warning(
                "Manager(%r): no PLC prim '%s%s' is loaded, so no data will arrive until one "
                "exists. A PLC is configured as a prim under /PLC/ (see the README).",
                name, system.system_root, name)

    def __del__(self):
        self.cleanup()

    def cleanup(self):
        for subscription in self._callbacks:
            subscription.unsubscribe()
        self._callbacks = []

    def _subscribe(self, event_type: str, callback):
        self._callbacks.append(self._event_stream.create_subscription_to_push_by_type(
            get_stream_name(event_type, self._plc_name), callback))

    def register_init_callback(self, callback: Callable[[carb.events.IEvent], None]):
        self._subscribe(self._events.EVENT_TYPE_DATA_INIT, callback)
        callback(None)

    def register_data_callback(self, callback: Callable[[carb.events.IEvent], None]):
        self._subscribe(self._events.EVENT_TYPE_DATA_READ, callback)

    def register_status_callback(self, callback: Callable[[carb.events.IEvent], None]):
        self._subscribe(self._events.EVENT_TYPE_STATUS, callback)

    def register_connection_callback(self, callback: Callable[[carb.events.IEvent], None]):
        self._subscribe(self._events.EVENT_TYPE_CONNECTION, callback)

    def add_cyclic_read_variables(self, variable_name_array: list):
        self._event_stream.push(
            event_type=get_stream_name(self._events.EVENT_TYPE_DATA_READ_REQ, self._plc_name),
            payload={"variables": list(variable_name_array)})

    def write_variable(self, name: str, value: Any):
        self.write_variables({name: value})

    def write_variables(self, data: dict):
        payload = {"variables": [{"name": name, "value": value} for name, value in data.items()]}
        self._event_stream.push(
            event_type=get_stream_name(self._events.EVENT_TYPE_DATA_WRITE_REQ, self._plc_name),
            payload=payload)


def get_system():
    from .delivery import get_system as _get
    return _get()
