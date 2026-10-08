# This software contains source code provided by NVIDIA Corporation.
# Copyright (c) 2022-2023, NVIDIA CORPORATION.  All rights reserved.
#
# NVIDIA CORPORATION and its licensors retain all intellectual property
# and proprietary rights in and to this software, related documentation
# and any modifications thereto.  Any use, reproduction, disclosure or
# distribution of this software and related documentation without an express
# license agreement from NVIDIA CORPORATION is strictly prohibited.
#
# Modifications copyright (c) 2024 Loupe, https://loupe.team, MIT License.

"""
The PLC Bridge window: a vendor-neutral panel per PLC (driver, enable,
refresh rate, variables, mirror, status, connection, live data) with the
driver's options below it, from the driver's `ui_panel` callback when it
registered one, else one field per option of its schema.
"""

import json
import logging
import threading
import time

import omni.kit.app
import omni.ui as ui

from . import registry
from .System import System

logger = logging.getLogger(__name__)

LABEL_WIDTH = 120
BUTTON_WIDTH = 100
STATUS_TTL_SEC = 3.0
MONITOR_EVERY_FRAMES = 6


class SystemUI:
    """Builds the window contents for a System. `build_ui()` is called whenever the window opens."""

    def __init__(self, system: System):
        self._system = system
        self._active = None
        self._status = {}           # text -> time, filled from the worker thread
        self._status_lock = threading.Lock()
        self._connection = "n/a"
        self._dirty = False
        self._removers = []
        self._frame = 0
        self._component_ui = None
        self._component_dropdown = None
        self._status_field = None
        self._connection_field = None
        self._monitor_field = None
        self._update_sub = (
            omni.kit.app.get_app().get_update_event_stream()
            .create_subscription_to_pop(self._on_update, name="loupe.simulation.bridge.ui")
        )

    active_runtime = property(lambda self: self._system.get_component(self._active) if self._active else None)

    def cleanup(self):
        self._unsubscribe()
        self._update_sub = None

    def on_menu_callback(self):
        pass

    # region - Selection

    def build_ui(self):
        self.components = self._system.get_component_names()
        drivers = registry.names()
        with ui.CollapsableFrame("Selection", collapsed=False):
            with ui.VStack(spacing=5, height=0):
                with ui.HStack(spacing=5, height=0):
                    ui.Label("Add component", width=LABEL_WIDTH)
                    self._component_name_field = ui.StringField(ui.SimpleStringModel("PLC1"))
                    self._driver_dropdown = ui.ComboBox(0, *drivers, width=120)
                    ui.Button("Add", clicked_fn=self.add_component, width=BUTTON_WIDTH)
                with ui.HStack(spacing=5, height=0):
                    ui.Label("Select component", width=LABEL_WIDTH)
                    self._component_dropdown = ui.ComboBox(0, *self.components)
                    self._component_dropdown.model.add_item_changed_fn(self.on_component_selected)
                    ui.Button("Refresh", clicked_fn=self.refresh_components, width=BUTTON_WIDTH)
                unresolved = self._system.unresolved
                if unresolved:
                    text = ", ".join(f"{path} ({driver})" for path, driver in unresolved.items())
                    ui.Label(f"No driver registered for: {text}", word_wrap=True, height=0)
                invalid = self._system.invalid
                if invalid:
                    text = "; ".join(f"{path}: {reason}" for path, reason in invalid.items())
                    ui.Label(f"Skipped (bad attributes or options): {text}", word_wrap=True, height=0)
                if not drivers:
                    ui.Label("No driver is registered: enable a vendor bridge extension.", height=0)
        self._component_ui = ui.VStack(spacing=5, height=0)
        if not self.components:
            return
        self.select_component(self.components[0])

    def on_component_selected(self, item_model, item):
        index = item_model.get_item_value_model().as_int
        if index < 0 or index >= len(self.components):
            return
        self.select_component(self.components[index])

    def select_component(self, name):
        self._unsubscribe()
        self._active = name
        runtime = self.active_runtime
        if runtime is None:
            return
        plc = runtime.plc
        self._removers = [
            plc.on_problem(lambda p: self._add_status(p.text)),
            plc.on_connection(self._on_connection),
            self._system.delivery.on_sample_main(name, self._on_sample),
        ]
        self._connection = "Connected" if runtime.is_connected else "Disconnected"
        self._dirty = True
        self.build_component_ui()

    def _unsubscribe(self):
        for remove in self._removers:
            remove()
        self._removers = []

    def add_component(self):
        name = self._component_name_field.model.as_string.strip()
        drivers = registry.names()
        if not name or not drivers:
            return
        index = self._driver_dropdown.model.get_item_value_model().as_int
        driver = drivers[min(max(index, 0), len(drivers) - 1)]
        try:
            self._system.add_component(name, {}, driver=driver)
        except Exception as e:
            # A bad name, a driver that rejects its defaults: report it in the
            # window; the System has left no prim behind.
            logger.warning("Add component %r failed: %s", name, e)
            self._add_status(f"Add {name}: {e}")
            return
        self.components = self._system.get_component_names()
        update_combo_box(self._component_dropdown, self.components)
        self.select_component(name)

    def on_system_rebuilt(self, visible: bool):
        """
        The System dropped its runtimes and built new ones (a stage was opened
        or closed). The callbacks on the old, stopped runtime are removed;
        with the window open the list and the panel are rebuilt from the new
        runtimes, otherwise `build_ui` does that when the window next opens.
        """
        self._unsubscribe()
        self._active = None
        self._connection = "n/a"
        if not visible or self._component_dropdown is None:
            return
        self.components = self._system.get_component_names()
        update_combo_box(self._component_dropdown, self.components)
        if self.components:
            self.select_component(self.components[0])
        elif self._component_ui is not None:
            self._component_ui.clear()

    def refresh_components(self):
        self.components = self._system.find_and_create_components() or []
        update_combo_box(self._component_dropdown, self.components)
        if self.components:
            self.select_component(self.components[0])
        else:
            self._unsubscribe()
            self._active = None
            if self._component_ui is not None:
                self._component_ui.clear()

    # endregion
    # region - Component panel

    def build_component_ui(self):
        self._component_ui.clear()
        runtime = self.active_runtime
        if runtime is None:
            return
        spec = runtime.spec
        with self._component_ui:
            with ui.CollapsableFrame("Configuration", collapsed=False):
                with ui.VStack(spacing=5, height=0):
                    self._row("Name", lambda: ui.Label(runtime.name))
                    self._row("Prim", lambda: ui.Label(runtime.path))
                    self._row("Driver", lambda: ui.Label(
                        f"{spec.title} ({spec.name})" + ("  [0.2.x attributes, deprecated]" if runtime.legacy else "")))

                    with ui.HStack(spacing=5, height=0):
                        ui.Label("Enable", width=LABEL_WIDTH)
                        self._enable_checkbox = ui.CheckBox(ui.SimpleBoolModel(runtime.enable_communication))
                        self._enable_checkbox.model.add_value_changed_fn(self._toggle_enable)
                    if runtime.held:
                        ui.Label("Disabled: " + runtime.held, word_wrap=True, height=0)

                    with ui.HStack(spacing=5, height=0):
                        ui.Label("Refresh Rate (ms)", width=LABEL_WIDTH)
                        self._refresh_field = ui.IntField(ui.SimpleIntModel(int(runtime.refresh_rate)))
                        self._refresh_field.model.set_min(1)
                        self._refresh_field.model.set_max(60000)
                        self._refresh_field.model.add_end_edit_fn(self._on_refresh_changed)

                    self._row("Mirror to USD", lambda: ui.Label(
                        ("on" if runtime.mirror else "off")
                        + (f", watching {', '.join(runtime.mirror_symbols)}" if runtime.mirror_symbols else "")
                        + "  (bridge:MirrorToUsd / bridge:MirrorSymbols on the prim)"))

                    with ui.CollapsableFrame("Cyclic Read Variables", collapsed=True):
                        with ui.VStack(spacing=5, height=200):
                            ui.Label("1 variable per line", height=0)
                            self._variables_field = ui.StringField(
                                ui.SimpleStringModel("\n".join(runtime.read_variables)), multiline=True)
                            self._variables_field.model.add_end_edit_fn(self._on_variables_changed)

                    if spec.ui_panel is not None:
                        try:
                            spec.ui_panel(runtime, spec)
                        except Exception:
                            logger.exception("%s: the driver's ui_panel raised", spec.name)
                    else:
                        self.build_driver_options(runtime, spec)

                    with ui.HStack(spacing=5, height=0):
                        ui.Label("Settings", width=LABEL_WIDTH)
                        ui.Button("Update From USD", clicked_fn=self.load_settings, width=BUTTON_WIDTH + 20)
                        ui.Button("Write To USD", clicked_fn=self.save_settings, width=BUTTON_WIDTH + 20)

            with ui.CollapsableFrame("Monitor", collapsed=False):
                with ui.VStack(spacing=5, height=500):
                    with ui.HStack(spacing=5, height=0):
                        ui.Label("Connection", width=LABEL_WIDTH)
                        self._connection_field = ui.StringField(ui.SimpleStringModel(self._connection), read_only=True)
                    with ui.HStack(spacing=5, height=0):
                        ui.Label("Status", width=LABEL_WIDTH)
                        self._status_field = ui.StringField(ui.SimpleStringModel("[]"), read_only=True)
                    self._monitor_field = ui.StringField(ui.SimpleStringModel("{}"), multiline=True, read_only=True)
        self._dirty = True

    @staticmethod
    def _row(label, build):
        with ui.HStack(spacing=5, height=0):
            ui.Label(label, width=LABEL_WIDTH)
            build()

    def build_driver_options(self, runtime, spec):
        """One field per option of the driver's schema; the default when it registered no ui_panel."""
        values = runtime.driver_options
        for option in spec.options:
            with ui.HStack(spacing=5, height=0):
                ui.Label(option.label or option.key, width=LABEL_WIDTH)
                value = values.get(option.key)
                if option.kind == "bool" and not option.secret:
                    field = ui.CheckBox(ui.SimpleBoolModel(bool(value)))
                    field.model.add_value_changed_fn(
                        lambda m, key=option.key: self._set_driver_option(key, m.get_value_as_bool()))
                elif option.kind == "int" and not option.secret:
                    field = ui.IntField(ui.SimpleIntModel(int(value or 0)))
                    field.model.add_end_edit_fn(
                        lambda m, key=option.key: self._set_driver_option(key, m.get_value_as_int()))
                elif option.kind == "float" and not option.secret:
                    field = ui.FloatField(ui.SimpleFloatModel(float(value or 0.0)))
                    field.model.add_end_edit_fn(
                        lambda m, key=option.key: self._set_driver_option(key, m.get_value_as_float()))
                else:
                    text = ",".join(value) if isinstance(value, (list, tuple)) else ("" if value is None else str(value))
                    field = ui.StringField(ui.SimpleStringModel(text))
                    field.model.add_end_edit_fn(
                        lambda m, key=option.key: self._set_driver_option(key, m.get_value_as_string()))
            if option.secret:
                ui.Label("    a reference: env:NAME or setting:/path; the value is never stored on the prim", height=0)

    # endregion
    # region - Edits

    def _set_driver_option(self, key, value):
        runtime = self.active_runtime
        if runtime is not None:
            try:
                runtime.set_driver_option(key, value)
            except Exception as e:
                self._add_status(f"{key}: {e}")

    def _toggle_enable(self, model):
        runtime = self.active_runtime
        if runtime is not None:
            runtime.enable_communication = model.get_value_as_bool()

    def _on_refresh_changed(self, model):
        runtime = self.active_runtime
        if runtime is not None:
            runtime.refresh_rate = model.get_value_as_int()

    def _on_variables_changed(self, model):
        runtime = self.active_runtime
        if runtime is not None:
            runtime.set_read_variables(model.get_value_as_string().split("\n"))

    def save_settings(self):
        if self._active:
            self._system.write_options_to_stage(self._active)

    def load_settings(self):
        if self._active:
            self._system.read_options_from_stage(self._active)
            self.build_component_ui()

    # endregion
    # region - Monitor

    def _add_status(self, text):
        # Worker thread: only record; the update callback touches the widgets.
        with self._status_lock:
            self._status[str(text)] = time.time()
        self._dirty = True

    def _on_connection(self, state):
        self._connection = state
        self._add_status(state)

    def _on_sample(self, sample):
        self._frame += 1
        if self._frame % MONITOR_EVERY_FRAMES or self._monitor_field is None:
            return
        try:
            self._monitor_field.model.set_value(json.dumps(sample.nested, indent=2, sort_keys=True, default=str))
        except Exception:
            pass

    def _on_update(self, event):
        now = time.time()
        with self._status_lock:
            expired = [text for text, t in self._status.items() if now - t > STATUS_TTL_SEC]
            for text in expired:
                del self._status[text]
                self._dirty = True
            if not self._dirty:
                return
            self._dirty = False
            texts = list(self._status)
        if self._status_field is not None:
            self._status_field.model.set_value(str(texts))
        if self._connection_field is not None:
            self._connection_field.model.set_value(self._connection)

    # endregion


def update_combo_box(combo_box, items):
    model = combo_box.model
    for item in model.get_item_children():
        model.remove_item(item)
    for value in items:
        model.append_child_item(None, ui.SimpleStringModel(value))
