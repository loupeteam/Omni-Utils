# This software contains source code provided by NVIDIA Corporation.
# Copyright (c) 2022-2023, NVIDIA CORPORATION.  All rights reserved.
#
# NVIDIA CORPORATION and its licensors retain all intellectual property
# and proprietary rights in and to this software, related documentation
# and any modifications thereto.  Any use, reproduction, disclosure or
# distribution of this software and related documentation without an express
# license agreement from NVIDIA CORPORATION is strictly prohibited.
#

import omni.ui as ui
import omni.timeline
import logging
import carb.events
import asyncio
import time

from threading import Timer
from .System import System
from .BridgeManager import Manager_Events
from .RuntimeBase import get_stream_name
import json

logger = logging.getLogger(__name__)


LABEL_WIDTH = 100
BUTTON_WIDTH = 100


class SystemUI:
    """
    Base class for creating a UI for a component.
    
    This class is meant to be inherited by the user to create a UI for a component.
    
    Override the following methods:
        - build_component_ui_runtime
        - _update_status
        - _cleanup
        - _on_data_read
    """
    def __init__(self, component_manager: System, bridge_events: Manager_Events):
        # UI elements created using a UIElementWrapper instance
        self.wrapped_ui_elements = []

        # Get access to the timeline to control stop/pause/play programmatically
        self._timeline = omni.timeline.get_timeline_interface()

        self._component_manager = component_manager
        self._bridge_manager = bridge_events

        # Data stream where the extension will dump the data that it reads from the component.
        self._event_stream = omni.kit.app.get_app().get_message_bus_event_stream()
        self._active_component = None
        self._component_subscriptions = []

        self._status_stack = dict()
        self._timer = None

    active_runtime = property(
        lambda self: self._component_manager.get_component(self._active_component)
    )

    def select_component(self, index):
        self.unsubscribe_component()
        self._active_component = index
        name = self._component_manager.get_component(index).name

        self._component_subscriptions = [
            self._event_stream.create_subscription_to_push_by_type(
                get_stream_name(self._bridge_manager.EVENT_TYPE_DATA_READ, name),
                self.on_data_read,
            ),
            self._event_stream.create_subscription_to_push_by_type(
                get_stream_name(self._bridge_manager.EVENT_TYPE_CONNECTION, name),
                self.on_connection,
            ),
            self._event_stream.create_subscription_to_push_by_type(
                get_stream_name(self._bridge_manager.EVENT_TYPE_STATUS, name),
                self.on_status,
            ),
        ]

        asyncio.ensure_future(self.build_component_ui())

    def unsubscribe_component(self):
        for subscription in self._component_subscriptions:
            subscription.unsubscribe()

    def cleanup(self):
        """
        Called when the stage is closed or the extension is hot reloaded.
        Perform any necessary cleanup such as removing active callback functions
        """
        self.unsubscribe_component()
        self._cleanup()
    
    def _cleanup(self):
        """
        **Overridable** Called when the stage is closed or the extension is hot reloaded.
        Perform any necessary cleanup such as removing active callback functions
        """
        pass

    def update_status(self):
        self.clean_status()
        status = str(self.get_status())
        self._status_field.model.set_value(status)
        if status != "[]":
            if self._timer:
                self._timer.cancel()
            self._timer = Timer(3, self.update_status)
            self._timer.start()
        
        self._update_status()
    
    def _update_status(self):
        """
        **Overridable** Called after the status is updated
        """
        pass

    def on_status(self, event):
        data = event.payload["status"]

        self.add_status(data)
        self.update_status()

    def get_status(self):
        status_list = []
        for status in self._status_stack:
            data = self._status_stack[status]
            status_list.append(data["data"])
        return status_list

    def clean_status(self):
        for status in list(self._status_stack.keys()):
            data = self._status_stack[status]
            if time.time() - data["time"] > 3:
                del self._status_stack[status]

    def add_status(self, data):
        self._status_stack[hash(str(data))] = {"time": time.time(), "data": str(data)}

    def on_connection(self, event):
        data = event.payload["status"]
        self.add_status(data)
        self.update_status()

    def on_data_read(self, event:carb.events.IEvent):
        data = event.payload["data"]
        data = json.dumps(data, indent=2, sort_keys=True)
        try:
            self._monitor_field.model.set_value(data)
            self.update_status()
        except Exception:
            pass
        self._on_data_read(event)
        
    def _on_data_read(self, event:carb.events.IEvent):
        """
        **Overridable** Called when new data is read from the component after default processing
        """
        pass

    def on_menu_callback(self):
        """**Overridable** Callback for when the UI is opened from the toolbar.
        This is called directly after build_ui().
        """
        pass

    def build_ui(self):
        """
        Build a custom UI tool to run your extension.
        This function will be called any time the UI window is closed and reopened.
        """

        self.components = self._component_manager.get_component_names()

        with ui.CollapsableFrame("Selection", collapsed=False):
            with ui.VStack(spacing=5, height=0):
                # Add a new component
                with ui.HStack(spacing=5, height=0):
                    ui.Label("Add component", width=LABEL_WIDTH)
                    self._component_name_field = ui.StringField(
                        ui.SimpleStringModel("Component_1")
                    )
                    ui.Button("Add", clicked_fn=self.add_component, width=BUTTON_WIDTH)

                with ui.HStack(spacing=5, height=0):
                    ui.Label("Select component", width=LABEL_WIDTH)
                    self._component_dropdown = ui.ComboBox(0, *self.components)
                    self._component_dropdown.model.add_item_changed_fn(
                        self.on_component_selected
                    )
                    ui.Button(
                        "Refresh",
                        clicked_fn=self.refresh_components,
                        width=BUTTON_WIDTH,
                    )

        self._component_ui = ui.VStack(spacing=5, height=0)

        if len(self.components) == 0:
            return

        self.select_component(self.components[0])

    def on_component_selected(
        self, item_model: ui.AbstractItemModel, item: ui.AbstractItem
    ):
        """
        Called when a component is selected from the dropdown
        """
        components = self.components

        if len(components) < 1 or item_model.get_item_value_model().as_int >= len(
            components
        ):
            return
        selected = self.components[item_model.get_item_value_model().as_int]

        self.select_component(selected)

    async def build_component_ui(self):
        self._component_ui.clear()

        if self.active_runtime is None:
            return

        with self._component_ui:
            with ui.CollapsableFrame("Configuration", collapsed=False):

                with ui.VStack(spacing=5, height=0):

                    with ui.HStack(spacing=5, height=0):
                        ui.Label("Name", width=LABEL_WIDTH)
                        ui.Label(self.active_runtime.name, width=LABEL_WIDTH)

                    self.build_component_ui_runtime()
                    self.build_component_ui_configuration()

                    with ui.HStack(spacing=5, height=0):
                        ui.Label("Settings", width=LABEL_WIDTH)
                        ui.Button(
                            "Update From USD",
                            clicked_fn=self.load_settings,
                            width=BUTTON_WIDTH,
                        )
                        ui.Button(
                            "Write To USD",
                            clicked_fn=self.save_settings,
                            width=BUTTON_WIDTH,
                        )

            self.build_component_ui_end()
            
            with ui.CollapsableFrame("Monitor", collapsed=False):
                with ui.VStack(spacing=5, height=500):
                    with ui.HStack(spacing=5, height=0):
                        ui.Label("Status", width=LABEL_WIDTH)
                        self._status_field = ui.StringField(
                            ui.SimpleStringModel("n/a"), read_only=True
                        )
                    self._monitor_field = ui.StringField(
                        ui.SimpleStringModel("{}"), multiline=True, read_only=True
                    )

    def build_component_ui_runtime(self):
        """
        **DEPRECATED!** **Overridable** Build the UI for the runtime object of the selected component
        Called by build_component_ui after the component is selected
        """
        pass

    def build_component_ui_configuration(self):
        """
        **Overridable** Build the UI for the configuration of the selected component
        Called by build_component_ui after the component is selected
        """
        pass
    
    def build_component_ui_end(self):
        """
        **Overridable** Build the UI for the selected component
        Called by build_component_ui after the component is selected
        """
        pass

    def add_component(self):
        name = self._component_name_field.model.as_string
        self._component_manager.add_component(name, {})
        self.components = self._component_manager.get_component_names()

        updateComboBox(self._component_dropdown, self.components)

        self.select_component(self.components[len(self.components) - 1])

    def refresh_components(self):
        self.components = self._component_manager.find_and_create_components()
        updateComboBox(self._component_dropdown, self.components)
        asyncio.ensure_future(self.build_component_ui())

    ####################################
    ####################################
    # Manage Settings
    ####################################
    ####################################
    def save_settings(self):
        self._component_manager.write_options_to_stage(self._active_component)

    def load_settings(self):
        self._component_manager.read_options_from_stage(self._active_component)
        asyncio.ensure_future(self.build_component_ui())


def updateComboBox(comboBox, items):
    combo_box = comboBox.model
    ...
    # clean combo box
    for item in combo_box.get_item_children():
        combo_box.remove_item(item)
    ...
    # fill combo box
    for value in items:
        combo_box.append_child_item(None, ui.SimpleStringModel(value))
