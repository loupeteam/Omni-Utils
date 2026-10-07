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
Extension entry point.

on_startup creates the System that owns one Runtime (driver + worker thread)
and the registered components (bus adapter, USD mirror) per PLC prim, builds
the menu entry and the (initially hidden) PLC Bridge window, and keeps the
runtimes in step with the stage on every open and close. Vendor extensions
depend on this one and register their driver in their own on_startup; the
System picks up the prims that name it as soon as it is registered.
"""

import asyncio
import gc
import logging
import weakref

import omni.ext
import omni.kit.app
import omni.ui as ui
import omni.usd
from omni.kit.menu.utils import MenuItemDescription, add_menu_items, remove_menu_items
from omni.usd import StageEventType

from .delivery import _set_system
from .System import System
from .SystemUI import SystemUI

logger = logging.getLogger(__name__)

EXTENSION_NAME = "loupe.simulation.bridge"
EXTENSION_TITLE = "PLC Bridge"
MENU_HEADER = "Loupe"
MENU_ITEM_NAME = "PLC Bridge"


class Extension(omni.ext.IExt):
    def on_startup(self, ext_id: str):
        self._ext_id = ext_id
        self._system = System()
        self._system.install_default_components()
        _set_system(self._system)

        self._usd_context = omni.usd.get_context()

        self._window = ui.Window(
            title=EXTENSION_TITLE, width=600, height=500, visible=False,
            dockPreference=ui.DockPreference.LEFT_BOTTOM,
        )
        self._window.set_visibility_changed_fn(self._on_window)
        self._menu_items = [
            MenuItemDescription(name=MENU_ITEM_NAME, onclick_fn=lambda a=weakref.proxy(self): a._menu_callback())
        ]
        add_menu_items(self._menu_items, MENU_HEADER)
        self._ui = SystemUI(self._system)

        # Stage events are subscribed here, not when the window is opened: the
        # runtimes follow the stage whether or not there is a window.
        self._stage_event_sub = (
            self._usd_context.get_stage_event_stream().create_subscription_to_pop(self._on_stage_event)
        )
        self._system.find_and_create_components()

    def on_shutdown(self):
        remove_menu_items(self._menu_items, MENU_HEADER, True)
        self._stage_event_sub = None
        self._window = None
        self._ui.cleanup()
        self._system.dispose()
        _set_system(None)
        gc.collect()

    system = property(lambda self: self._system)

    def _on_stage_event(self, event):
        if event.type in (int(StageEventType.OPENED), int(StageEventType.CLOSED)):
            # Drop the old runtimes and rebuild from whatever the (new) stage holds.
            self._system.cleanup()
            self._system.find_and_create_components()

    def _on_window(self, visible):
        if self._window.visible:
            self._build_ui()

    def _build_ui(self):
        with self._window.frame:
            with ui.VStack(spacing=5, height=0):
                self._ui.build_ui()

        async def dock_window():
            await omni.kit.app.get_app().next_update_async()
            target = ui.Workspace.get_window("Viewport")
            window = ui.Workspace.get_window(EXTENSION_TITLE)
            if window and target:
                window.dock_in(target, ui.DockPosition.LEFT, 0.33)

        self._task = asyncio.ensure_future(dock_window())

    def _menu_callback(self):
        self._window.visible = not self._window.visible
        self._ui.on_menu_callback()
