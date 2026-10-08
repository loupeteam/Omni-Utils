"""
How Kit code gets PLC data: the system, a PLC's runtime, and a per-frame
main-thread callback.

Copyright (c) 2024 Loupe, https://loupe.team
Part of Omni-Utils, licensed under the MIT License.

    from loupe.simulation.bridge import get_plc, on_sample_main

    plc = get_plc("PLC1")                       # the plc_bridge.PlcRuntime
    plc.on_sample(cb)                           # every sample, worker thread
    sample = plc.latest()                       # pull, any thread
    remove = on_sample_main("PLC1", cb)         # newest sample, once per app
                                                # update, main thread

`on_sample_main` is the documented default for Kit code that touches the
stage or the UI. It keeps only the newest sample between two app updates, so
at 15 fps with a 20 ms PLC period a callback sees one sample in three; the
others are dropped. A consumer that needs every sample (edge detection on a
short pulse) uses `plc.on_sample` and marshals itself, or compares
`Sample.seq` between calls to see how many were skipped.
"""

import logging
import threading
from typing import Callable, Optional

import omni.kit.app

from plc_bridge import PlcRuntime, Sample

logger = logging.getLogger(__name__)

_system = None


def get_system():
    """The one System the extension owns, or None before it started."""
    return _system


def _set_system(system):
    global _system
    _system = system


def get_plc(name: str) -> Optional[PlcRuntime]:
    """The PlcRuntime of the PLC named `name` (`/PLC/<name>`), or None when no such PLC is loaded."""
    system = get_system()
    if system is None:
        return None
    runtime = system.get_component(name)
    return None if runtime is None else runtime.plc


def on_sample_main(name: str, callback: Callable[[Sample], None]) -> Callable[[], None]:
    """
    Call `callback(sample)` on the main thread, once per app update, with the
    newest sample the PLC named `name` produced since the previous update.
    Nothing is called for an update without a new sample.

    The registration survives the PLC: it is attached when a PLC of that name
    is created (a stage open) and detached when it goes, so it can be made
    before the stage is loaded.

    Returns:
        A function that removes the callback again.
    """
    system = get_system()
    if system is None:
        raise RuntimeError("loupe.simulation.bridge is not started")
    return system.delivery.on_sample_main(name, callback)


class MainThreadDelivery:
    """
    The coalescing step behind `on_sample_main`. One per System; the System
    tells it when a runtime appears or goes.

    The worker thread stores the newest sample per PLC under a lock; the app
    update callback (main thread) takes them all and calls the listeners. A
    listener that raises is logged and does not stop the others.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._callbacks = {}      # name -> [callback]
        self._pending = {}        # name -> newest Sample since the last update
        self._calls = []          # callables to run on the next update, main thread
        self._attached = {}       # name -> remover for the runtime's on_sample listener
        self._frames = 0
        self._delivered = 0
        self._update_sub = (
            omni.kit.app.get_app().get_update_event_stream()
            .create_subscription_to_pop(self._on_update, name="loupe.simulation.bridge.delivery")
        )

    def cleanup(self):
        self._update_sub = None
        for remove in list(self._attached.values()):
            remove()
        self._attached.clear()
        with self._lock:
            self._pending.clear()
            self._callbacks.clear()
            self._calls.clear()

    # Counters for the harness and the tests.
    frames = property(lambda self: self._frames, doc="App updates seen.")
    delivered = property(lambda self: self._delivered, doc="Callback invocations made.")

    def on_sample_main(self, name: str, callback: Callable[[Sample], None]) -> Callable[[], None]:
        with self._lock:
            self._callbacks.setdefault(name, []).append(callback)

        def remove():
            with self._lock:
                callbacks = self._callbacks.get(name, [])
                if callback in callbacks:
                    callbacks.remove(callback)
                if not callbacks:
                    self._callbacks.pop(name, None)
                    self._pending.pop(name, None)

        return remove

    def listeners(self, name: str) -> int:
        with self._lock:
            return len(self._callbacks.get(name, ()))

    def attach(self, name: str, plc: PlcRuntime):
        """Start coalescing samples of this runtime. Called by the System when a component is created."""
        self.detach(name)
        self._attached[name] = plc.on_sample(lambda sample, name=name: self._store(name, sample))

    def detach(self, name: str):
        remove = self._attached.pop(name, None)
        if remove is not None:
            remove()
        with self._lock:
            self._pending.pop(name, None)

    def _store(self, name: str, sample: Sample):
        # Worker thread. Keep only the newest; a cheap dict assignment, so the
        # PLC loop is never slowed by a busy main thread.
        with self._lock:
            # Attached, not merely subscribed: a runtime that was detached
            # while this sample was in flight must not leave a stale delivery.
            if name in self._attached and name in self._callbacks:
                self._pending[name] = sample

    def call_on_main(self, fn: Callable[[], None]):
        """Run `fn()` on the main thread at the next app update. Safe from any thread."""
        with self._lock:
            self._calls.append(fn)

    def _on_update(self, event):
        self._frames += 1
        with self._lock:
            calls, self._calls = self._calls, []
        for fn in calls:
            try:
                fn()
            except Exception:
                logger.exception("a call_on_main callable raised")
        with self._lock:
            if not self._pending:
                return
            batch = self._pending
            self._pending = {}
            callbacks = {name: list(self._callbacks.get(name, ())) for name in batch}
        for name, sample in batch.items():
            for callback in callbacks[name]:
                # A callback earlier in this batch may have removed the
                # component (or this listener); re-check before each call.
                if name not in self._attached:
                    break
                with self._lock:
                    if callback not in self._callbacks.get(name, ()):
                        continue
                self._delivered += 1
                try:
                    callback(sample)
                except Exception:
                    logger.exception("%s: an on_sample_main listener raised", name)
