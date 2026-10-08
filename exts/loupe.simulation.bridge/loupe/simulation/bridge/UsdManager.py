"""
The USD mirror: PLC values as prims under the PLC prim, and `write:value`
edits on those prims as writes to the PLC.

Copyright (c) 2024 Loupe, https://loupe.team
Part of Omni-Utils, licensed under the MIT License.

A registered component (see System.py), created for a PLC whose prim has
`bridge:MirrorToUsd` true (absent means true in 0.3). It takes the newest
sample once per app update through the main-thread delivery, so everything
here runs on the main thread, and it uses the sample's flat `values`
directly: the symbol a value was read under is the symbol written back, so a
B&R `TestProg:lreal` stays `TestProg:lreal` (0.2.x re-derived the name from
the nested data and lost the colon). Everything the mirror authors goes into
the session layer: runtime state, never saved, never a pending change.

Layout, for a Beckhoff symbol `GVL.Axes[0].ActualPosition` on `/PLC/PLC1`:

    /PLC/PLC1/GVL/Axes/_0/ActualPosition
        double value          the last read value
        double write:value    edit it to write (declared, no session opinion)
        bool   write:once     set true to send write:value once
        bool   write:pause    true: edits of write:value are not sent
        string symbol         "GVL.Axes[0].ActualPosition"

A struct or array read whole (one symbol whose value is a dict or list) is
expanded into members and `_<index>` children the same way; `None` entries of
a padded array are skipped. `bridge:MirrorSymbols` narrows the mirror to the
listed symbols (and everything under them); empty means every variable.
"""

import ast
import logging
import re
from contextlib import contextmanager

import numpy as np
import omni.usd
from pxr import Gf, Sdf, Tf, Usd, UsdGeom

logger = logging.getLogger(__name__)

ATTR_CURRENT_VALUE = "value"
ATTR_WRITE_VALUE = "write:value"
ATTR_WRITE_PAUSE = "write:pause"
ATTR_WRITE_ONCE = "write:once"
ATTR_WRITE_SYMBOL = "symbol"

# Kept for code that imported it from here; the schema module owns it now.
ATTR_MIRROR_USD = "bridge:MirrorToUsd"

# How many times a symbol may fail to be written before it is given up on. More
# than one because a failure is not always permanent: create_attr fixes an
# attribute's USD type from the first value it ever sees, so a single bad
# sample would otherwise disable a symbol whose every later value is fine.
UNWRITABLE_RETRIES = 3

# A given-up symbol is tried again once every this many updates, so a symbol
# that failed for a while (a PLC that dropped out and came back) recovers by
# itself instead of staying dark until the stage is reopened.
UNWRITABLE_RETRY_EVERY = 120

# A symbol part from here on names a USD operation rather than a PLC struct
# member: "...usd:attr:xformOp:translate" sets that attribute on the parent
# prim, "...usd:type:Xform" defines the parent prim with that type.
USD_OP_MARKER = "usd:"


class RuntimeUsd:
    """
    The mirror of one PLC.

    Args:
        prim_path: the PLC prim; mirrored values go under it.
        runtime: the framework Runtime of the PLC (writes go to `runtime.queue_write`).
        delivery: the System's MainThreadDelivery, which hands over samples.
        watch: symbols to mirror; empty means all.
    """

    def __init__(self, prim_path, runtime, delivery, watch=()):
        self._root_prim = None
        self._root_prim_path = prim_path
        self._runtime = runtime
        self._separators = runtime.driver.symbol_separators or "."
        self._watch = [w for w in (watch or ()) if w]
        self._in_notice = False

        # Symbols this mirror could not represent in USD, with a failure count,
        # so a value that cannot be written is attempted a few times rather
        # than every frame.
        self._unwritable = {}
        self._update_count = 0
        self._last_seq = None

        self._usd_context = omni.usd.get_context()
        self._stage = self._usd_context.get_stage()

        # No stage-event subscription: the System drops every mirror when a
        # stage is opened or closed and builds new ones on the new stage.
        # A write:value edit anywhere under the PLC prim becomes a write.
        self._stage_listener = Tf.Notice.Register(Usd.Notice.ObjectsChanged, self._notice_changed, self._stage)
        self._remove_delivery = delivery.on_sample_main(runtime.name, self._on_sample)

    def __del__(self):
        self.cleanup()

    @property
    def root_prim(self):
        if self._root_prim is not None:
            return self._root_prim
        self._root_prim = self._stage.GetPrimAtPath(self._root_prim_path)
        if not self._root_prim.IsValid():
            with session_layer_context(self._stage):
                self._root_prim = self._stage.DefinePrim(self._root_prim_path)
        return self._root_prim

    watch = property(lambda self: list(self._watch), doc="The watch list; empty means every symbol.")
    last_seq = property(lambda self: self._last_seq, doc="Sample.seq of the last sample mirrored.")

    def cleanup(self):
        self._stage_listener = None
        remove = getattr(self, "_remove_delivery", None)
        if remove is not None:
            remove()
            self._remove_delivery = None

    # region - PLC -> USD

    def _wanted(self, symbol: str) -> bool:
        if not self._watch:
            return True
        for entry in self._watch:
            if symbol == entry:
                return True
            if symbol.startswith(entry) and symbol[len(entry)] in self._separators + "[":
                return True
        return False

    def _on_sample(self, sample):
        """Main thread, once per app update, with the newest sample."""
        if self._stage is None or self._stage.expired:
            return
        self._last_seq = sample.seq
        flat = {}
        for symbol, value in sample.values.items():
            expand_value(symbol, value, flat)
        if self._watch:
            flat = {symbol: value for symbol, value in flat.items() if self._wanted(symbol)}
        if not flat:
            return

        self._update_count += 1
        retry_now = self._update_count % UNWRITABLE_RETRY_EVERY == 0
        create = {}
        with session_layer_context(self._stage):
            # Resolve (and if needed define) the root prim before the change
            # block: a prim defined inside an Sdf.ChangeBlock is not composed
            # until the block ends, so DefinePrim fails.
            root_path = self.root_prim.GetPath().pathString
            with Sdf.ChangeBlock():
                for key, value in flat.items():
                    if self._unwritable.get(key, 0) >= UNWRITABLE_RETRIES and not retry_now:
                        continue
                    full_key = symbol_to_prim_path(root_path, key, self._separators)
                    op = get_op_from_key(full_key, key)
                    try:
                        if not op.execute(self._stage, value):
                            create[full_key] = (op, value)
                        else:
                            self._unwritable.pop(key, None)
                    except Exception as e:
                        self._mark_unwritable(key, value, e)
            # New prims are defined outside the change block.
            for key, (op, value) in create.items():
                try:
                    op.create(self._stage, value)
                    self._unwritable.pop(op.key, None)
                except Exception as e:
                    self._mark_unwritable(op.key, value, e)

    def _mark_unwritable(self, key, value, error):
        """Count a symbol that could not be mirrored, and give up on it after a few tries, with one warning."""
        self._unwritable[key] = self._unwritable.get(key, 0) + 1
        if self._unwritable[key] != UNWRITABLE_RETRIES:
            return
        logger.warning(
            "USD mirror: cannot represent '%s' (%s), so it will not be mirrored. "
            "The value is still delivered to data subscribers. Reason: %s",
            key, type(value).__name__, error)

    # endregion
    # region - USD -> PLC

    def _notice_changed(self, notice, stage):
        if self._stage.expired:
            return
        # Resetting write:once below edits the stage, which re-enters this
        # handler synchronously; without the guard the nested call sent the
        # value a second time.
        if self._in_notice:
            return
        self._in_notice = True
        try:
            self._handle_write_changes(notice)
        finally:
            self._in_notice = False

    def _handle_write_changes(self, notice):
        # Sdf.Path prefix, not a string prefix: "/PLC/PLC1" is a string prefix
        # of "/PLC/PLC10/...", which would send PLC10's writes to PLC1 as well.
        root = Sdf.Path(self._root_prim_path)
        for changed in list(notice.GetChangedInfoOnlyPaths()):
            if not changed.HasPrefix(root):
                continue
            if changed.name not in (ATTR_WRITE_VALUE, ATTR_WRITE_ONCE, ATTR_WRITE_PAUSE):
                continue
            prim = self._stage.GetPrimAtPath(changed.GetPrimPath())
            # The symbol attribute is written last when a value prim is
            # created, so its absence means the prim is still being built.
            write_symbol = prim.GetAttribute(ATTR_WRITE_SYMBOL)
            if not write_symbol.IsValid():
                continue
            write_once_attr = prim.GetAttribute(ATTR_WRITE_ONCE)
            write_pause_attr = prim.GetAttribute(ATTR_WRITE_PAUSE)
            # Send when write:once is set, or when write:pause is not: pause
            # lets a user edit write:value without sending every keystroke.
            if write_once_attr.Get() or not write_pause_attr.Get():
                if write_once_attr.Get():
                    # Reset the trigger in the layer the user set it in (the
                    # current edit target). If an opinion survives in a
                    # stronger layer, clear that one too.
                    write_once_attr.Set(False)
                    if write_once_attr.Get():
                        with session_layer_context(self._stage):
                            write_once_attr.Set(False)
                value = prim.GetAttribute(ATTR_WRITE_VALUE).Get()
                # write:value is declared without a value; nothing to send
                # until the user sets one (write:once is reset above anyway).
                if value is not None:
                    self._runtime.queue_write(write_symbol.Get(), value)

    # endregion


@contextmanager
def session_layer_context(stage):
    """
    Direct edits to the stage's session layer. The session layer is composed
    like any other, so the prims are visible to the property window, scripts
    and OmniGraph, but it is never saved and its edits do not count as
    pending changes.
    """
    edit_target = Usd.EditTarget(stage.GetSessionLayer())
    with Usd.EditContext(stage, edit_target):
        yield


def expand_value(symbol: str, value, out: dict) -> dict:
    """
    Flatten one read value into `out`, keyed by the symbol the PLC knows.

    A scalar lands as is. A dict (a struct read whole) contributes
    `symbol.member`; a list or tuple (an array read whole, or the parser's
    `None`-padded list) contributes `symbol[index]` for every non-None entry.
    """
    if isinstance(value, dict):
        for member, item in value.items():
            expand_value(f"{symbol}.{member}", item, out)
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            if item is not None:
                expand_value(f"{symbol}[{index}]", item, out)
    else:
        out[symbol] = value
    return out


_ARRAY_INDEX = re.compile(r"\[(\d+)\]")


def symbol_to_prim_path(root_path: str, symbol: str, separators: str = ".") -> str:
    """
    Map a PLC symbol to the path of its mirror prim under the component prim.

    Every part becomes a child prim; the driver's separators (`.` for
    Beckhoff, `:` and `.` for B&R) all split. An array element becomes a
    child of the array named `_<index>`, because a USD prim name cannot start
    with a digit or contain brackets. A trailing `usd:...` part is kept whole
    (see UsdOp).

        "GVL.Axes[0].ActualPosition" -> "<root>/GVL/Axes/_0/ActualPosition"
        "TestProg:lreal"             -> "<root>/TestProg/lreal"
    """
    tail = None
    marker = symbol.find(USD_OP_MARKER)
    if marker > 0 and symbol[marker - 1] in separators:
        symbol, tail = symbol[:marker - 1], symbol[marker:]
    text = _ARRAY_INDEX.sub(r"/_\1", symbol)
    parts = [part for part in re.split("[" + re.escape(separators) + "/]", text) if part]
    if tail:
        parts.append(tail)
    return root_path + "/" + "/".join(parts)


def set_symbol_prim_value(stage, full_key, key, value) -> bool:
    """Set the value attribute of an existing prim; False when the prim or attribute has to be created."""
    prim = stage.GetPrimAtPath(full_key)
    if not prim:
        return False
    attr = prim.GetAttribute(key)
    if not attr:
        return False
    set_attr(attr, value)
    return True


def create_typed_prim(stage, full_key, value) -> None:
    stage.DefinePrim(full_key, value)


def create_symbol_prim_value(stage, full_key, attr, key, value) -> None:
    """Create a value prim: the value attribute, the write attributes, and the symbol last."""
    prim = stage.DefinePrim(full_key)
    create_attr(prim, attr, value)
    # Declare the write attributes but do not author a value for them. The
    # mirror authors into the session layer, which is stronger than the root
    # layer: a value authored here would mask everything the user later types
    # into write:value in the property window.
    declare_attr(prim, ATTR_WRITE_VALUE, value)
    declare_attr(prim, ATTR_WRITE_ONCE, False)
    declare_attr(prim, ATTR_WRITE_PAUSE, False)
    # The symbol last, so that _handle_write_changes can tell a prim under construction.
    if key:
        create_attr(prim, ATTR_WRITE_SYMBOL, key)


def declare_attr(prim: Usd.Prim, attr_name: str, like_value) -> Usd.Attribute:
    """Create an attribute with the USD type matching like_value, without authoring a value."""
    if type(like_value) is str:
        return prim.CreateAttribute(attr_name, Sdf.ValueTypeNames.String)
    if type(like_value) is bool:
        return prim.CreateAttribute(attr_name, Sdf.ValueTypeNames.Bool)
    return prim.CreateAttribute(attr_name, Sdf.ValueTypeNames.Double)


def set_attr(attr: Usd.Attribute, value) -> None:
    attr_type = attr.GetTypeName()
    if attr_type.cppTypeName == "GfMatrix4d":
        value = Gf.Matrix4d(np.array(ast.literal_eval(value)))
    elif attr_type.cppTypeName == "GfVec3f":
        val = ast.literal_eval(value)
        value = Gf.Vec3f(val[0], val[1], val[2])
    elif attr_type.cppTypeName == "GfVec3d":
        val = ast.literal_eval(value)
        value = Gf.Vec3d(val[0], val[1], val[2])
    attr.Set(value)


def create_attr(prim: Usd.Prim, attr_name: str, value) -> Usd.Attribute:
    """Create an attribute typed from the value and set it."""
    if attr_name == "xformOp:transform":
        UsdGeom.Xformable(prim).AddTransformOp()
    if attr_name == "xformOp:translate":
        UsdGeom.Xformable(prim).AddTranslateOp()
    if attr_name == "xformOp:rotateXYZ":
        UsdGeom.Xformable(prim).AddRotateXYZOp()
    if type(value) is str:
        attr = prim.CreateAttribute(attr_name, Sdf.ValueTypeNames.String)
    elif type(value) is bool:
        attr = prim.CreateAttribute(attr_name, Sdf.ValueTypeNames.Bool)
    else:
        attr = prim.CreateAttribute(attr_name, Sdf.ValueTypeNames.Double)
    set_attr(attr, value)
    return attr


def get_or_create_attr(prim: Usd.Prim, attr_name: str, attr_type: Sdf.ValueTypeName) -> Usd.Attribute:
    attr = prim.GetAttribute(attr_name)
    if not attr:
        attr = prim.CreateAttribute(attr_name, attr_type)
    return attr


class UsdOp:
    """
    One mirror write. The default operation sets `value` on the symbol's
    prim. A symbol ending in `usd:attr:<name>` sets attribute `<name>` on
    the parent prim instead, and `usd:type:<Type>` defines the parent prim
    with that type, so a PLC can drive a transform or a prim type directly.
    """

    def __init__(self, operation, path, key, value):
        self.operation = operation
        self.path = path
        self.key = key
        self.value = value

    def create(self, stage, value):
        if self.operation == "type":
            create_typed_prim(stage, self.path, value)
        else:
            create_symbol_prim_value(stage, self.path, self.value, self.key, value)

    def execute(self, stage, value):
        if self.operation == "type":
            return bool(stage.GetPrimAtPath(self.path))
        return set_symbol_prim_value(stage, self.path, self.value, value)


def get_op_from_key(full_path: str, key: str) -> UsdOp:
    last = full_path.split("/")[-1]
    command = last.split(":")
    if len(command) > 1 and command[0] == "usd":
        key_path = "/".join(full_path.split("/")[:-1])
        return UsdOp(command[1], key_path, key, ":".join(command[2:]))
    return UsdOp("set", full_path, key, ATTR_CURRENT_VALUE)
