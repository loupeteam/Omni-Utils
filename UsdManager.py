import omni
import omni.usd
import numpy as np
from omni.usd import StageEventType
import ast
import logging
import re

from pxr import Sdf, Tf, Gf
from pxr import Usd, UsdGeom
from .BridgeManager import BridgeManager
from threading import RLock
from contextlib import contextmanager

logger = logging.getLogger(__name__)

ATTR_CURRENT_VALUE = "value"
ATTR_WRITE_VALUE = "write:value"
ATTR_WRITE_PAUSE = "write:pause"
ATTR_WRITE_ONCE = "write:once"
ATTR_WRITE_SYMBOL = "symbol"

# Opt out of mirroring read values onto prims. Set it on a component prim when
# something else owns the USD those values would land on, or when the symbols
# cannot be represented as scalar attributes. Default is on, so existing scenes
# that carry no such attribute behave exactly as before.
ATTR_MIRROR_USD = "bridge:MirrorToUsd"

# How many times a symbol may fail to be written before it is given up on. More than
# one because a failure is not always permanent: create_attr fixes an attribute's USD
# type from the first value it ever sees, so a single bad sample -- a partial read
# arriving as None -- would otherwise disable a symbol whose every later value is fine.
UNWRITABLE_RETRIES = 3

# A given-up symbol is tried again once every this many updates, so a symbol that
# failed for a while (a PLC that dropped out and came back) recovers by itself
# instead of staying dark until the stage is reopened. One attempt per ~2 s at
# 60 fps keeps the log quiet: the warning is logged once, when the symbol is given up.
UNWRITABLE_RETRY_EVERY = 120


class RuntimeUsd:
    """
    Runtime class for managing USD objects
    - Handles reading and writing data to the USD
    - Subscribes to a bridge manager for reading and writing data
    - Subscribes to USD events for writing data
    """

    def __init__(self, prim_path, manager: BridgeManager, mirror: bool = True):

        self._root_prim = None
        self._root_prim_path = prim_path
        self._bridge_manager = manager
        # Whether to copy read values onto prims. Off does NOT disable this object:
        # it also carries the other direction -- _notice_changed turns an edit of a
        # write:value attribute into a write to the bridge -- and that has nothing to
        # do with mirroring.
        self._mirror = mirror
        self._lock = RLock()
        self._data_update = dict()
        self._in_notice = False

        # Symbols this mirror could not represent in USD. Kept so a value that cannot
        # be written is attempted once rather than every frame -- see _on_update_event.
        self._unwritable = dict()
        self._update_count = 0

        # The USD context doesn't change, so we can get it once
        self._usd_context = omni.usd.get_context()

        # Get the stage from the context. This may change, but the stage may already be open
        self._stage = self._usd_context.get_stage()

        # Subscribe to events
        self._subscribe()

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

    def cleanup(self):
        """
        Cleanup the runtime
        """
        self._unsubscribe()

    def _subscribe(self):
        """
        Subscribe to events
        """
        # Subscribe to stage events
        # We need to know if a new stage is opened
        self._stage_event_sub = (
            self._usd_context.get_stage_event_stream().create_subscription_to_pop(
                self._on_stage_event
            )
        )

        # Subscribe to update events
        # We need to make changes to the USD in the main thread
        self._update_event_sub = (
            omni.kit.app.get_app()
            .get_update_event_stream()
            .create_subscription_to_pop(self._on_update_event)
        )

        # Subscribe to USD changes
        # We need to know if a write attribute is changed
        self._stage_listener = Tf.Notice.Register(
            Usd.Notice.ObjectsChanged,
            self._notice_changed,
            self._usd_context.get_stage(),
        )

        # Subscribe to data read events
        # We need to update the USD with the new data as it comes in.
        # Not subscribing when mirroring is off is deliberate: an unread _data_update
        # would otherwise grow for the life of the process.
        if self._mirror:
            self._bridge_manager.register_data_callback(self._on_data_read)

    def _unsubscribe(self):
        """
        Unsubscribe from events
        """
        # unsubscription
        self._stage_event_sub = None
        self._update_event_sub = None
        self._stage_listener = None

    def _on_data_read(self, event):
        """
        Handle data read events
        - Update the data to be written to the USD
        - Flatten the deep object into a single-level dictionary
        - update the flat dictionary with the new data
        - overwrite the old data if it exists
        """
        # Get the data from the event
        data = event.payload["data"]

        # Flatten the data
        data = flatten_obj(data)

        # Update the data in a threadsafe way
        with self._lock:
            self._data_update.update(data)

    def _mark_unwritable(self, key, value, error):
        """
        Count a symbol that could not be mirrored, and give up on it after a few tries.

        Without this, an unrepresentable value is retried on every app update forever:
        the exception escapes _on_update_event, the log fills at frame rate, and the
        application becomes unusable. Any array element symbol does it -- flatten_obj()
        recurses into dicts but treats a list as a leaf, so "GVL.Axes[0].Position"
        flattens to the single key "GVL.Axes" holding a list, and writing a list to a
        scalar attribute raises.

        A budget rather than one strike, because create_attr fixes an attribute's USD
        type from the first value it ever sees: one bad sample -- a partial read
        arriving as None -- would otherwise disable a symbol whose every later value is
        fine. The count is cleared when a new stage is opened.

        Giving up keeps the bridge itself working: the value still reaches data
        subscribers through the read event, only its USD mirror is dropped.
        """
        self._unwritable[key] = self._unwritable.get(key, 0) + 1
        if self._unwritable[key] != UNWRITABLE_RETRIES:
            # below the budget: keep trying quietly; above it: this was a periodic
            # retry that failed again, already warned
            return
        logger.warning(
            "USD mirror: cannot represent '%s' (%s), so it will not be mirrored. "
            "The value is still delivered to data subscribers. Reason: %s",
            key,
            type(value).__name__,
            error,
        )

    def _notice_changed(self, notice, stage):
        """
        Handle changes in the stage
        - If a write attribute is changed, write the value to the bridge
        """
        if self._stage.expired:
            return

        # Resetting write:once below edits the stage, which re-enters this handler
        # synchronously; without the guard the nested call sent the value a second
        # time.
        if self._in_notice:
            return
        self._in_notice = True
        try:
            self._handle_write_changes(notice)
        finally:
            self._in_notice = False

    def _handle_write_changes(self, notice):
        # Sdf.Path prefix, not a string prefix: "/PLC/PLC1" is a string prefix of
        # "/PLC/PLC10/...", which would send PLC10's writes to PLC1 as well.
        root = Sdf.Path(self._root_prim_path)
        for changed in list(notice.GetChangedInfoOnlyPaths()):
            if changed.HasPrefix(root):
                if (
                    changed.name == ATTR_WRITE_VALUE
                    or changed.name == ATTR_WRITE_ONCE
                    or changed.name == ATTR_WRITE_PAUSE
                ):
                    # Get the value of the write attribute
                    changed = str(changed).split(".")[0]
                    prim = self._stage.GetPrimAtPath(str(changed))

                    # If the symbol hasn't been added yet,
                    # we are creating the prim, don't write the value
                    write_symbol = prim.GetAttribute(ATTR_WRITE_SYMBOL)
                    if not write_symbol.IsValid():
                        continue

                    # Get the write attributes to find out if we should write the value
                    write_once_attr = prim.GetAttribute(ATTR_WRITE_ONCE)
                    write_pause_attr = prim.GetAttribute(ATTR_WRITE_PAUSE)

                    # We should write the value if the write_once attribute is set
                    # or the write_pause attribute is not set
                    # This allows the user to make changes to the write value without it writing
                    # intermediate values to the bridge
                    if write_once_attr.Get() or not write_pause_attr.Get():
                        # Reset the trigger in the layer the user set it in (the
                        # current edit target). The session layer is stronger than
                        # the root layer, so resetting it there would mask every
                        # later user edit of write:once. If an opinion survives in a
                        # stronger layer, clear that one too.
                        if write_once_attr.Get():
                            write_once_attr.Set(False)
                            if write_once_attr.Get():
                                with session_layer_context(self._stage):
                                    write_once_attr.Set(False)

                        # Get the value attribute
                        write_value_attr = prim.GetAttribute(ATTR_WRITE_VALUE)
                        self._bridge_manager.write_variable(
                            write_symbol.Get(), write_value_attr.Get()
                        )

    def _on_update_event(self, event):
        """
        Update the stage with the new data
        """
        # Ensure we are not trying to update the stage after it has been destroyed
        if self._stage.expired:
            return

        # If there is no data to update, return
        if len(self._data_update) == 0:
            return

        # Do a threadsafe data copy and clear the update data
        flat = None
        with self._lock:
            flat = self._data_update
            self._data_update = dict()

        # Keep track of the prims that need to be created
        create = dict()

        # Everything the mirror authors goes into the session layer. The values are
        # runtime state: they must not show up as unsaved changes, must not be saved
        # into the user's file, and must not linger in a stage opened without the PLC.
        self._update_count += 1
        retry_now = self._update_count % UNWRITABLE_RETRY_EVERY == 0

        with session_layer_context(self._stage):
            # Resolve (and if needed define) the root prim before the change block:
            # a prim defined inside an Sdf.ChangeBlock is not composed until the
            # block ends, so DefinePrim fails with "Failed to define UsdPrim".
            root_path = self.root_prim.GetPath().pathString
            # Make changes to existing prims in the change block
            with Sdf.ChangeBlock():
                for key, value in flat.items():
                    if self._unwritable.get(key, 0) >= UNWRITABLE_RETRIES and not retry_now:
                        continue
                    full_key = symbol_to_prim_path(root_path, key)
                    op = get_op_from_key(full_key, key)
                    try:
                        if not op.execute(self._stage, value):
                            create[full_key] = (op, value)
                        else:
                            # written: whatever failed before is over
                            self._unwritable.pop(key, None)
                    except Exception as e:
                        self._mark_unwritable(key, value, e)

            # Create new prims outside the change block
            for key, value in create.items():
                try:
                    value[0].create(self._stage, value[1])
                    self._unwritable.pop(value[0].key, None)
                except Exception as e:
                    self._mark_unwritable(value[0].key, value[1], e)

    def _on_stage_event(self, event):
        """
        Handle stage events
        - Opened: Get the stage and listen for changes
        """
        if event.type == int(StageEventType.OPENED):
            # A new stage means new prims and new attribute types, so a symbol that
            # could not be written into the old one deserves another go.
            self._unwritable.clear()
            self._stage = self._usd_context.get_stage()
            self._stage_listener = Tf.Notice.Register(
                Usd.Notice.ObjectsChanged, self._notice_changed, self._stage
            )


@contextmanager
def session_layer_context(stage):
    """
    Context manager that directs edits to the stage's session layer.
    The session layer is composed like any other, so the prims are visible to the
    property window, scripts and OmniGraph, but it is never saved and its edits do
    not count as pending changes.
    """
    edit_target = Usd.EditTarget(stage.GetSessionLayer())
    with Usd.EditContext(stage, edit_target):
        yield


_ARRAY_INDEX = re.compile(r"\[(\d+)\]")


def symbol_to_prim_path(root_path: str, symbol: str) -> str:
    """
    Map a PLC symbol to the path of its mirror prim under the component prim.

    Struct members become child prims. An array element becomes a child prim of
    the array named "_<index>", because a USD prim name cannot start with a digit
    or contain brackets:

        "GVL.Axes[0].ActualPosition" -> "<root>/GVL/Axes/_0/ActualPosition"
    """
    return root_path + "/" + "/".join(_ARRAY_INDEX.sub(r"/_\1", symbol).split("."))


def set_symbol_prim_value(stage, full_key, key, value) -> None:
    """
    Set the value attribute of a prim
    If the prim or attribute does not exist, return False to indicate that it should be created
    """

    # Get or create a prim for the variable
    prim = stage.GetPrimAtPath(full_key)

    # If the prim does not exist, create it
    if not prim:
        return False

    # Set the value of the prim
    attr = prim.GetAttribute(key)
    if not attr:
        return False
    set_attr(attr, value)
    return True


def create_typed_prim(stage, full_key, value) -> None:
    prim = stage.DefinePrim(full_key, value)


def create_symbol_prim_value(stage, full_key, attr, key, value) -> None:
    """
    Create a prim for the symbol and set the value attribute
    Create the attributes for writing the symbol
    """

    prim = stage.DefinePrim(full_key)

    # Set the value of the prim
    create_attr(prim, attr, value)

    # Declare the write attributes but do not author a value for them. The mirror
    # authors into the session layer, which is stronger than the root layer: a value
    # authored here would mask everything the user later types into write:value in
    # the property window (whose edits land in the root layer). With no opinion of
    # our own, the user's opinion is the composed value.
    declare_attr(prim, ATTR_WRITE_VALUE, value)
    declare_attr(prim, ATTR_WRITE_ONCE, False)
    declare_attr(prim, ATTR_WRITE_PAUSE, False)
    # Write the symbol last, so that we can detect that it has just been added
    if key:
        create_attr(prim, ATTR_WRITE_SYMBOL, key)


def declare_attr(prim: Usd.Prim, attr_name: str, like_value: any) -> Usd.Attribute:
    """
    Create an attribute with the USD type matching like_value, without authoring a
    value for it.
    """
    if type(like_value) is str:
        return prim.CreateAttribute(attr_name, Sdf.ValueTypeNames.String)
    if type(like_value) is bool:
        return prim.CreateAttribute(attr_name, Sdf.ValueTypeNames.Bool)
    return prim.CreateAttribute(attr_name, Sdf.ValueTypeNames.Double)


def set_attr(attr: Usd.Attribute, value: any) -> None:
    """
    Set the value of an attribute
    """
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

def set_or_create_attr(
    prim: Usd.Prim, attr_name: str, value: any
) -> tuple[Usd.Attribute, bool]:
    """
    Set an attribute on a prim, creating it if it does not exist.
    Return the attribute and a boolean indicating if it was created.
    """
    attr = prim.GetAttribute(attr_name)
    created = False
    if not attr:
        created = True
        attr = create_attr(prim, attr_name, value)
    else:
        attr.Set(value)

    return (attr, created)


def create_attr(
    prim: Usd.Prim, attr_name: str, value: any
) -> tuple[Usd.Attribute, bool]:
    """
    Set an attribute on a prim, creating it if it does not exist.
    """
    if attr_name == "xformOp:transform":
        xformable = UsdGeom.Xformable(prim)
        xformable.AddTransformOp()
    if attr_name == "xformOp:translate":
        xformable = UsdGeom.Xformable(prim)
        xformable.AddTranslateOp()
    if attr_name == "xformOp:rotateXYZ":
        xformable = UsdGeom.Xformable(prim)
        xformable.AddRotateXYZOp()
    if type(value) is str:
        attr = prim.CreateAttribute(attr_name, Sdf.ValueTypeNames.String)
    elif type(value) is bool:
        attr = prim.CreateAttribute(attr_name, Sdf.ValueTypeNames.Bool)
    else:
        attr = prim.CreateAttribute(attr_name, Sdf.ValueTypeNames.Double)

    set_attr(attr, value)
    return attr


def get_or_create_attr(
    prim: Usd.Prim, attr_name: str, attr_type: Sdf.ValueTypeNames
) -> Usd.Attribute:
    """
    Get an attribute on a prim, creating it if it does not exist.
    """
    attr = prim.GetAttribute(attr_name)
    if not attr:
        attr = prim.CreateAttribute(attr_name, attr_type)
    return attr


def flatten_obj(obj: dict[str, any]) -> dict[str, any]:
    """
    Flattens a nested object into a single-level dictionary keyed by PLC symbol.

    Structs (dicts) contribute ".member"; arrays (lists) contribute "[index]", so
    the keys are the symbol names the PLC knows and can be written straight back:

        {"GVL": {"Axes": [{"Pos": 1.0}, None, {"Pos": 3.0}]}}
        -> {"GVL.Axes[0].Pos": 1.0, "GVL.Axes[2].Pos": 3.0}

    None entries are the parser's placeholders for array indices that were not
    read, and are skipped.
    """

    def flatten(obj, key):
        if isinstance(obj, dict):
            for k in obj:
                flatten(obj[k], k if key == "" else key + "." + k)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                if item is not None:
                    flatten(item, "{}[{}]".format(key, i))
        else:
            flat_obj[key] = obj

    flat_obj = {}
    flatten(obj, "")
    return flat_obj


def get_options_from_prim(prim: Usd.Prim, defaults: dict) -> dict[str, any]:
    """
    Get the options stored in the prim
    """
    options = defaults.copy()
    for option in defaults:
        attr = prim.GetAttribute(option)
        if attr.IsValid():
            options[option] = attr.Get()
    return options


def set_options_on_prim(prim, options):
    """
    Store options on a prim
    """
    for key, value in options.items():
        attr = prim.GetAttribute(key)
        if not attr:
            if type(value) is bool:
                attr = prim.CreateAttribute(key, Sdf.ValueTypeNames.Bool)
            elif type(value) is int:
                attr = prim.CreateAttribute(key, Sdf.ValueTypeNames.Int)
            elif type(value) is str:
                attr = prim.CreateAttribute(key, Sdf.ValueTypeNames.String)
            elif type(value) is list:
                if type(value[0]) is str:
                    attr = prim.CreateAttribute(key, Sdf.ValueTypeNames.StringArray)
                elif type(value[0]) is float:
                    attr = prim.CreateAttribute(key, Sdf.ValueTypeNames.FloatArray)
        attr.Set(value)


class UsdOp:
    def __init__(self, operation, path, key, value):
        self.operation = operation
        self.path = path
        self.key = key
        self.value = value

    def create(self, stage, value):
        if self.operation == "attr":
            create_symbol_prim_value(stage, self.path, self.value, self.key, value)
        elif self.operation == "type":
            create_typed_prim(stage, self.path, value)
        else:
            create_symbol_prim_value(stage, self.path, self.value, self.key, value)

    def execute(self, stage, value):
        if self.operation == "attr":
            return set_symbol_prim_value(stage, self.path, self.value, value)
        elif self.operation == "type":
            # TODO: Check the type of the prim
            prim = stage.GetPrimAtPath(self.path)
            # If the prim does not exist, create it
            if not prim:
                return False
            else:
                return True
        else:
            return set_symbol_prim_value(stage, self.path, self.value, value)


def get_op_from_key(full_path: str, key: str):
    """
    Get a property from a key
    """
    last = full_path.split("/")[-1]
    command = last.split(":")
    if len(command) > 1:
        if command[0] == "usd":
            key_path = "/".join(full_path.split("/")[:-1])
            return UsdOp(command[1], key_path, key, ":".join(command[2:]))

    return UsdOp("set", full_path, key, ATTR_CURRENT_VALUE)
