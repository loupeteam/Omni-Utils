import omni
import omni.usd
import numpy as np
from omni.usd import StageEventType
import ast

from pxr import Sdf, Tf, Gf
from pxr import Usd, UsdGeom
from .BridgeManager import BridgeManager
from threading import RLock
from contextlib import contextmanager

ATTR_CURRENT_VALUE = "value"
ATTR_WRITE_VALUE = "write:value"
ATTR_WRITE_PAUSE = "write:pause"
ATTR_WRITE_ONCE = "write:once"
ATTR_WRITE_SYMBOL = "symbol"


class RuntimeUsd:
    """
    Runtime class for managing USD objects
    - Handles reading and writing data to the USD
    - Subscribes to a bridge manager for reading and writing data
    - Subscribes to USD events for writing data
    """

    def __init__(self, prim_path, manager: BridgeManager):

        # The user can specify the layer to edit
        # Notes: This has been disabled, since it breaks omnigraphs. We may want to enable it later.
        self.edit_layer = 0

        self._root_prim = None
        self._root_prim_path = prim_path
        self._bridge_manager = manager
        self._lock = RLock()
        self._data_update = dict()

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
        # We need to update the USD with the new data as it comes in
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

    def _notice_changed(self, notice, stage):
        """
        Handle changes in the stage
        - If a write attribute is changed, write the value to the bridge
        """
        if self._stage.expired:
            return

        for changed in list(notice.GetChangedInfoOnlyPaths()):
            if str(changed).startswith(self._root_prim_path):
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
                        # Set the write attribute to False
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

        # Make changes to existing prims in the change block
        with Sdf.ChangeBlock():
            for key, value in flat.items():
                path = self.root_prim.GetPath()
                full_key = path.pathString + "/" + "/".join(key.split("."))
                op = get_op_from_key(full_key, key)
                if not op.execute(self._stage, value):
                    create[full_key] = (op, value)
                # if not set_symbol_prim_value(self._stage, full_key, key, value):
                #     # If the prim does not exist, create it, but it must be done outside the change block
                #     create[full_key] = (key, value)

        # Create new prims outside the change block
        for key, value in create.items():
            value[0].create(self._stage, value[1])
            # get_op_from_key(key).create(self._stage, value)

    def _on_stage_event(self, event):
        """
        Handle stage events
        - Opened: Get the stage and listen for changes
        """
        if event.type == int(StageEventType.OPENED):
            self._stage = self._usd_context.get_stage()
            self._stage_listener = Tf.Notice.Register(
                Usd.Notice.ObjectsChanged, self._notice_changed, self._stage
            )


@contextmanager
def layer_context(stage, layer_id: int | str = 0):
    """
    Context manager to set the current layer for the stage
    """
    layer = stage.GetLayerStack()[layer_id]
    if not layer:
        layer = stage.GetLayerStack()[0]

    edit_target = Usd.EditTarget(layer)
    with Usd.EditContext(stage, edit_target):
        yield


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

    # If the symbol has just been added, set the write attributes
    create_attr(prim, ATTR_WRITE_VALUE, value)
    create_attr(prim, ATTR_WRITE_ONCE, False)
    create_attr(prim, ATTR_WRITE_PAUSE, False)
    # Write the symbol last, so that we can detect that it has just been added
    if key:
        create_attr(prim, ATTR_WRITE_SYMBOL, key)


def set_attr(attr: Usd.Attribute, value: any) -> None:
    """
    Set the value of an attribute
    """
    attr_type = attr.GetTypeName()

    if attr_type.cppTypeName == "GfMatrix4d":
        value = Gf.Matrix4d(np.array(ast.literal_eval(value)))

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
        # attr = prim.CreateAttribute(attr_name, Sdf.ValueTypeNames.Matrix4d)
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
    Flattens a nested object into a single-level dictionary.
    """

    def flatten(obj, key=""):
        if isinstance(obj, dict):
            for k in obj:
                flatten(obj[k], key + k + ".")
        else:
            flat_obj[key[:-1]] = obj

    flat_obj = {}
    flatten(obj)
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
                attr = prim.CreateAttribute(key, Sdf.ValueTypeNames.StringArray)
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
