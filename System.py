import omni
from .RuntimeBase import Runtime_Base
from .BridgeManager import BridgeManager
from .UsdManager import RuntimeUsd, get_options_from_prim, set_options_on_prim


class Component:
    """
    This is a holder for the Runtime and USD Component of a single component
    """

    def __init__(self, runtime, usd):
        """
        Initializes the System class with runtime and USD parameters.

        Args:
            runtime: The runtime environment for the system.
            usd: The USD (Universal Scene Description) file or object.
        """
        self.runtime = runtime
        self.usd = usd


class System:
    """
    System class for managing multiple component objects
    """

    def __init__(
        self,
        system_root: str,
        DefaultAttributes: dict,
        runtime_class: Runtime_Base,
        manager_class: BridgeManager,
    ):
        self.init(system_root, DefaultAttributes, runtime_class, manager_class)

    def init(
        self, system_root: str, DefaultAttributes: dict, runtime_class, manager_class
    ):
        """ """
        self.default_properties = DefaultAttributes
        self._system_root = system_root
        self._components:dict[str, Component] = dict()
        self._runtime_class = runtime_class
        self._manager_class = manager_class

    system_root = property(lambda self: self._system_root)
    def get_normalize_prim_name(self, name: str) -> str:
        if name.startswith("/"):
            return name
        return self.system_root + name
    
    def cleanup(self):
        """
        Remove all the runtime and USD objects from the system
        """
        for component in self._components.values():
            component.runtime.cleanup()
            component.usd.cleanup()

        self._components.clear()

    def find_components(self) -> dict[str, dict[str, any]]:
        """
        Find all the prims in the stage that have the Bridge parameters
        Return a dictionary of the prim names and their options
        """
        stage = omni.usd.get_context().get_stage()
        if stage is None:
            return
        components_prims = []
        for prim in stage.Traverse():
            for option in self.default_properties:
                if not prim.HasAttribute(option):
                    continue
                components_prims.append(prim)
                break
        # For all the prims found, get the prim name and add it to the list
        names = dict()
        for component in components_prims:
            if component.GetPath().pathString.startswith(self.system_root):
                name = component.GetPath().pathString.split("/")[-1]
            else:
                name = component.GetPath().pathString
            names[name] = get_options_from_prim(component, self.default_properties)
        return names

    def find_and_create_components(self) -> list[str]:
        """
        Find the components defined in the stage and create runtime objects for them
        """
        # Find all the components in the stage
        components = self.find_components()
        if components is None:
            return

        # Add new components to the stage
        for name, options in components.items():
            if name not in self._components:
                self.add_component(name, options)

        # Remove components that are not in the stage
        for name in list(self._components.keys()):
            if name not in components:
                self._components[name].runtime.cleanup()
                self._components[name].usd.cleanup()
                del self._components[name]

        # Return the names of the components
        return self.get_component_names()

    def create_component_prim(self, name: str, options: dict) -> str:
        """
        Create a new component in the stage with the given name and options
        """
        prim_name = self.get_normalize_prim_name(name)

        component_prim = omni.usd.get_context().get_stage().DefinePrim(prim_name)
        set_options_on_prim(component_prim, options)
        return prim_name

    def get_component(self, name: str | int) -> Runtime_Base | None:
        """
        Get the runtime object for the given component name
        """
        if name not in self._components:
            return None
        return self._components[name].runtime

    def write_options_to_stage(self, component_name: str | int):
        """
        Write the options for the given component from the runtime object to the stage
        """
        component = self.get_component(component_name)
        if component is None:
            return

        component_prim = (
            omni.usd.get_context()
            .get_stage()
            .GetPrimAtPath(self.get_normalize_prim_name(component_name))
        )
        if component_prim is None:
            return

        set_options_on_prim(component_prim, component.options)

    def read_options_from_stage(self, component_name: str | int):
        """
        Read the options for the given component from the stage to the runtime object
        """
        component = self.get_component(component_name)
        if component is None:
            return

        component_prim = (
            omni.usd.get_context()
            .get_stage()
            .GetPrimAtPath(self.get_normalize_prim_name(component_name))
        )
        if component_prim is None:
            return

        component.options = get_options_from_prim(
            component_prim, self.default_properties
        )

    # Return the names of the components as a list
    def get_component_names(self) -> list[str]:
        """
        Get the names of all the components in the system
        """
        component = self.find_components()
        if component is None:
            return []
        return list(self._components.keys())

    def add_component(self, name, options):
        """
        Add a new component to the system with the given name and options
        Create the PRIM in the stage
        Create the runtime and USD objects for the component
        """
        if name not in self._components:
            input_options = self.default_properties.copy()
            input_options.update(options)
            prim_name = self.create_component_prim(name, input_options)
            self._components[name] = Component(
                self._runtime_class(name, input_options),
                RuntimeUsd(prim_name, self._manager_class(name)),
            )
