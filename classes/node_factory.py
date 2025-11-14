from .civ import Civ
from .building import Building
from .research import Research
from .unit_category import UnitCategory
from .unit import Unit


class NodeFactory:

    def __init__(self) -> None:
        pass

    def create_node(self, node: dict) -> Civ | Building | Research | UnitCategory | Unit:
        """
        Create a node type from the given node by parsing the "Node Type" field
        """
        node_type = node["Node Type"]
        if node_type == ""