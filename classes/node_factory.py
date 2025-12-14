from .civ import Civ
from .building import Building
from .research import Research
from .unit_category import UnitCategory
from .unit import Unit

from pydantic import BaseModel
import pandas as pd


class NodeFactory:

    def __init__(self, 
                civ_names: list[str], 
                building_node_ids: list[int], 
                unit_building_connections: pd.DataFrame,
                research_building_connections: pd.DataFrame,
                unit_category_names: list[str],
                unit_line_ids: list[int]) -> None:
        self.civ_names = civ_names
        self.building_node_ids = building_node_ids
        self.unit_building_connections = unit_building_connections
        self.research_building_connections = research_building_connections
        self.unit_category_names = unit_category_names
        self.unit_line_ids = unit_line_ids

    def create_node(self, node: dict) -> Civ | Building | Research | UnitCategory | Unit:
        """
        Create a node type from the given node by parsing the "Node Type" field
        """
        node_type = node["Node Type"]
        if node_type == "Civ":
            result = Civ(
                name = node["civ_id"]
            )
        elif node_type in ["BuildingNonTech", "BuildingTech", "UniqueBuilding", "RegionalBuilding"]:
            result = Building(
                    age_id = node["Age ID"],
                    building_id = node["Building ID"],
                    name = node["Name"],
                    node_id = node["Node ID"]
                )
        elif node_type in ["Unit", "UnitUpgrade", "UniqueUnit", "RegionalUnit", "BuildingNonTech"]:
            result = Unit(
                age_id = node["Age ID"],
                building_id = node["Building ID"],
                name = node["Name"],
                node_id = node["Node ID"],
                node_type = node["Node Type"]
                )
        elif node_type == "Research":
                result = Research(
                    age_id = node["Age ID"],
                    building_id = node["Building ID"],
                    name = node["Name"],
                    node_id = node["Node ID"]
                )
        else:
            raise ValueError(f"Unhandled Node Type encountered: {node_type} in object: {node}")

        return result

    def get_write_statement_from_node(self, node_type: str, node: BaseModel) -> str:
        """
        Get a cypher statement to create a node of the given type
        """
        properties = node.model_dump()
        props_list = [f"{key}: ${key}" for key in properties.keys()]
        props_string = ", ".join(props_list)
        
        query = f"CREATE (n:{node_type} {{{props_string}}})"
        return query
