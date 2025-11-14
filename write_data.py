from dotenv import load_dotenv
load_dotenv()

import os
from neo4j import GraphDatabase
import json
import logging
from pydantic import BaseModel
import pdb

from classes.building import Building
from classes.civ import Civ
from classes.research import Research
from classes.unit_category import UnitCategory
from classes.unit_line import UnitLine
from classes.unit import Unit

uri = "bolt://localhost:7687"
username = os.getenv("NEO4j_INSTANCE_USERNAME")
password = os.getenv("NEO4J_INSTANCE_PASSWORD")
data_path = "./data/"

DATABASE = "neo4j"

driver = GraphDatabase.driver(uri, auth=(username, password))
driver.verify_connectivity()

logger = logging.Logger("logger")

def get_write_statement_from_node(node_type: str, node: BaseModel) -> str:
    properties = node.model_dump()
    props_list = [f"{key}: ${key}" for key in properties.keys()]
    props_string = ", ".join(props_list)
    
    query = f"CREATE (n:{node_type} {{{props_string}}})"
    return query

if __name__ == "__main__":

    with open(data_path+"civilizations.json", "r") as infile:
        civilizations = json.load(infile)
    civilizations = civilizations["civilization_list"]

    with open(data_path+"civTechTrees.json", "r") as infile:
        civ_tech_trees = json.load(infile)
    civ_tech_trees = civ_tech_trees["civs"]

    with open(data_path+"unitcategories.json", "r") as infile:
        unit_categories = json.load(infile)

    with open(data_path+"unitlines.json", "r") as infile:
        unit_lines = json.load(infile)
    unit_lines = unit_lines["UnitLines"]
    
    # explicitly stating the type for clarity
    statements: list[tuple[str, dict[str, any]]] = []
    civ_names = driver.execute_query(
        "MATCH (a:Civ) "
        "RETURN a.name",
        result_transformer_=lambda x: x.value(),  # return only the text values, not the Record objects
        database_=DATABASE
    )
    building_node_ids = driver.execute_query(
        "MATCH (a:Building) "
        "RETURN a.building_id",
        result_transformer_=lambda x: x.value(),
        database_=DATABASE
    )
    unit_or_research_node_ids = driver.execute_query(
        "MATCH (n) "
        "WHERE n:Unit OR n:Research "
        "RETURN n.node_id",
        result_transformer_=lambda x: x.value(),
        database_=DATABASE
    )
    unit_category_names = driver.execute_query(
        "MATCH (n:UnitCategory) "
        "RETURN n.name",
        result_transformer_=lambda x: x.value(),
        database_=DATABASE
    )
    unit_line_ids = driver.execute_query(
        "MATCH (n:UnitLine) "
        "RETURN n.line_id",
        result_transformer_=lambda x: x.value(),
        database_=DATABASE
    )
    for civ in civ_tech_trees:
        # Each civ has building and unit arrays, unit array also contains research
        civ_name = civ["civ_id"]
        connections = driver.execute_query(
            f"MATCH (c:Civ {{name: \"{civ_name}\"}}) -[r]->(n) "
            "WHERE n:Unit OR n:Research OR n:Building "
            "RETURN n.node_id",
            result_transformer_=lambda x: x.value(),
            database_=DATABASE
        )
        if civ_name not in civ_names:
            civ_names.append(civ_name)
            civ_node = Civ(name=civ_name)
            statements.append((get_write_statement_from_node("Civ", civ_node), civ_node.model_dump()))
        for building in civ["civ_techs_buildings"]:
            building_node_id = building["Node ID"]
            if building_node_id not in building_node_ids:
                building_node_ids.append(building_node_id)
                building_node = Building(
                    age_id = building["Age ID"],
                    building_id = building["Building ID"],
                    name = building["Name"],
                    node_id = building_node_id
                )
                statements.append((get_write_statement_from_node("Building", building_node), building_node.model_dump()))
            if building["Node Status"] != "NotAvailable" and building_node_id not in connections:
                statements.append(
                                    ("MATCH (civ:Civ {name: $name}), (building:Building {node_id: $node_id}) " +
                                    "MERGE (civ)-[r:HAS_BUILDING]->(building)",
                                    {"name": civ_name, "node_id": building_node_id}))  # always pass 2ple w params
                connections.append(building_node_id)
        for unit_or_research in civ["civ_techs_units"]:
            unit_or_research_node_id = unit_or_research["Node ID"]
            node_type = unit_or_research["Node Type"]
            if unit_or_research_node_id not in unit_or_research_node_ids:
                unit_or_research_node_ids.append(unit_or_research_node_id)
                if node_type == "Unit" or node_type == "UnitUpgrade" or node_type == "UniqueUnit" or node_type == "RegionalUnit" or node_type == "BuildingNonTech" or node_type == "UniqueBuilding":
                    unit_node = Unit(
                        age_id = unit_or_research["Age ID"],
                        building_id = unit_or_research["Building ID"],
                        name = unit_or_research["Name"],
                        node_id = unit_or_research["Node ID"],
                        node_type = unit_or_research["Node Type"]
                    )
                    statements.append((get_write_statement_from_node("Unit", unit_node), unit_node.model_dump()))
                    statements.append(
                                            ("MATCH " +
                                            "(u:Unit {node_id: $node_id}), " +
                                            "(b:Building {building_id: $building_id}) " +
                                            "MERGE (u)-[r:CREATED_AT]->(b)",
                                            {"node_id": unit_or_research_node_id, "building_id": unit_node.building_id}
                                            )
                                        )
                    if unit_or_research["Node Status"] != "NotAvailable" and unit_or_research_node_id not in connections:
                        statements.append(
                                            ("MATCH " +
                                            "(civ:Civ {name: $name}), " +
                                            "(unit:Unit {node_id: $node_id}) " +
                                            "MERGE (civ)-[r:HAS_UNIT]->(unit)",
                                            {"name": civ_name, "node_id": unit_or_research_node_id}
                                            )
                                        )
                        connections.append(unit_or_research_node_id)
                elif node_type == "Research":
                    # TODO merge the code for research and units
                    research_node = Research(
                        age_id = unit_or_research["Age ID"],
                        building_id = unit_or_research["Building ID"],
                        name = unit_or_research["Name"],
                        node_id = unit_or_research["Node ID"]
                    )
                    statements.append((get_write_statement_from_node("Research", research_node), research_node.model_dump()))
                    statements.append(
                                            ("MATCH " +
                                            "(research:Research {node_id: $node_id}), " +
                                            "(b:Building {building_id: $building_id}) " +
                                            "MERGE (research)-[r:RESEARCHED_AT]->(b)",
                                            {"node_id": unit_or_research_node_id, "building_id": research_node.building_id}
                                            )
                                        )
                    if unit_or_research["Node Status"] != "NotAvailable" and unit_or_research_node_id not in connections:
                        statements.append(
                                        
                                            ("MATCH " +
                                            "(civ:Civ {name: $name}), " +
                                            "(research:Research {node_id: $node_id}) " +
                                            "MERGE (civ)-[r:HAS_RESEARCH]->(research)",
                                            {"name": civ_name, "node_id": unit_or_research_node_id}
                                            )
                                        )
                        connections.append(unit_or_research_node_id)
                else:
                    raise ValueError(f"Unhandled Node Type encountered: {node_type} in object: {unit_or_research}")

    
    for unit_category in unit_categories.keys():
        if unit_category not in unit_category_names:
            unit_category_names.append(unit_category)
            unit_category_node = UnitCategory(
                name = unit_category
            )
            statements.append((get_write_statement_from_node("UnitCategory", unit_category_node), unit_category_node.model_dump()))
        connections = driver.execute_query(
            f"MATCH (u:Unit) -[r]->(uc:UnitCategory {{name: \"{unit_category}\"}}) "
            "RETURN u.node_id",
            result_transformer_=lambda x: x.value(),
            database_=DATABASE
        )  # Will be empty list if category has not been created yet
        for unit in unit_categories[unit_category]:
            node_id = unit["ID"]
            if node_id not in connections:
                statements.append(
                                    ("MATCH " +
                                    "(u:Unit {node_id: $node_id}), " +
                                    "(uc:UnitCategory {name: $name})" +
                                    "MERGE (u)-[r:IS_UNIT_CATEGORY]->(uc)",
                                    {"name": unit_category, "node_id": node_id}
                                    )
                                )
                connections.append(node_id)

    # for unit_line in unit_lines:
    #     for unit_id in unit_lines[:-1]:
    #         statement = "MATCH (u1:Unit {})"

    with driver.session(database=DATABASE).begin_transaction() as tx:
        try:
            for pair in statements:
                tx.run(pair[0], pair[1])
            tx.commit()
        except ValueError as ve:
            pdb.set_trace()

    driver.close()
