from classes.node_factory import NodeFactory
from dotenv import load_dotenv
load_dotenv()

import os
from neo4j import GraphDatabase
import json
import logging
from pydantic import BaseModel
import pdb
import pandas as pd

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


"""
EDGE CASES TO INVESTIGATE
horse collar has comes_from links to both farm and mill
harbor and dock connection
multiple serjeant, tarkan and elite tarkan nodes
types of duplicates:
    unit which can be created at multiple buildings
        different node ids
    Elite Kipchak exists as both a unique unit and a unit upgrade
        same node_type

"""

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
    unit_node_ids = driver.execute_query(
        "MATCH (u:Unit) "
        "RETURN u.node_id",
        result_transformer_=lambda x: x.value(),
        database_=DATABASE
    )
    research_node_ids = driver.execute_query(
        "MATCH (r:Research) "
        "RETURN r.node_id",
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
    factory = NodeFactory(
        civ_names,
        building_node_ids, 
        unit_node_ids, 
        research_node_ids, 
        unit_category_names, 
        unit_line_ids)
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
            civ_node = factory.create_node({"civ_id": civ_name, "Node Type": "Civ"})  # add node type so create_node can parse it
            statements.append((factory.get_write_statement_from_node("Civ", civ_node), civ_node.model_dump()))
        for building in civ["civ_techs_buildings"]:
            building_node_id = building["Node ID"]
            if building_node_id not in building_node_ids:
                building_node_ids.append(building_node_id)
                building_node = factory.create_node(building)
                statements.append((factory.get_write_statement_from_node("Building", building_node), building_node.model_dump()))
            if building["Node Status"] != "NotAvailable" and building_node_id not in connections:
                statements.append(
                                    ("MATCH (civ:Civ {name: $name}), (building:Building {node_id: $node_id}) " +
                                    "MERGE (civ)-[r:HAS_BUILDING]->(building)",
                                    {"name": civ_name, "node_id": building_node_id}))  # always pass 2ple w params
                connections.append(building_node_id)
        for unit_or_research in civ["civ_techs_units"]:
            unit_or_research_node_id = unit_or_research["Node ID"]
            node_type = unit_or_research["Node Type"]
            # Create node no matter what to check connections
            unit_or_research_node = factory.create_node(unit_or_research)
            if isinstance(unit_or_research_node, Unit):
                creation_node_type = "Unit"
                relation_type = "HAS_UNIT"
                node_is_created = False
                if unit_or_research_node_id in unit_node_ids:
                    node_is_created = True
                else:
                    unit_node_ids.append(unit_or_research_node_id)
            elif isinstance(unit_or_research_node, Research):
                creation_node_type = "Research"
                relation_type = "HAS_RESEARCH"
                node_is_created = False
                if unit_or_research_node_id in research_node_ids:
                    node_is_created = True
                else:
                    research_node_ids.append(unit_or_research_node_id)
            if not node_is_created:
                statements.append((factory.get_write_statement_from_node(creation_node_type, unit_or_research_node), unit_or_research_node.model_dump()))
                statements.append(
                                        (f"MATCH (n:{creation_node_type} " +
                                        "{node_id: $node_id}), " +
                                        "(b:Building {building_id: $building_id}) " +
                                        "MERGE (n)-[r:COMES_FROM]->(b)",
                                        {"node_id": unit_or_research_node_id, "building_id": unit_or_research_node.building_id}
                                        )
                                    )
            if unit_or_research["Node Status"] != "NotAvailable" and unit_or_research_node_id not in connections:
                # Create the connection if the civ has the node and it does not exist
                statements.append(
                                    ("MATCH " +
                                    "(civ:Civ {name: $name}), " +
                                    f"(n:{creation_node_type} " +
                                    "{node_id: $node_id}) " +
                                    f"MERGE (civ)-[r:{relation_type}]->(n)",
                                    {"name": civ_name, "node_type": creation_node_type, "node_id": unit_or_research_node_id}
                                    )
                                )
                connections.append(unit_or_research_node_id)
    
    for unit_category in unit_categories.keys():
        if unit_category not in unit_category_names:
            unit_category_names.append(unit_category)
            unit_category_node = UnitCategory(
                name = unit_category
            )
            statements.append((factory.get_write_statement_from_node("UnitCategory", unit_category_node), unit_category_node.model_dump()))
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

    for unit_line in unit_lines:
        unit_line_node = UnitLine(
            name=unit_line["Name"],
            building=unit_line["Building"] if "Building" in unit_line.keys() else False,
            line_id=unit_line["LineID"],
            id_chain=unit_line["IDChain"]
        )
        # TODO check for existing connections?
        if unit_line_node.line_id not in unit_line_ids:
            # Create unit line
            unit_line_ids.append(unit_line_node.line_id)
            statements.append((factory.get_write_statement_from_node("UnitLine", unit_line_node), unit_line_node.model_dump()))
        for idx, line_member in enumerate(unit_line_node.id_chain):
            # Create connections with unit line members
            statements.append(
                    ("MATCH " +
                    f"(unit_line:UnitLine "
                    "{line_id: $line_id}), " +
                    f"(unit:{'Building' if unit_line_node.building else 'Unit'} "
                    "{node_id: $unit_id})" +
                    "MERGE (unit_line)-[r:HAS_MEMBER]->(unit)",
                    {"line_id": unit_line_node.line_id, "unit_id": line_member}
                    )
            )
            if idx != len(unit_line_node.id_chain) - 1:
                # Create connections between line id members
                statements.append(
                    ("MATCH " +
                    f"(unit:{'Building' if unit_line_node.building else 'Unit'} "
                    "{node_id: $base_unit}), " +
                    f"(unitUpgrade:{'Building' if unit_line_node.building else 'Unit'} "
                    "{node_id: $upgrade_unit})" +
                    "MERGE (unit)-[r:HAS_UPGRADE]->(unitUpgrade)",
                    {"base_unit": line_member, "upgrade_unit": unit_line_node.id_chain[idx+1]}
                    )
            )

    with driver.session(database=DATABASE).begin_transaction() as tx:
        try:
            for pair in statements:
                tx.run(pair[0], pair[1])
            tx.commit()
        except ValueError as ve:
            pdb.set_trace()

    driver.close()
