from dotenv import load_dotenv
load_dotenv()

import os
from neo4j import GraphDatabase
import json
import logging

uri = "bolt://localhost:7687"
username = os.getenv("NEO4j_INSTANCE_USERNAME")
password = os.getenv("NEO4J_INSTANCE_PASSWORD")
data_path = "./data/"

driver = GraphDatabase.driver(uri, auth=(username, password))
driver.verify_connectivity() # Optional: Verify the connection

logger = logging.Logger()

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
    
    statements = []
    

    # for civ in civilizations:
    #     logger.info(msg=f"Reading Civ {civ} from document")
    #     create_cypher = f"CREATE ()"



    with driver.session.begin_transaction() as tx:
        for statement in statements:
            tx.run(statement)
        tx.commit()

    # summary = driver.execute_query("""
    #     CREATE (a:Person {name: $name})
    #     CREATE (b:Person {name: $friendName})
    #     CREATE (a)-[:KNOWS]->(b)
    #     """,
    #     name="Alice", friendName="David",
    #     database_="age-data",
    # ).summary
    # print("Created {nodes_created} nodes in {time} ms.".format(
    #     nodes_created=summary.counters.nodes_created,
    #     time=summary.result_available_after
    # ))

    driver.close()