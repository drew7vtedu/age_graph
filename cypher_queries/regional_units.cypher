MATCH (c:Civ) -[hu:HAS_UNIT]-> (ru:Unit {node_type: "RegionalUnit"})
RETURN c, hu, ru