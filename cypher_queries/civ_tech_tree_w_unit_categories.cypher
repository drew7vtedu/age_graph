MATCH (c:Civ {name: "BRITONS"}) -[hb:HAS_BUILDING]-> (b:Building) <-[cf:COMES_FROM]- (n) <-[has]- (c)
WHERE has:HAS_UNIT OR has:HAS_RESEARCH
OPTIONAL MATCH (uc:UnitCategory) <-[iuc:IS_UNIT_CATEGORY]- (n)
OPTIONAL MATCH (n) -[hu:HAS_UPGRADE]-> (n)
RETURN c, hb, b, cf, n, uc, iuc, hu