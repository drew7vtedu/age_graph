MATCH (u:Unit)
WITH u.name AS propertyValue, collect(u) AS units
WHERE size(units) >= 2
UNWIND units AS unit
RETURN unit