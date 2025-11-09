from pydantic import BaseModel


class Unit(BaseModel):
    age_id: int
    building_id: int
    name: str
    node_id: int
    