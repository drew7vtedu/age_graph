from pydantic import BaseModel


class Building(BaseModel):
    age_id: int
    building_id: int
    name: str
    node_id: int
    