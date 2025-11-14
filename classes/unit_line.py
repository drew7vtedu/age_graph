from pydantic import BaseModel


class UnitLine(BaseModel):
    name: str
    identifier: str
    building: bool
    line_id: int
    id_chain: list[int]
    