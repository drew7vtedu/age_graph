from pydantic import BaseModel


class UnitLine(BaseModel):
    name: str
    building: bool
    line_id: int
    id_chain: list[int]
