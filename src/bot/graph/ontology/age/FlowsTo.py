from typing import Annotated
from pydantic import BaseModel, Field

class FlowsTo(BaseModel):
    """流向/复制"""
    from_id: Annotated[str, Field(description="源数据实体的ID")]
    to_id: Annotated[str, Field(description="目标数据实体的ID")]
    id: Annotated[str, Field(description="ID")]

    @classmethod
    def parse(cls, cell):
        return cls(id=f"{cell['id']}", 
        from_id=f"{cell['from_id']}", 
        to_id=f"{cell['to_id']}")

    @classmethod
    def fit(cls, cell) -> bool:
        return cell['label'] == 'FLOWS_TO'