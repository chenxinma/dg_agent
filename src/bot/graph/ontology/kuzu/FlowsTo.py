from typing import Annotated
from pydantic import BaseModel, Field

class FlowsTo(BaseModel):
    """流向/复制"""
    from_id: Annotated[str, Field(description="源数据实体的ID")]
    to_id: Annotated[str, Field(description="目标数据实体的ID")]
    id: Annotated[str, Field(description="ID")]

    @classmethod
    def parse(cls, cell):
        return cls(id=f"{cell['_id']['offset']}:{cell['_id']['table']}", 
        from_id=f"{cell['_src']['offset']}:{cell['_src']['table']}", 
        to_id=f"{cell['_dst']['offset']}:{cell['_dst']['table']}")

    @classmethod
    def fit(cls, cell) -> bool:
        return cell['_label'] == 'FLOWS_TO'