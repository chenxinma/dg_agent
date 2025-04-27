from typing import Annotated
from pydantic import BaseModel, Field

class RelatedTo(BaseModel):
    """关联"""
    from_id: Annotated[str, Field(description="上联数据实体的ID")]
    to_id: Annotated[str, Field(description="下联数据实体的ID")]
    id: Annotated[str, Field(description="ID")]
    rel: Annotated[str, Field(description="具体的关联信息，包含关联字段和条件", default='')]

    @classmethod
    def parse(cls, cell):
        print("relate to:", cell)
        rel = cell.get("rel", "")
        return cls(
            id=f"{cell['id']}", 
            from_id=f"{cell['from_id']}", 
            to_id=f"{cell['to_id']}",
            rel=rel)

    @classmethod
    def fit(cls, cell) -> bool:
        return cell['label'] == 'RELATED_TO'
