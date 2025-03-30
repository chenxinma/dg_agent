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
        rel = cell.get("rel", "")
        return cls(
            id=f"{cell['_id']['offset']}:{cell['_id']['table']}", 
            from_id=f"{cell['_src']['offset']}:{cell['_src']['table']}", 
            to_id=f"{cell['_dst']['offset']}:{cell['_dst']['table']}",
            rel=rel)

    @classmethod
    def fit(cls, cell) -> bool:
        return cell['_label'] == 'RELATED_TO'
