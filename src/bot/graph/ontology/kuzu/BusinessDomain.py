from typing import Annotated
from pydantic import Field

from .. import MetaObject

class BusinessDomain(MetaObject):
    """业务域"""
    code: Annotated[str, Field(description="业务域代码")]

    @classmethod
    def parse(cls, cell):
        return cls(id=f"{cell['_id']['offset']}:{cell['_id']['table']}", 
                name=cell['name'], 
                code=cell['code'],
                node=cell['_label'])

    @classmethod
    def fit(cls, cell) -> bool:
        return cell['_label'] == 'BusinessDomain'
