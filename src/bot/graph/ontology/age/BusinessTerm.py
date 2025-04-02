from typing import Annotated
from pydantic import Field

from .. import MetaObject

class BusinessTerm(MetaObject):
    """业务术语"""
    definition: Annotated[str, Field(description="定义")]
    owner: Annotated[str, Field(description="拥有者")]
    status: Annotated[str, Field(description="状态")]

    @classmethod
    def parse(cls, cell):
        return BusinessTerm(id=f"{cell['id']}", 
                            name=cell["name"],
                            definition=cell["definition"],
                            owner=cell["owner"],
                            status=cell["status"],
                            node=cell['label']
                            )

    @classmethod
    def fit(cls, cell) -> bool:
        return cell['label'] == 'BusinessTerm'
