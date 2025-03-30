from abc import abstractmethod
from typing import Annotated
from typing_extensions import Self

from pydantic import BaseModel, Field

class MetaObject(BaseModel):
    """元对象"""
    id: Annotated[str, Field(description="ID")]
    name: Annotated[str, Field(description="名称")]
    node: Annotated[str, Field(description="节点类型")]

    def __eq__(self, other):
        if isinstance(other, MetaObject):
            return self.id == other.id
        return False

    def __hash__(self):
        return hash(self.id)

    @classmethod
    @abstractmethod
    def parse(cls, cell) -> Self:
        """解析元对象"""
        pass

    @classmethod
    @abstractmethod
    def fit(cls, cell) -> bool:
        """判断是否匹配元对象"""
        pass
