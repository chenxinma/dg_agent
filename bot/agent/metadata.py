"""数据治理 元模型
"""
from abc import abstractmethod
from dataclasses import dataclass
from typing import Annotated, List, Dict

from pydantic import BaseModel, Field
from bot.graph.age_graph import AGEGraph


class MetaObject(BaseModel):
    """元对象"""
    id: Annotated[int, Field(description="ID")]
    name: Annotated[str, Field(description="名称")]
    node: Annotated[str, Field(description="节点类型")]

    def __eq__(self, other):
        if isinstance(other, MetaObject):
            return self.id == other.id
        return False

class Application(MetaObject):
    """应用程序"""


class BusinessDomain(MetaObject):
    """业务域"""
    code: Annotated[str, Field(description="业务域代码")]

class PhysicalTable(MetaObject):
    """物理表"""
    db_schema: Annotated[str, Field(description="schema")]
    full_table_name: Annotated[str, Field(description="完整表名")]
    table_name: Annotated[str, Field(description="表名")]
    columns: Annotated[List[Dict], Field(default=None)]

class DataEntity(MetaObject):
    """数据实体"""
    tables: Annotated[List[PhysicalTable], Field(default=[], description="物理表")]

class Column(MetaObject):
    """列"""
    dtype: Annotated[str, Field(description="数据类型")]

class RelatedTo(BaseModel):
    """关联"""
    from_id: Annotated[int, Field(description="上联数据实体的ID")]
    to_id: Annotated[int, Field(description="下联数据实体的ID")]
    id: Annotated[int, Field(description="ID")]
    rel: Annotated[str, Field(description="具体的关联信息，包含关联字段和条件", default='')]

@dataclass(init=False)
class MetaFactory:
    """元数据工厂"""
    @abstractmethod
    def fit(self, cell)->bool:
        """判断是否可以转换"""

    @abstractmethod
    def convert(self, cell, graph:AGEGraph) -> MetaObject | BaseModel:
        """转换"""
