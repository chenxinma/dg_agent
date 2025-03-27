"""数据治理 元模型
"""
from abc import abstractmethod
from dataclasses import dataclass
from typing import Annotated, List, Dict

from pydantic import BaseModel, Field
from bot.graph.base_graph import BaseGraph


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
    tables: Annotated[List[Dict], Field(default=[], description="物理表")]

class Column(MetaObject):
    """列"""
    dtype: Annotated[str, Field(description="数据类型")]

class BusinessTerm(MetaObject):
    """业务术语"""
    definition: Annotated[str, Field(description="定义")]
    owner: Annotated[str, Field(description="拥有者")]
    status: Annotated[str, Field(description="状态")]

class RelatedTo(BaseModel):
    """关联"""
    from_id: Annotated[int, Field(description="上联数据实体的ID")]
    to_id: Annotated[int, Field(description="下联数据实体的ID")]
    id: Annotated[int, Field(description="ID")]
    rel: Annotated[str, Field(description="具体的关联信息，包含关联字段和条件", default='')]

class FlowsTo(BaseModel):
    """流向/复制"""
    from_id: Annotated[int, Field(description="源数据实体的ID")]
    to_id: Annotated[int, Field(description="目标数据实体的ID")]
    id: Annotated[int, Field(description="ID")]


@dataclass(init=False)
class MetaFactory:
    """元数据工厂"""
    @abstractmethod
    def fit(self, cell)->bool:
        """判断是否可以转换"""

    @abstractmethod
    def convert(self, cell, graph:BaseGraph) -> MetaObject | BaseModel:
        """转换"""
