import age
from pydantic import BaseModel, Field
from dataclasses import dataclass
from typing import List, Dict


class AGEVertex(BaseModel):
    id: int
    name: str = Field(description="名称")
    node: str = Field(description="节点类型")
    
    def __eq__(self, other):
        if isinstance(other, AGEVertex):
            return self.id == other.id
        return False
    
class Application(AGEVertex):
    """应用程序"""
    pass

class Domain(AGEVertex):
    """业务域"""
    code: str = Field(description="业务域代码")
        
 
class Entity(AGEVertex):
    """数据实体"""
    pass

class Table(AGEVertex):
    """物理表"""
    db_schema: str = Field(description="schema")
    full_table_name: str = Field(description="完整表名")
    table_name: str = Field(description="表名")
    columns: List[Dict] = Field(default=None)

class Entity(AGEVertex):
    """数据实体"""
    tables: List[Table] = []
    
@dataclass
class Link:
    from_id:int
    to_id:int
    id:int
    
@dataclass(init=False)
class MetaFactory:
    def __init__(self, graph_name:str):
        self.graph_name = graph_name
    def fit(self, cell)->bool:
        pass

    def convert(self, cell, conn):
        pass

