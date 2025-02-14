import age
from dataclasses import dataclass
from pydantic import BaseModel, Field
from pydantic_ai import Agent 

from typing import Annotated, Union, List
from annotated_types import MinLen
from typing_extensions import TypeAlias

from bot.settings import settings
from .metadata import AGEVertex


GRAPH_NAME = settings.get_setting("age")["graph"]
DSN = settings.get_setting("age")["dsn"]

@dataclass
class Deps:
    g_name: str
    url: str

    def create_ag(self):
        return age.connect(graph=GRAPH_NAME, dsn=DSN)


class CypherQuery(BaseModel):
    """cypher脚本"""

    sql: Annotated[str, MinLen(1)]
    explanation: str = Field(
        '', description='查询的解释，以 Markdown 格式呈现'
    )


class InvalidRequest(BaseModel):
    """当用户的输入未包含生成 SQL脚本 所需的足够信息。"""
    error_message: str

Response: TypeAlias = Union[CypherQuery, InvalidRequest]


class AgentFactory:
    def __init__(self):
        pass
    
    @staticmethod
    def get_agent() -> Agent:
        pass

class DataGovResponse(BaseModel):
    """数据治理相关信息"""
    contents: List[List[Union[str, AGEVertex]]] = Field(default=[], 
                                                        description="`cypher_query·给出的CypherQuery`的查询结果")
    description: str = Field(default="", description="查询结果描述")    
    
    def add(self, row: List[Union[str, AGEVertex]]):
        self.contents.append(row)