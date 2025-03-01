"""Agent for DataGov
"""
from dataclasses import dataclass
from typing import Annotated, Union, List
from typing_extensions import TypeAlias
from annotated_types import MinLen

from pydantic import BaseModel, Field
from pydantic_ai import Agent

from bot.graph import age_graph



@dataclass
class Deps:
    """Agent执行依赖"""
    graph: age_graph.AGEGraph


class CypherQuery(BaseModel):
    """cypher脚本"""

    cypher: Annotated[str, MinLen(1)]
    explanation: str = Field(
        '', description='查询的解释，以 Markdown 格式呈现'
    )


class InvalidRequest(BaseModel):
    """当用户的输入未包含生成 Cypher脚本 所需的足够信息。"""
    error_message: str

Response: TypeAlias = Union[CypherQuery, InvalidRequest]


class AgentFactory:
    """AgentFactory"""
    def __init__(self):
        pass

    @staticmethod
    def get_agent() -> Agent:
        """make a agent"""



class DataGovResponse(BaseModel):
    """数据治理相关信息"""
    contents: List[List[Union[str, BaseModel]]] = \
        Field(default=[],
              description="`cypher_query·给出的CypherQuery`的查询结果")
    description: str = Field(default="", description="查询结果描述")


class PlanStep(BaseModel):
    """执行步骤"""
    tool: str = Field(description="需要调用的工具名")
    prompt: str = Field(description="执行步骤说明和必要参数")

class PlanResponse(BaseModel):
    """执行计划"""
    thoughts: List[str] = Field(description="思考步骤说明")
    steps: List[PlanStep] = Field(description="执行步骤清单")

class SQLResponse(BaseModel):
    """sql response"""
    sql: str
