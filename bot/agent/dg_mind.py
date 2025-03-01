"""问答状态机
"""
from __future__ import annotations as _annotations

import json
from dataclasses import dataclass, field
from typing import List, Union
from pydantic_ai.messages import ModelMessage
from pydantic_graph import BaseNode, End, Graph, GraphRunContext

import sqlparse

try:
    from . import (Deps,
               CypherQuery,
               SQLResponse,
               DataGovResponse,
               PlanResponse,
               )
    from .metadata import PhysicalTable, DataEntity, RelatedTo
    from .age_cypher_agent import age_agent
    from .sql_agent import sql_agent
    from .plan_agent import plan_agent
    from .metadata_tools import MetadataHelper, PhysicalTableEncoder
finally:
    pass

from bot.settings import settings
from bot.graph.age_graph import AGEGraph

@dataclass
class State:
    """运行状态"""
    question: str
    deps: Deps
    tables: List[PhysicalTable] = field(default=None)
    rels: List[RelatedTo] = field(default=None)
    agent_messages: list[ModelMessage] = field(default_factory=list)
    current_step:int = 0
    plan: PlanResponse = field(default=None)


@dataclass
class PlanGen(BaseNode[State]):
    """执行计划生成
    """
    async def run(self, ctx: GraphRunContext[State]) -> StepRunner:
        result = await plan_agent.run(
            ctx.state.question,
            message_history=ctx.state.agent_messages,
        )
        return StepRunner(result.data) # 启动计划执行


@dataclass
class StepRunner(BaseNode[State, None, Union[PlanResponse, DataGovResponse, SQLResponse]]):
    """运行执行计划
    按生成的plan顺序执行
    如果执行完所有步骤，则返回 DataGovResponse 或 SQLResponse
    """
    response: Union[PlanResponse, DataGovResponse, SQLResponse]

    async def run(self, ctx: GraphRunContext[State]) -> MetaCypherGen | SqlGen | End:
        if isinstance(self.response, PlanResponse):
            ctx.state.plan = self.response
            ctx.state.current_step = 0
            ctx.state.tables = []
            ctx.state.rels = []

        if ctx.state.current_step < len(ctx.state.plan.steps):
            step = ctx.state.plan.steps[ctx.state.current_step]
            ctx.state.current_step += 1
            if "sql_agent" == step.tool:
                return SqlGen(prompt=step.prompt)
            elif "age_agent" == step.tool:
                return MetaCypherGen(prompt=step.prompt)
        else:
            if isinstance(self.response, SQLResponse):
                return End(self.response.sql) # SQL
            else:
                return End(self.response) # 最后一次的步骤结果


@dataclass
class SqlGen(BaseNode[State]):
    """SQL生成
    """
    prompt:str | None = None

    async def run(self, ctx: GraphRunContext[State]) -> StepRunner:
        prompt = "问题:{}\n参考物理表:{}\n物理表关联:{}".format(self.prompt,
                    "\n".join([json.dumps(t, cls=PhysicalTableEncoder) for t in ctx.state.tables]),
                    "\n".join([r.rel for r in ctx.state.rels]))
        result = await sql_agent.run(
            prompt,
            deps=ctx.state.deps,
        )
        ctx.state.agent_messages += result.all_messages()
        result.data.sql = sqlparse.format(result.data.sql, reindent=True, keyword_case='upper')
        return StepRunner(result.data)


@dataclass
class MetaCypherGen(BaseNode[State, Deps, str]):
    """MetaCypherGen 数据元模型查询语句生成
    """
    prompt: str | None = None

    async def run(self, ctx: GraphRunContext[State]) -> AgeCypherQuery:
        result = await age_agent.run(
            self.prompt,
            deps=ctx.state.deps,
            message_history=ctx.state.agent_messages,
        )
        ctx.state.agent_messages += result.all_messages()
        return AgeCypherQuery(result.data)


@dataclass
class AgeCypherQuery(BaseNode[State, None, CypherQuery]):
    """执行AGE的数据元模型查询
    """
    query: CypherQuery

    def collect_table_defines(self,
                              contents: List | DataEntity | PhysicalTable | RelatedTo,
                              state:State) -> None:
        """收集表定义
        """
        if isinstance(contents, list):
            for element in contents:
                self.collect_table_defines(element, state)
        else:
            if isinstance(contents, DataEntity):
                state.tables.extend(contents.tables)
            if isinstance(contents, PhysicalTable):
                state.tables.append(contents)
            if isinstance(contents, RelatedTo):
                state.rels.append(contents)

    async def run(
        self,
        ctx: GraphRunContext[State],
    ) -> MetaCypherGen | StepRunner:
        metadata_helper : MetadataHelper = MetadataHelper(ctx.state.deps.graph)
        result:DataGovResponse = metadata_helper.query(self.query)
        # 如果描述为空，则重新执行Cypher生成
        if result.description is None:
            return MetaCypherGen(prompt=self.query.explanation)

        self.collect_table_defines(result.contents, ctx.state)
        return StepRunner(result)

graph = Graph(nodes=(PlanGen, StepRunner, SqlGen, MetaCypherGen, AgeCypherQuery))

async def do_it(question: str):
    """执行任务"""
    age_graph = AGEGraph(graph_name=settings.get_setting("age")["graph"],
                        dsn=settings.get_setting("age")["dsn"])
    state = State(question=question, 
                  deps=Deps(graph=age_graph))
    result, _ = await graph.run(PlanGen(), state=state)
    return result

def to_marimo():
    """运行图"""
    print(graph.mermaid_code(start_node=PlanGen))
