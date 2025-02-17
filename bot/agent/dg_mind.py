from __future__ import annotations as _annotations

import json
from dataclasses import dataclass, field
from typing import List, Union
from pydantic_ai.messages import ModelMessage
from pydantic_graph import BaseNode, End, Graph, GraphRunContext

try:
    from . import (Deps, 
               CypherQuery,
               SQLResponse, 
               DataGovResponse,
               PlanResponse,
               GRAPH_NAME, 
               DSN)
    from .metadata import *
    from .age_cypher_agent import age_agent
    from .sql_agent import sql_agent
    from .plan_agent import plan_agent
    from .metadata_tools import create_factory_chain, age_metadata_query, TableEncoder
finally:
    pass

@dataclass
class State:
    question: str
    deps: Deps
    tables: List[Table] = field(default=None)
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
        
        if ctx.state.current_step < len(ctx.state.plan.steps):
            step = ctx.state.plan.steps[ctx.state.current_step]
            ctx.state.current_step += 1
            if "sql_agent" == step.tool:
                return SqlGen(prompt=step.prompt)
            elif "age_agent" == step.tool:
                return MetaCypherGen(prompt=step.prompt)
        else:
            return End(self.response) # 最后一次的步骤结果


@dataclass
class SqlGen(BaseNode[State]):
    """SQL生成
    """
    prompt:str | None = None
    
    async def run(self, ctx: GraphRunContext[State]) -> StepRunner:
        prompt = "问题：{}\n参考物理表:{}".format(self.prompt, json.dumps(ctx.state.tables, cls=TableEncoder))
        result = await sql_agent.run(
            prompt,
            deps=ctx.state.deps,
        )
        ctx.state.agent_messages += result.all_messages()
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
    meta_factories = create_factory_chain(GRAPH_NAME)
    
    def collect_table_defines(self, 
                              contents: List | Entity | Table, 
                              state:State) -> None:
        if isinstance(contents, list):
            for element in contents:
                self.collect_table_defines(element, state)
        else:
            if isinstance(contents, Entity):
                state.tables.extend(contents.tables)
            if isinstance(contents, Table):
                state.tables.append(contents)

    async def run(
        self,
        ctx: GraphRunContext[State],
    ) -> MetaCypherGen | StepRunner:
        result:DataGovResponse = age_metadata_query(ctx.state.deps.create_ag(), 
                                    GRAPH_NAME, 
                                    AgeCypherQuery.meta_factories, 
                                    self.query)
        # 如果描述为空，则重新执行Cypher生成
        if result.description is None:
            return MetaCypherGen(prompt=self.query.explanation)
        
        self.collect_table_defines(result.contents, ctx.state)
        return StepRunner(result)


async def do_it(question: str):
    state = State(question=question, deps=Deps(g_name=GRAPH_NAME, url=DSN))
    graph = Graph(nodes=(PlanGen, StepRunner, SqlGen, MetaCypherGen, AgeCypherQuery))
    result, _ = await graph.run(PlanGen(), state=state)
    return result