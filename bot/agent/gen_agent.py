from __future__ import annotations as _annotations

from dataclasses import dataclass, field
from typing import Union, List
import age

from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.format_as_xml import format_as_xml
from pydantic_ai.messages import ModelMessage
from pydantic_graph import BaseNode, End, Graph, GraphRunContext
from bot.settings import settings

try:
    import bot.models as models
    from . import (Deps, 
               CypherQuery, 
               GRAPH_NAME, 
               DSN,
               AgentFactory, 
               DataGovResponse, 
               Domain, 
               Entity, 
               Application)
finally:
    pass

try:
    from agent.age_cypher_agent import age_agent
except:
    from bot.agent.age_cypher_agent import age_agent

import logfire

@dataclass
class State:
    question: str
    deps: Deps
    agent_messages: list[ModelMessage] = field(default_factory=list)
    
@dataclass
class QueryGen(BaseNode[State]):
    feedback: str | None = None
    
    async def run(self, ctx: GraphRunContext[State]) -> Feedback:
        result = await age_agent.run(
            ctx.state.question,
            deps=ctx.state.deps,
            message_history=ctx.state.agent_messages,
        )
        ctx.state.agent_messages += result.all_messages()
        return Feedback(result.data)
    
@dataclass
class Feedback(BaseNode[State, None, CypherQuery]):
    query: CypherQuery
    
    def _do_query(self, 
                  deps: Deps,
                  query: CypherQuery) -> DataGovResponse:
        """按照cypher脚本进行查询

        Args:
            deps: 依赖配置，AGE连接串.
            query: ·cypher_query·给出的CypherQuery
        """
        
        _conn = deps.create_ag().connection
        with logfire.span("Age Query"):
            with _conn.cursor() as _cursor:
                q = query.sql.replace("{GRAPH_NAME}", GRAPH_NAME)
                logfire.info("query: %s " % (query.explanation))
                _cursor.execute(q)            
                result = _cursor.fetchall()
                resp = DataGovResponse(description=query.explanation)
                for _r in result:
                    row: List[Union[str, BaseModel]] = []
                    for c in _r:
                        if isinstance(c, age.models.Vertex):
                            if c.label == "Domain":
                                d = Domain(id=c.properties["nid"], 
                                            name=c.properties["name"], 
                                            node=c.label,
                                            code=c.properties["code"])
                            else:
                                d = eval("{}(id='{}', name='{}', node='{}')".format(c.label, 
                                                            c.properties["nid"], 
                                                            c.properties["name"],
                                                            c.label))
                            row.append(d)
                        else:
                            row.append(str(c))
                    resp.add(row)
                logfire.info("result rows: %d " % (len(resp.contents)))
                return resp

    async def run(
        self,
        ctx: GraphRunContext[State],
    ) -> QueryGen | End:
        result = self._do_query(ctx.state.deps, self.query)
        if result.description is None:
            return QueryGen(feedback=result.description)
        return End(result)

async def do_it(question: str):
    state = State(question=question, deps=Deps(g_name=GRAPH_NAME, url=DSN))
    graph = Graph(nodes=(QueryGen, Feedback))
    result, _ = await graph.run(QueryGen(), state=state)
    return result