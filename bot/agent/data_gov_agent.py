from __future__ import annotations as _annotations
import age

from typing import Union, List
from pydantic_ai import Agent, RunContext
from pydantic_ai.messages import ModelMessage
from pydantic_ai.usage import UsageLimits
from pydantic import BaseModel

try:
    import bot.models as models
    from . import (Deps, 
               CypherQuery, 
               GRAPH_NAME, 
               AgentFactory, 
               DataGovResponse, 
               Domain, 
               Entity, 
               Application)
finally:
    pass

from bot.settings import settings

import logfire
    
usage_limits = UsageLimits(request_limit=5) 

class DataGovAgentFactory(AgentFactory):
    @staticmethod
    def get_agent() -> Agent:
        try:
            from agent.age_cypher_agent import age_agent
        except:
            from bot.agent.age_cypher_agent import age_agent
        
        def _do_query(deps: Deps,
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
        
        async def metadata_query(ctx: RunContext[Deps],
                            query_description:str) -> DataGovResponse:
            """按问题对业务域、应用、数据实体相关的信息、关系进行查询

            Args:
                ctx: The context.
                query_description: 查询的问题
            """
            message_history: Union[list[ModelMessage], None] = None
            
            result = await age_agent.run(query_description, 
                                         deps=ctx.deps, 
                                         message_history=message_history,
                                         usage_limits=usage_limits)
            _c = result.data
            if isinstance(_c, CypherQuery):
                _content = _do_query(ctx.deps, _c)
                return _content
            else:
                message_history = result.all_messages(
                    result_tool_return_content='未能查询到结果。'
                )
                
        _mode_setting = settings.get_setting("agents")["data_gov_agent"]
        return Agent(
            models.infer_model(_mode_setting["model_name"], _mode_setting["api_key"]),
            deps_type=Deps,
            result_type=DataGovResponse,
            system_prompt=(
                '如果是业务域、应用、数据实体相关的问题，使用工具metadata_query获得查询结果。\n' +
                '约束：不重复执行查询，如果查询结果为空，则返回"未能查询到结果"。',
                '补充：metadata_query查询结果内容节点类型定义如下\n' +
                '- node=Application（应用程序/应用）\n' +
                '- node=Domain（业务域）\n' +
                '- node=Entity（数据实体）',
            ),
            tools=[metadata_query],
        )

data_gov_agent = DataGovAgentFactory.get_agent()