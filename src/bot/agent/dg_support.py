"""
数据治理知识支持Agent
功能：
1. 提供数据治理知识支持，根据数据治理元模型的信息查询知识库给出结果
2. 提供SQL生成，查询知识库获得的物理表和字段信息，生成SQL查询语句
"""
from __future__ import annotations as _annotations

from typing import Union
from typing_extensions import TypeAlias
from pydantic_ai import Agent, RunContext, ModelRetry
import logfire
import sqlparse

try:
    import bot.models as models
    from . import SQLResponse, DataGovResponse, AgentFactory, CypherQuery, InvalidRequest
finally:
    pass
# from bot.graph.age_graph import AGEGraph
from bot.graph import KuzuGraph
from bot.graph.ontology.kuzu import MetadataHelper, EXAMPLES as _EXAMPLES
from bot.settings import settings

SupportResponse: TypeAlias = Union[InvalidRequest, SQLResponse, DataGovResponse]

metadata_helper = MetadataHelper()

class DataGovSupportAgentFactory(AgentFactory):
    """数据治理知识支持Agent"""
    @staticmethod
    def get_agent() -> Agent[KuzuGraph, str]:
        """获取数据治理知识支持Agent"""

        def _wrap_cypher(cypher: str) -> str:
            c = cypher.replace("\\n", "\n")
            if c.endswith(";"):
                c = c[:-1]
            return c

        def cypher_query(ctx: RunContext[KuzuGraph], query: CypherQuery) -> DataGovResponse:
            """Graph query executor
            
            Args:
                ctx: The agent context.
                query: cypher and explanation from agent to execute
            """
            _graph = ctx.deps
            # vaildate cypher
            with logfire.span("Validate query"):
                if not query.cypher.upper().startswith('MATCH'):
                    raise ModelRetry('请编写一个MATCH的查询。') 

            with logfire.span("Execute query"):
                try:
                    _wraped_cypher = _wrap_cypher(query.cypher)
                    contents = metadata_helper.query(_wraped_cypher, _graph)
                except Exception as e:
                    logfire.warn('错误查询: {e}', e=e)
                    logfire.warn('Cypher {q}', q=query.cypher)
                    raise ModelRetry(f'错误查询: {e}') from e
                if not contents:
                    raise ModelRetry('未找到相关结果，请重新查询。')
                result : DataGovResponse = {
                    "contents":contents,
                    "description": query.explanation
                }
            return result

        def sql_validate(sql: str) -> SQLResponse:
            """SQL query executor
            
            Args:
                sql: 根据物理表和关联生成的SQL
            """
            # vaildate sql
            with logfire.span("Validate SQL"):
                _wrap_sql = sqlparse.format(sql, reindent=True, keyword_case='upper')

            return SQLResponse(sql=_wrap_sql)

        def get_graph_schema(ctx: RunContext[KuzuGraph]) -> str:
            return ctx.deps.schema + \
                  "\n" + _EXAMPLES

        model_name = settings.get_setting("agents.plan_agent.model_name")
        api_key = settings.get_setting("agents.plan_agent.api_key")
        agent = Agent(
            models.infer_model(model_name, api_key=api_key), # pyright: ignore[reportArgumentType]
            model_settings={'temperature': 0.0},
            deps_type=KuzuGraph,
            result_type=str,
            system_prompt=(
                "你是一个数据治理知识支持助手。"
                "你可以根据下面给定的图数据架构生成Cyher查询语句。"
                "+ 你会被问及关于这个图数据中关于业务域、应用、数据实体、物理表和数据实体间的关联（RELATE_TO）相关问题，" +
                " 此时可以通过使用'cypher_query'工具执行Cypher获得结果直接反馈。",
                "+ 你会被问数据统计查询相关的问题，可以通过'cypher_query'工具获得物理表的定义(获得的物理表内包含表名、列信息，不需要额外获取)，然后根据物理表定义来编写SQL查询，" +
                " 此时可以通过'sql_validate'验证生成的SQL是否正确。",
                "+ 你会被问及一些业务定义和术语说明，此时可以通过'cypher_query'工具执行Cypher查询'业务术语(BusinessTerm)'获得结果。",
                "注意：请不要使用'cypher_query'工具执行SQL查询。",
                "注意：对name属性的查询例如数据实体名、应用名、业务域名等，不要翻译。",
                "注意：工具使用后的结果应组织成合适的MarkDown文本格式回复。",
            )
        )

        agent.system_prompt(get_graph_schema)
        agent.tool(cypher_query, require_parameter_descriptions=True) # pyright: ignore[reportCallIssue]
        agent.tool_plain(sql_validate, require_parameter_descriptions=True) # pyright: ignore[reportCallIssue]

        return agent


dg_support_agent = DataGovSupportAgentFactory.get_agent()
