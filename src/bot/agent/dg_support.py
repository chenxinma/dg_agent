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
    from .metadata_tools import MetadataHelper
finally:
    pass
from bot.graph.age_graph import AGEGraph
from bot.settings import settings

SupportResponse: TypeAlias = Union[InvalidRequest, SQLResponse, DataGovResponse]

_EXAMPLES = """
## 参考以下示例生成Cypher查询语句。

需求：查询某个数据实体对应的物理表
查询：
MATCH (e:DataEntity {name: 'EntityName'})-[:IMPLEMENTS]->(t:PhysicalTable)
RETURN e, t
-- 替换 EntityName 为目标数据实体的名称。

需求：查找与某个数据实体对应的物理表名称
查询：
MATCH (e:DataEntity {name: 'EntityName'})-[:IMPLEMENTS]->(t:PhysicalTable)
RETURN t.full_table_name as full_table_name
-- 替换 EntityName 为目标数据实体的名称。

需求：查询两个关联实体及其物理表
查询：
MATCH (e1:DataEntity {name: 'EntityName'})-[r]->(e2:DataEntity),
    (e1)-[:IMPLEMENTS]->(t1:PhysicalTable),
    (e2)-[:IMPLEMENTS]->(t2:PhysicalTable)
RETURN e1, e2, r, t1, t2
-- 替换 EntityName 为目标数据实体的名称。

需求：查询某个应用关联的所有数据实体
查询：
MATCH (app:Application {name: 'ApplicationName'})-[r]-(e:DataEntity)
RETURN app, e
-- 替换 ApplicationName 为目标应用程序的名称。

需求：查询某个应用关联的所有数据实体及其物理表
查询：
MATCH (app:Application {name: 'ApplicationName'})-[r]-(e:DataEntity)-[:IMPLEMENTS]->(t:PhysicalTable)
RETURN app, e, t
-- 替换 ApplicationName 为目标应用程序的名称。

需求：查找业务域下的所有实体
查询：
MATCH (d:BusinessDomain {name: 'DomainName'})-[:CONTAINS]-(a:Application)-[r]-(e:DataEntity)
RETURN e
-- 替换 DomainName 为目标业务域的名称。

需求：列出前 n 个数据实体
查询：
MATCH (e:DataEntity)
RETURN e
LIMIT n
-- 替换 n 为目标数量（例如 10）

需求：统计某个业务域下所有应用程序的数量
查询：
MATCH (d:BusinessDomain {name: 'DomainName'})-[:CONTAINS]-(a:Application)
RETURN count(a) AS application_count
-- 替换 DomainName 为目标业务域的名称。

需求：查找两个数据实体之间的连接关系。
查询：
MATCH (e1:DataEntity {name: 'Entity1'})-[r:RELATED_TO*1..2]->(e2:DataEntity {name: 'Entity2'})
RETURN e1,r,e2
-- 替换 Entity1 和 Entity2 为目标数据实体的名称。

需求：查找某个数据实体的所有复制实体。
查询：
MATCH (e1:DataEntity {name: 'EntityName'})-[:FLOWS_TO]-(e2:DataEntity)
RETURN e2
-- 替换 EntityName 为目标数据实体的名称。
"""

class DataGovSupportAgentFactory(AgentFactory):
    """数据治理知识支持Agent"""
    @staticmethod
    def get_agent() -> Agent:
        _mode_setting = settings.get_setting("agents")["plan_agent"]

        def _wrap_cypher(cypher: str) -> str:
            c = cypher.replace("\\n", "\n")
            if c.endswith(";"):
                c = c[:-1]
            return c

        def cypher_query(ctx: RunContext[AGEGraph], query: CypherQuery) -> DataGovResponse:
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

                query.cypher = _wrap_cypher(query.cypher)
                try:
                    _graph.explain(query.cypher)
                except Exception as e:
                    logfire.warn('错误查询: {e}', e=e)
                    logfire.warn('Cypher {q}', q=query.cypher)
                    raise ModelRetry(f'错误查询: {e}') from e

            with logfire.span("Execute query"):
                metadata_helper : MetadataHelper = MetadataHelper(_graph)
                result:DataGovResponse = metadata_helper.query(query)

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

        def get_graph_schema(ctx: RunContext[AGEGraph]) -> str:
            return ctx.deps.schema + \
                  "\n" + _EXAMPLES


        agent = Agent(
            models.infer_model(_mode_setting["model_name"], _mode_setting["api_key"]),
            model_settings={'temperature': 0.0},
            deps_type=AGEGraph,
            result_type=str,
            system_prompt=(
                "你是一个数据治理知识支持助手。"
                "你可以根据下面给定的图数据架构生成Cyher查询语句。"
                "+ 你会被问及关于这个图数据中关于业务域、应用、数据实体、物理表和数据实体间的关联（RELATE_TO）相关问题，" +
                " 此时可以通过使用'cypher_query'工具执行Cypher获得结果直接反馈。",
                "+ 你会被问数据统计查询相关的问题，可以通过'cypher_query'工具获得物理表的定义(获得的物理表内包含表名、列信息，不需要额外获取)，然后根据物理表定义来编写SQL查询，" +
                " 此时可以通过'sql_validate'验证生成的SQL是否正确。",
                "注意：请不要使用'cypher_query'工具执行SQL查询。",
                "注意：对name属性的查询例如数据实体名、应用名、业务域名等，不要翻译。",
                "注意：工具使用后的结果应组织成合适的MarkDown文本格式回复。",
            )
        )

        agent.system_prompt(get_graph_schema)
        agent.tool(cypher_query, require_parameter_descriptions=True)
        agent.tool_plain(sql_validate, require_parameter_descriptions=True)

        return agent


dg_support_agent = DataGovSupportAgentFactory.get_agent()
