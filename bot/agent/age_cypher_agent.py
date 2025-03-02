"""
数据治理元模型查询Cypher生成Agent的构造工厂
"""
from __future__ import annotations as _annotations

from pydantic_ai import Agent, ModelRetry, RunContext

import logfire

try:
    import bot.models as models
    from . import Response, InvalidRequest, AgentFactory
finally:
    pass

from bot.graph.age_graph import AGEGraph
from bot.settings import settings


class AgeAgentFactory(AgentFactory):
    """AgeAgentFactory"""
    S_PROMPT = """
任务描述：
你是一个数据治理方面专家了解你所管理的数据治理图谱模型，写出Cypher查询图数据并在explanation给出相应的说明。
以下是一个图数据库的结构描述，请根据用户的需求生成相应的Cypher查询语句。
注意:变量名避免使用SQL关键字。
DataEntity (数据实体) 的 name 内容都是中文，不做英文翻译。
获取DataEntity (数据实体)的同时获取 RELATED_TO（关联）信息。
数据实体之间的关系可能是2-3层的间接关联。
"""

    EXAMPLES = """

## 示例：

需求：查找属于某个业务域的所有应用程序。
查询：
MATCH (d:BusinessDomain {name: 'DomainName'})-[:CONTAINS]-(a:Application)
RETURN a
-- 替换 DomainName 为目标业务域的名称。

需求：查找与某个应用程序关联的所有数据实体。
查询：
MATCH (a:Application {name: 'ApplicationName'})-[r]-(e:DataEntity)
RETURN e
-- 替换 ApplicationName 为目标应用程序的名称。

需求：查找某个数据实体所关联的应用。
查询：
MATCH (e:DataEntity {name: 'EntityName'})-[r]-(a:Application)
RETURN a, type(r) as r_name
-- 替换 EntityName 为目标数据实体的名称。

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

需求：查找某个业务域下所有应用程序关联的数据实体。
查询：
MATCH (d:BusinessDomain {name: 'DomainName'})-[:CONTAINS]-(a:Application)-[r]-(e:DataEntity)
RETURN e
-- 替换 DomainName 为目标业务域的名称。

需求：统计某个业务域下所有应用程序的数量
查询：
MATCH (d:BusinessDomain {name: 'DomainName'})-[:CONTAINS]-(a:Application)
RETURN count(a) AS application_count
-- 替换 DomainName 为目标业务域的名称。

需求：列出前 n 个数据实体
查询：
MATCH (e:DataEntity)
RETURN e
LIMIT n
-- 替换 n 为目标数量（例如 10）

需求：查询某个数据实体对应的物理表
查询：
MATCH (e:DataEntity {name: 'EntityName'})-[:IMPLEMENTS]->(t:PhysicalTable)
RETURN e, t
-- 替换 EntityName 为目标数据实体的名称。

需求：查询两个关联实体及其物理表
查询：
MATCH (e1:DataEntity {name: 'EntityName'})-[r]->(e2:DataEntity),
    (e1)-[:IMPLEMENTS]->(t1:PhysicalTable),
    (e2)-[:IMPLEMENTS]->(t2:PhysicalTable)
RETURN e1, e2, r, t1, t2
-- 替换 EntityName 为目标数据实体的名称。

需求：查询某个应用关联的所有实体及其物理表
查询：
MATCH (app:Application {name: 'ApplicationName'})-[r]-(e:DataEntity)-[:IMPLEMENTS]->(t:PhysicalTable)
RETURN app, e, t
-- 替换 ApplicationName 为目标应用程序的名称。

需求：查找业务域下的所有实体
查询：
MATCH (d:BusinessDomain {name: 'DomainName'})-[:CONTAINS]-(a:Application)-[r]-(e:DataEntity)
RETURN e
-- 替换 DomainName 为目标业务域的名称。
    """

    @staticmethod
    def get_agent() -> Agent:
        _mode_setting = settings.get_setting("agents")["age_agent"]

        def graph_schema(ctx: RunContext[AGEGraph]) -> str:
            return ctx.deps.schema + AgeAgentFactory.EXAMPLES

        agent = Agent(
            models.infer_model(_mode_setting["model_name"], _mode_setting["api_key"]),
            model_settings={'temperature': 0.0},
            deps_type=AGEGraph,
            result_type=Response,
            system_prompt=(
                AgeAgentFactory.S_PROMPT,
            ),
        )

        async def validate_result(ctx: RunContext[AGEGraph], result: Response) -> Response:
            if isinstance(result, InvalidRequest):
                return result
            with logfire.span("Validate Age query"):
                if not result.cypher.upper().startswith('MATCH'):
                    raise ModelRetry('请编写一个MATCH的查询。')

                result.cypher = result.cypher.replace("\\n", "\n")
                print(result.cypher)

                try:
                    ctx.deps.explain(result.cypher)
                except Exception as e:
                    logfire.warn('错误查询: {e}', e=e)
                    raise ModelRetry(f'错误查询: {e}') from e

                return result

        agent.result_validator(validate_result)
        agent.system_prompt(graph_schema)

        return agent

age_agent = AgeAgentFactory.get_agent()
