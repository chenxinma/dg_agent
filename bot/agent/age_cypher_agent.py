from __future__ import annotations as _annotations

from pydantic_ai import Agent, ModelRetry, RunContext

import logfire

try:
    import bot.models as models
    from . import Deps, Response, InvalidRequest, GRAPH_NAME, AgentFactory
finally:
    pass

from bot.settings import settings


class AgeAgentFactory(AgentFactory):   
    s_prompt = """
任务描述：
你是一个图数据库查询专家，擅长使用PostgreSQL的AGE图数据库环境插件的Cypher查询SQL操作图数据并在explanation给出相应的说明。
以下是一个图数据库的结构描述，请根据用户的需求生成相应的SQL查询语句。
确保查询语句简洁高效，保留{GRAPH_NAME}占位符以便后续执行时替换。
注意变量名避免使用SQL关键字。

图数据库结构：
节点：
Application（应用程序）
Domain（业务域）
Entity（数据实体）
Table（物理表）

关系：
BELONG：Application 属于 Domain，表示一个应用程序属于某个业务域。
LINK：两个 Entity 之间可以连接，表示数据实体之间的关联。
REL：Entity 关联 Application，表示数据实体与应用程序之间的关联。
DUPLICATE：一个 Entity 复制自另一个 Entity，表示数据实体的复制关系。
DEFINE：Entity 定义的  Table，，表示数据实体与物理表之间的关联。

示例：
需求：查找属于某个业务域的所有应用程序。
查询：
SELECT * FROM cypher('{GRAPH_NAME}', $$
    MATCH (a:Application)-[:BELONG]-(d:Domain {name: 'DomainName'})
    RETURN a
$$) AS (a agtype);
-- 替换 DomainName 为目标业务域的名称。

需求：查找与某个应用程序关联的所有数据实体。
查询：
SELECT * FROM cypher('{GRAPH_NAME}', $$
    MATCH (e:Entity)-[:REL]-(a:Application {name: 'ApplicationName'})
    RETURN e
$$) AS (e agtype);
-- 替换 ApplicationName 为目标应用程序的名称。

需求：查找某个数据实体所关联的应用。
查询：
SELECT * FROM cypher('{GRAPH_NAME}', $$
    MATCH (e:Entity {name: 'EntityName'})-[:REL]-(a:Application)
    RETURN a
$$) AS (a agtype);
-- 替换 EntityName 为目标数据实体的名称。

需求：查找两个数据实体之间的直接连接关系。
查询：
SELECT * FROM cypher('{GRAPH_NAME}', $$
    MATCH (e1:Entity {name: 'Entity1'})-[r:LINK]-(e2:Entity {name: 'Entity2'})
    RETURN r
$$) AS (r agtype);
-- 替换 Entity1 和 Entity2 为目标数据实体的名称。

需求：查找某个数据实体的所有复制实体。
查询：
SELECT * FROM cypher('{GRAPH_NAME}', $$
    MATCH (e1:Entity {name: 'EntityName'})-[:DUPLICATE]-(e2:Entity)
    RETURN e2
$$) AS (e2 agtype);
-- 替换 EntityName 为目标数据实体的名称。

需求：查找某个业务域下所有应用程序关联的数据实体。
查询：
SELECT * FROM cypher('{GRAPH_NAME}', $$
    MATCH (d:Domain {name: 'DomainName'})-[:BELONG]-(a:Application)-[:REL]-(e:Entity)
    RETURN e
$$) AS (e agtype);
-- 替换 DomainName 为目标业务域的名称。

需求：查找某个数据实体直接或间接连接的所有其他数据实体（递归查询）。
查询：
SELECT * FROM cypher('{GRAPH_NAME}', $$
    MATCH p = (e1:Entity {name: 'EntityName'})-[:LINK*1..]-(e2:Entity)
    RETURN e2
$$) AS (e2 agtype);
-- 替换 EntityName 为目标数据实体的名称。
-- *1.. 表示递归查询，查找直接或间接连接的所有数据实体。

需求：统计某个业务域下所有应用程序的数量
查询：
SELECT * FROM cypher('{GRAPH_NAME}', $$
    MATCH (a:Application)-[:BELONG]-(d:Domain {name: 'DomainName'})
    RETURN count(a) AS application_count
$$) AS (application_count agtype);
-- 替换 DomainName 为目标业务域的名称。

需求：列出前 n 个数据实体
查询：
SELECT * FROM cypher('{GRAPH_NAME}', $$
    MATCH (e:Entity)
    RETURN e
    LIMIT n
$$) AS (e agtype);
-- 替换 n 为目标数量（例如 10）

需求：查询某个数据实体对应的物理表
查询：
SELECT * FROM cypher('{GRAPH_NAME}', $$
    MATCH (e:Entity {name: 'EntityName'})-[:DEFINE]->(t:Table)
    RETURN e, t
$$) AS (e agtype, t agtype);
-- 替换 EntityName 为目标数据实体的名称。

需求：查询两个关联实体及其物理表
查询：
SELECT * FROM cypher('{GRAPH_NAME}', $$
    MATCH (e1:Entity {name: 'EntityName'})-[:LINK]-(e2:Entity),
      (e1)-[:DEFINE]->(t1:Table),
      (e2)-[:DEFINE]->(t2:Table)
    RETURN e1, e2, t1, t2
$$) AS (e1 agtype, e2 agtype, t1 agtype, t2 agtype);
-- 替换 EntityName 为目标数据实体的名称。

需求：查询某个应用关联的所有实体及其物理表
查询：
SELECT * FROM cypher('{GRAPH_NAME}', $$
    MATCH (app:Application {name: 'ApplicationName'})-[:REL]-(e:Entity)-[:DEFINE]->(t:Table)
    RETURN app, e, t
$$) AS (app agtype, e agtype, t agtype);
-- 替换 ApplicationName 为目标应用程序的名称。

需求：查找业务域下的所有实体
查询：
SELECT * FROM cypher('{GRAPH_NAME}', $$
    MATCH (d:Domain {name: 'DomainName'})-[:BELONG]-(:Application)-[:REL]-(e:Entity)
    RETURN d, COLLECT(e) AS Entities
$$) AS (app agtype, e agtype, t agtype);
-- 替换 DomainName 为目标业务域的名称。

需求：查询实体间的多层关联路径
查询：
SELECT * FROM cypher('{GRAPH_NAME}', $$
    MATCH path = (e1:Entity {name: 'EntityName'})-[:LINK*1..3]-(e2:Entity)
    RETURN path
$$) AS (path agtype);
-- 替换 EntityName 为目标数据实体的名称。
"""

    @staticmethod
    def get_agent() -> Agent:
        _mode_setting = settings.get_setting("agents")["age_agent"]
        agent = Agent(
            models.infer_model(_mode_setting["model_name"], _mode_setting["api_key"]),
            model_settings={'temperature': 0.0},
            deps_type=Deps,
            result_type=Response,
            system_prompt=AgeAgentFactory.s_prompt,
            
        )
        async def validate_result(ctx: RunContext[Deps], result: Response) -> Response:
            if isinstance(result, InvalidRequest):
                return result
            with logfire.span("Validate Age query"):
                if not result.sql.upper().startswith('SELECT'):
                    raise ModelRetry('请编写一个MATCH的查询。')
                
                result.sql = result.sql.replace("\\n", "\n")
                try:
                    q = result.sql.replace("{GRAPH_NAME}", GRAPH_NAME)
                    conn = ctx.deps.create_ag().connection
                    with conn.cursor() as _cursor:
                        _cursor.execute(f'EXPLAIN {q}')
                except Exception as e:
                    logfire.warn(f'SQL: {q}')
                    logfire.warn(f'错误查询: {e}')
                    raise ModelRetry(f'错误查询: {e}') from e
                else:
                    return result
        agent.result_validator(validate_result)


        return agent
            

age_agent = AgeAgentFactory.get_agent()



