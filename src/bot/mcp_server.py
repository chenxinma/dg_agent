"""MCP Server"""
import sys
from pathlib import Path

from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict

import logfire
from mcp.server.fastmcp import FastMCP, Context
from mcp.server.fastmcp.prompts.base import Message, AssistantMessage, UserMessage

MCP_DIR = Path(__file__).parent.parent
sys.path.append(str(MCP_DIR))

try:
    from bot.agent import DataGovResponse, CypherQuery
    from bot.agent.metadata_tools import MetadataHelper
    from bot.graph.age_graph import AGEGraph
    from bot.settings import Settings
finally:
    pass

settings = Settings()

# 配置日志
logfire.configure(environment='local', send_to_logfire=False,)

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

class MCPRetry(Exception):
    """Retry exception"""

@asynccontextmanager
async def app_lifespan(_server: FastMCP) -> AsyncIterator[Dict[str, AGEGraph]]:
    """Manage application lifecycle with type-safe context"""
    # 资源初始化
    _metadata_graph = \
        AGEGraph(graph_name=settings.get_setting("age")["graph"],
                dsn=settings.get_setting("age")["dsn"])
    yield {'metadata_graph': _metadata_graph}

# Pass lifespan to server
mcp = FastMCP("data_governance", lifespan=app_lifespan)

def _wrap_cypher(cypher: str) -> str:
    c = cypher.replace("\\n", "\n")
    if c.endswith(";"):
        c = c[:-1]
    return c

@mcp.tool()
def cypher_query(query: CypherQuery, ctx: Context) -> DataGovResponse:
    """业务域、应用、数据实体、物理表、业务术语信息查询工具。
       ** 提示，在执行 cypher_query 前需要先做以下步骤：**
       + 1.引用资源 @schema://data_governance 获得数据治理图数据结构。
       + 2.需要使用'cypher_query_prompt'提示词模板生成query参数。
       
    Args:
        query: cypher and explanation from agent to execute
        ctx: mcp server context
    """
    _graph = ctx.request_context.lifespan_context["metadata_graph"]
    # vaildate cypher

    if not query.cypher.upper().startswith('MATCH'):
        raise MCPRetry('请编写一个MATCH的查询。')

    query.cypher = _wrap_cypher(query.cypher)
    try:
        _graph.explain(query.cypher)
    except Exception as e:
        raise MCPRetry(f'错误查询: {e}') from e

    metadata_helper : MetadataHelper = MetadataHelper(_graph)
    result:DataGovResponse = metadata_helper.query(query)

    return result


@mcp.resource("schema://data_governance")
def get_schema() -> str:
    """Provide the database schema as a resource"""
    ctx = mcp.get_context()
    _graph = ctx.request_context.lifespan_context["metadata_graph"]
    return _graph.schema + "\n 备注：dtype为数据类型"

@mcp.prompt(name="cypher_generate_prompt",
            description="cypher_query执行前的Cypher查询生成时的提示词模板")
def cypher_generate_prompt(message: str) -> list[Message]:
    """
    cypher_query执行前的Cypher查询生成时的提示词模板
    
    Args:
        message: Cypher查询生成时的提示词
    Returns:
        list[Message]: 提示词模板
    """
    ctx = mcp.get_context()
    _graph = ctx.request_context.lifespan_context["metadata_graph"]
    return [
        AssistantMessage(content="""
你是一个数据治理知识支持助手。
你可以根据下面给定的图数据架构生成Cyher查询语句。
+ 你会被问及关于这个图数据中关于业务域、应用、数据实体、物理表和数据实体间的关联（RELATE_TO）相关问题，
 此时可以通过使用'cypher_query'工具执行Cypher获得结果直接反馈。
+ 你会被问及一些业务定义和术语说明，此时可以通过'cypher_query'工具执行Cypher查询'业务术语(BusinessTerm)'获得结果。
注意：对name属性的查询例如数据实体名、应用名、业务域名等，不要翻译。
        """),
        AssistantMessage(content=_graph.schema ),
        AssistantMessage(content=_EXAMPLES ),
        UserMessage(content=message),
    ]

if __name__ == "__main__":
    # Initialize and run the server
    mcp.run()
    # import uvicorn
    # uvicorn.run(
    #     'bot.mcp_server:mcp',
    # )
