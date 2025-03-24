"""MCP Server"""
import sys
from pathlib import Path

from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict

import logfire
import mcp
from mcp import types
from mcp.server.lowlevel import NotificationOptions, Server
from mcp.server.models import InitializationOptions

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
async def app_lifespan(_server: Server) -> AsyncIterator[Dict[str, AGEGraph]]:
    """Manage application lifecycle with type-safe context"""
    # 资源初始化
    _metadata_graph = \
        AGEGraph(graph_name=settings.get_setting("age")["graph"],
                dsn=settings.get_setting("age")["dsn"])
    yield {'metadata_graph': _metadata_graph}

# Pass lifespan to server
server = Server("data_governance", lifespan=app_lifespan)

def _wrap_cypher(cypher: str) -> str:
    c = cypher.replace("\\n", "\n")
    if c.endswith(";"):
        c = c[:-1]
    return c

@server.list_tools()
async def list_tools() -> list[types.Tool]:
    """list tools"""
    _graph = server.request_context.lifespan_context["metadata_graph"]
    return [
        types.Tool(
            name="cypher_query",
            description=
f"""业务域、应用、数据实体、物理表、业务术语信息查询工具。
注意：对name属性的查询例如数据实体名、应用名、业务域名等，不要翻译。
{_graph.schema}
{_EXAMPLES}
""",
            inputSchema={
                "type": "object",
                "properties": {
                    "cypher": {"type": "string", "description": "Cypher query"},
                    "explanation": {"type": "string", "description": "查询的解释，以 Markdown 格式呈现'"},
                },
                "required": ["cypher", "explanation"]
            }
        )
    ]

@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent | types.EmbeddedResource]:
    """Call tool"""
    if name == "cypher_query":
        resp = cypher_query(CypherQuery(cypher=arguments["cypher"],
                                        explanation=arguments["explanation"]))
        return [types.TextContent(type="text", text=str(resp))]
    raise MCPRetry(f"Unknown tool name: {name}")

def cypher_query(query: CypherQuery) -> DataGovResponse:
    """Do cypher query       
    Args:
        query: cypher and explanation from agent to execute
    """
    _graph = server.request_context.lifespan_context["metadata_graph"]
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

async def run():
    """Execute the server"""
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="data_governance",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    import asyncio
    asyncio.run(run())
