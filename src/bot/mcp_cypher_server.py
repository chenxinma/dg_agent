"""MCP Server"""
import os
import sys
from pathlib import Path

from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict

import logfire
import mcp
from mcp import types
from mcp.server.lowlevel import NotificationOptions, Server
from mcp.server.models import InitializationOptions

from mcp.server.sse import SseServerTransport 
from starlette.applications import Starlette 
from starlette.routing import Mount, Route

MCP_DIR = Path(__file__).parent.parent
sys.path.append(str(MCP_DIR))

try:
    from bot.agent import DataGovResponse, CypherQuery
    from bot.graph.age_graph import AGEGraph
    # from bot.graph.kuzu_graph import KuzuGraph
    from bot.graph.ontology.age import MetadataHelper, EXAMPLES as _EXAMPLES
    # from bot.graph.ontology.kuzu import MetadataHelper, EXAMPLES as _EXAMPLES
    from bot.settings import Settings
finally:
    pass

settings = Settings()

# 配置日志
logfire.configure(environment='local', send_to_logfire=False,)

class MCPRetry(Exception):
    """Retry exception"""

@asynccontextmanager
async def app_lifespan(_server: Server) -> AsyncIterator[Dict[str, AGEGraph]]:
    """Manage application lifecycle with type-safe context"""
    # 资源初始化
    # _metadata_graph = \
    #     KuzuGraph(settings.get_setting("kuzu.database"))
    _metadata_graph = \
        AGEGraph(settings.get_setting("age.graph"),
                 settings.get_setting("age.dsn"))
    yield {'metadata_graph': _metadata_graph}

# Pass lifespan to server
sse = SseServerTransport("/messages/")
server = Server("data_governance", lifespan=app_lifespan)
metadata_helper : MetadataHelper = MetadataHelper()

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

    try:
        _wraped_cypher = _wrap_cypher(query.cypher)
        contents = metadata_helper.query(_wraped_cypher, _graph)
    except Exception as e:
        logfire.warn('错误查询: {e}', e=e)
        logfire.warn('Cypher {q}', q=query.cypher)
        raise e

    result : DataGovResponse = {
        "contents":contents,
        "description": query.explanation
    }

    return result

async def handle_sse(request):
    # 定义异步函数handle_sse，处理SSE请求
    # 参数: request - HTTP请求对象
    
    async with sse.connect_sse(
        request.scope, request.receive, request._send
    ) as streams:
        # 建立SSE连接，获取输入输出流
        await server.run(
            streams[0], streams[1], server.create_initialization_options()
        )  # 运行MCP应用，处理SSE连接

starlette_app = Starlette(
    debug=True,  # 启用调试模式
    routes=[
        Route("/sse", endpoint=handle_sse),  # 设置/sse路由，处理函数为handle_sse
        Mount("/messages/", app=sse.handle_post_message),  # 挂载/messages/路径，处理POST消息
    ],
)  # 创建Starlette应用实例，配置路由

if __name__ == "__main__":
    import uvicorn  # 导入uvicorn ASGI服务器
    mcp_host = os.environ.get("MCP_HOST", "127.0.0.1")
    mcp_port = int(os.environ.get("MCP_PORT", "8001"))
    uvicorn.run(starlette_app, host=mcp_host, port=mcp_port)  # 运行Starlette应用，监听默认127.0.0.1和指定端口8001
