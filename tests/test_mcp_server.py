"""测试MCP Server"""
import asyncio
import logfire
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# pylint: disable=E0401
from bot.agent import CypherQuery

# 配置日志
logfire.configure(environment='local', send_to_logfire=False,)

server_params = StdioServerParameters(
    command="python", # Executable
    args=["src\\bot\\mcp_cypher_server.py"], # Optional command line arguments
    env=None # Optional environment variables
)

async def run():
    """run"""
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            # Initialize the connection
            await session.initialize()

            # List available prompts
            prompts = await session.list_prompts()
            print("prompts", prompts)

            # Get a prompt
            prompt = \
                await session.get_prompt("cypher_generate_prompt",
                                        arguments={"message": "查询'外包'相关的应用有哪些数据实体?"})
            print("prompt1", prompt)

            # List available resources
            resources = await session.list_resources()
            print("resources", resources)

            # List available tools
            tools = await session.list_tools()
            print("tools", tools)

            # Read a resource
            content, mime_type = await session.read_resource("schema://data_governance") # pyright: ignore[reportArgumentType]
            print("content and mime_type", content, mime_type)

            # Call a tool
            result = await session.call_tool("cypher_query",
                        arguments={
                            "query": CypherQuery(
                                cypher="MATCH (n:DataEntity) RETURN n LIMIT 10",
                                explanation="列出10个数据实体"
                            ),
                        })
            print("result", result)

def test_case01():
    """test_case01"""
    asyncio.run(run())
