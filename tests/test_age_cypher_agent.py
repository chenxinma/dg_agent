import asyncio
from bot.agent import Deps, GRAPH_NAME, DSN, CypherQuery
from bot.agent.age_cypher_agent import age_agent

import logfire

# 配置日志
logfire.configure(environment='local')

async def bot_call(question: str):
    deps = Deps(g_name=GRAPH_NAME, url=DSN)
    result = await age_agent.run(question, deps=deps)
    return result.data

def test_01():
    question = "列出所有应用"
    data = asyncio.run(bot_call(question))
    print(data)
    assert isinstance(data, CypherQuery)
