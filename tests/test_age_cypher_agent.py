"""
测试 age_cypher_agent.py 生成Cypher查询
"""
import asyncio
import logfire
from bot.agent import Deps, GRAPH_NAME, DSN, CypherQuery
from bot.agent.age_cypher_agent import age_agent

# 配置日志
logfire.configure(environment='local', send_to_logfire=False)

async def bot_call(question: str):
    """
    age_cypher_agent.py 生成Cypher查询
    """
    deps = Deps(g_name=GRAPH_NAME, url=DSN)
    result = await age_agent.run(question, deps=deps)
    return result.data

def test_01():
    """
    测试 age_cypher_agent.py 生成Cypher查询 case1
    """
    question = "列出所有应用"
    data = asyncio.run(bot_call(question))
    print(data)
    assert isinstance(data, CypherQuery)

def test_02():
    """
    测试 age_cypher_agent.py 生成Cypher查询 case2
    """
    question = "查找应用“财务管理平台收付费管理”使用的所有数据实体"
    data = asyncio.run(bot_call(question))
    print(data)
    assert isinstance(data, CypherQuery)
    
def test_03():
    """
    测试 age_cypher_agent.py 生成Cypher查询 case2
    """
    question = "查找与“财务管理平台收付费管理”关联的所有数据实体。"
    data = asyncio.run(bot_call(question))
    print(data)
    assert isinstance(data, CypherQuery)