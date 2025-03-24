"""
测试 age_cypher_agent.py 生成Cypher查询
"""
import asyncio
import logfire

# pylint: disable=E0401
from bot.agent import CypherQuery
from bot.agent.age_cypher_agent import age_agent
from bot.graph.age_graph import AGEGraph
from bot.agent.metadata_tools import MetadataHelper
from bot.settings import settings

# 配置日志
logfire.configure(environment='local', send_to_logfire=False)

age_graph = AGEGraph(graph_name=settings.get_setting("age")["graph"],
                    dsn=settings.get_setting("age")["dsn"])
metadata_helper = MetadataHelper(age_graph)

async def bot_call(question: str):
    """
    age_cypher_agent.py 生成Cypher查询
    """
    result = await age_agent.run(question, deps=age_graph)
    q:CypherQuery = result.data

    resp = metadata_helper.query(q)
    return resp

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
