"""
测试 age_cypher_agent.py 生成Cypher查询
"""
import asyncio
import logfire
from pydantic_ai import UnexpectedModelBehavior, capture_run_messages

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
    with capture_run_messages() as messages:
        try:
            result = await age_agent.run(question, deps=age_graph)
            q:CypherQuery = result.data

            resp = metadata_helper.query(q)
            return resp
        except UnexpectedModelBehavior as e:
            print(messages)
            raise e

# def test_01():
#     """
#     测试 age_cypher_agent.py 生成Cypher查询 case1
#     """
#     question = "查询与'客户账单'相关的数据实体及其RELATED_TO关联信息。"
#     result = asyncio.run(bot_call(question))
#     print(result)

# def test_02():
#     """
#     测试 age_cypher_agent.py 生成Cypher查询 case2
#     """
#     question = "列出10个应用"
#     result = asyncio.run(bot_call(question))
#     print(result)

def test_03():
    """
    测试 age_cypher_agent.py 生成Cypher查询 case3
    """
    question = "请提供与'客户账单'相关的物理表以及数据实体间关联的属性。"

    result = asyncio.run(bot_call(question))
    print(result)
