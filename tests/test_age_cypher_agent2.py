"""
测试 age_cypher_agent.py 生成Cypher查询
"""
import asyncio
import logfire
import age
from bot.agent import Deps, GRAPH_NAME, DSN, CypherQuery
from bot.agent.age_cypher_agent import age_agent
from bot.agent.metadata_tools import age_metadata_query, create_factory_chain

# 配置日志
logfire.configure(environment='local', send_to_logfire=False)

async def bot_call(question: str):
    """
    age_cypher_agent.py 生成Cypher查询
    """
    deps = Deps(g_name=GRAPH_NAME, url=DSN)
    result = await age_agent.run(question, deps=deps)
    q:CypherQuery = result.data
    
    chain = create_factory_chain(GRAPH_NAME)
    resp = age_metadata_query(deps.create_ag(), GRAPH_NAME, chain, q)
    return resp

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
    question = "请提供与'客户账单'相关的物理表名称以及它们之间RELATED_TO关系的具体定义。"

    result = asyncio.run(bot_call(question))
    print(result)