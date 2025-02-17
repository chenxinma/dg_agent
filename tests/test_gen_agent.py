import asyncio
from bot.agent.dg_mind import do_it

import logfire

# 配置日志
logfire.configure(environment='local')

# def test_01():
#     result = asyncio.run(do_it("客户账单 这个数据实体 连接的数据实体有哪些？"))
#     print(result)
    
# def test_02():
#     result = asyncio.run(do_it("统计客户账单数量"))
#     print(result)
    
def test_03():
    result = asyncio.run(do_it("统计本月资金账户收入流水的总金额"))
    print(result)