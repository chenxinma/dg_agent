import asyncio
from bot.agent.dg_mind import do_it

import logfire

# 配置日志
logfire.configure(environment='local', send_to_logfire=False)

# def test_01():
#     result = asyncio.run(do_it("客户账单 这个数据实体 连接的数据实体有哪些？"))
#     print(result)
    
# def test_02():
#     result = asyncio.run(do_it("统计客户账单数量"))
#     print(result)
    
# def test_03():
#     result = asyncio.run(do_it("统计本月按结算'客户账单'的收款匹配金额"))
#     print(result)

def test_04():
    result = asyncio.run(do_it("列出10个应用"))
    print(result)