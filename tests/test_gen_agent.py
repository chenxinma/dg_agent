import asyncio
from bot.agent.gen_agent import do_it

import logfire

# 配置日志
logfire.configure(environment='local')

def test_01():
    result = asyncio.run(do_it("账单 这个数据实体 连接的数据实体有哪些？"))
    print(result)