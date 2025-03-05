"""dg_support_agent tests"""
import asyncio
import pytest
import logfire
from bot.graph.age_graph import AGEGraph
from bot.agent.dg_support import dg_support_agent, SupportResponse

from bot.settings import settings

# 配置日志
logfire.configure(environment='local', send_to_logfire=False)

class TestDataGovSupportAgent:
    """DataGovSupportAgent tests"""
    age_graph:AGEGraph

    def setup_method(self):
        """初始化"""
        g_name = settings.get_setting("age")["graph"]
        dsn = settings.get_setting("age")["dsn"]

        self.age_graph = AGEGraph(graph_name=g_name, dsn=dsn)

    async def call_agent(self, question: str) -> SupportResponse:
        """调用agent"""

        result = await dg_support_agent.run(question, deps=self.age_graph)
        return result.data

    @pytest.mark.skip()
    def test_ask_01(self):
        """测试ask"""
        question = "列出所有业务域"
        result = asyncio.run(self.call_agent(question))
        print(result)

    @pytest.mark.skip()
    def test_ask_02(self):
        """测试ask"""
        question = "‘财务’业务域下有哪些应用？"
        result = asyncio.run(self.call_agent(question))
        print(result)

    @pytest.mark.skip()
    def test_ask_03(self):
        """测试ask"""
        question = "按月统计'资金账户收入流水'的总金额"
        result = asyncio.run(self.call_agent(question))
        print(result)

    def test_ask_04(self):
        """测试ask"""
        question = "统计本月按结算'客户账单'的收款匹配金额"
        result = asyncio.run(self.call_agent(question))
        print(result)
