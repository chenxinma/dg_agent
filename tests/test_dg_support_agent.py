"""dg_support_agent tests"""
import sys
import asyncio
from pathlib import Path

import pytest
import logfire

# pylint: disable=C0413
# pylint: disable=E0401
# from bot.graph.age_graph import AGEGraph
from bot.graph.kuzu_graph import KuzuGraph
from bot.agent.dg_support import dg_support_agent, SupportResponse, SupportDependencies
from bot.settings import settings

# 配置日志
logfire.configure(environment='local', send_to_logfire=False)

class TestDataGovSupportAgent:
    """DataGovSupportAgent tests"""
    kuzu_graph:KuzuGraph

    def setup_method(self):
        """初始化"""
        self.kuzu_graph = KuzuGraph(settings.get_setting("kuzu.database"))

    async def call_agent(self, question: str) -> SupportResponse:
        """调用agent"""
        from bot.graph.ontology.kuzu import MetadataHelper
        result = await dg_support_agent.run(question, 
            deps=SupportDependencies(graph=self.kuzu_graph, metadata_helper=MetadataHelper()))
        return result.data # pyright: ignore

    # @pytest.mark.skip()
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

    @pytest.mark.skip()
    def test_ask_04(self):
        """测试ask"""
        question = "统计本月按结算'客户账单'的收款匹配金额"
        result = asyncio.run(self.call_agent(question))
        print(result)
