import sys
import asyncio
from pathlib import Path

import pytest
import logfire
import chromadb
from chromadb.api import ClientAPI
from pydantic_ai.usage import UsageLimits

from bot.agent.ner_agent import ner_agent
from bot.settings import settings

# 配置日志
logfire.configure(environment='local', send_to_logfire=False)

class TestNERAgent:
    """NER Agent tests"""
    client: ClientAPI

    def setup_method(self):
        self.client = chromadb.PersistentClient(
                path=settings.get_setting("chromadb.persist_directory"))

    async def call_agent(self, question: str) -> str:
        """调用agent"""
        result = await ner_agent.run(question, deps=self.client, 
                    usage_limits=UsageLimits(request_limit=10, total_tokens_limit=32768))
        return result.data

    def test_ask_01(self):
        """测试ask"""
        question = "交付域下有几个应用？"
        result = asyncio.run(self.call_agent(question))
        print(result)
