"""对话模型测试"""
import asyncio

from pydantic_ai import Agent
from pydantic_ai.messages import (
    ModelResponse,
    TextPart,
)
import logfire
# pylint: disable=E0401
from bot.chat_app import to_chat_message
from bot.agent.dg_mind import do_it
import bot.models as models
from bot.settings import settings

# 配置日志
logfire.configure(environment='local', send_to_logfire=False)

async def chat(prompt:str):
    """Streams new line delimited JSON `Message`s to the client."""
    _mode_setting = settings.get_setting("agents")["chat_agent"]

    agent = Agent(models.infer_model(_mode_setting["model_name"], _mode_setting["api_key"]),
                  result_type=str,
                  system_prompt="根据提示内容，用中文回答做简单表述以Markdown格式输出。不要额外增加不存在的内容。",)

    answer = await do_it(prompt)

    question = f"""问题：{prompt}
    回答参考：{answer}
"""
    async with agent.run_stream(question) as result:
        async for text in result.stream(debounce_by=0.01):
            # text here is a `str` and the frontend wants
            # JSON encoded ModelResponse, so we create one
            m = ModelResponse(parts=[TextPart(text)], timestamp=result.timestamp())
            print(to_chat_message(m))

def test_01():
    """case 1"""
    prompt = "客户账单 这个数据实体 连接的数据实体有哪些？"
    asyncio.run(chat(prompt))
    