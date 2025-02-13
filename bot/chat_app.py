from __future__ import annotations as _annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Literal
from pydantic_ai.usage import UsageLimits

import logfire

# 配置日志
logfire.configure(environment='local')

import fastapi
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from typing_extensions import TypedDict

from pydantic_ai import Agent
from pydantic_ai.exceptions import UnexpectedModelBehavior
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    UserPromptPart,
)

import bot.models as models
from bot.agent.gen_agent import do_it
from bot.settings import settings

origins = [
    "*"
]

THIS_DIR = Path(__file__).parent
usage_limits = UsageLimits(request_limit=10) 

app = fastapi.FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChatMessage(TypedDict):
    """Format of messages sent to the browser."""

    role: Literal['user', 'model']
    timestamp: str
    content: str

def to_chat_message(m: ModelMessage) -> ChatMessage:
    first_part = m.parts[0]
    if isinstance(m, ModelRequest):
        if isinstance(first_part, UserPromptPart):
            return {
                'role': 'user',
                'timestamp': first_part.timestamp.isoformat(),
                'content': first_part.content,
            }
    elif isinstance(m, ModelResponse):
        if isinstance(first_part, TextPart):
            return {
                'role': 'model',
                'timestamp': m.timestamp.isoformat(),
                'content': first_part.content,
            }
    raise UnexpectedModelBehavior(f'Unexpected message type for chat app: {m}')

_mode_setting = settings.get_setting("agents")["chat_agent"]
agent = Agent(models.infer_model(_mode_setting["model_name"], _mode_setting["api_key"]),
              result_type=str,
              system_prompt="根据问题和查询结果，用中文回答做简单表述以Markdown格式输出。不要额外增加不存在的内容。",)

@app.post('/chat/')
async def post_chat(
    prompt: Annotated[str, fastapi.Form()]
) -> StreamingResponse:
    async def stream_messages():
        """Streams new line delimited JSON `Message`s to the client."""
        # stream the user prompt so that can be displayed straight away
        yield (
            json.dumps(
                {
                    'role': 'user',
                    'timestamp': datetime.now(tz=timezone.utc).isoformat(),
                    'content': "内部查询中...",
                }
            ).encode('utf-8')
            + b'\n'
        )
        
        answer = await do_it(prompt)
        question = f"问题：{prompt}\n查询结果：{answer.model_dump_json()}"
        async with agent.run_stream(question) as result:
            async for text in result.stream(debounce_by=0.01):
                # text here is a `str` and the frontend wants
                # JSON encoded ModelResponse, so we create one
                m = ModelResponse(parts=[TextPart(text)], timestamp=result.timestamp())
                yield json.dumps(to_chat_message(m)).encode('utf-8') + b'\n'


    return StreamingResponse(stream_messages(), media_type='text/plain')


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(
        'bot.chat_app:app', reload=True, reload_dirs=[str(THIS_DIR)]
    )
