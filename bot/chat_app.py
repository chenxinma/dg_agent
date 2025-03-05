"""交互服务端"""
from __future__ import annotations as _annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Literal
from contextlib import asynccontextmanager
import fastapi
from fastapi import Depends, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from typing_extensions import TypedDict

import logfire
from pydantic import ValidationError
from pydantic_ai.usage import UsageLimits
from pydantic_ai.exceptions import UnexpectedModelBehavior
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    UserPromptPart,
)
from bot.agent.dg_support import dg_support_agent
from bot.graph.age_graph import AGEGraph
from bot.settings import settings


# 配置日志
logfire.configure(environment='local', send_to_logfire=False,)

origins = [
    "*"
]

THIS_DIR = Path(__file__).parent
usage_limits = UsageLimits(request_limit=10)

@asynccontextmanager
async def lifespan(_app: fastapi.FastAPI):
    """资源初始化"""
    _metadata_graph = \
        AGEGraph(graph_name=settings.get_setting("age")["graph"],
                dsn=settings.get_setting("age")["dsn"])
    yield {'metadata_graph': _metadata_graph}

app = fastapi.FastAPI(lifespan=lifespan)

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
    """Convert a `ModelMessage` to a `ChatMessage`."""
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
            _timestamp = m.timestamp
            if isinstance(_timestamp, datetime):
                _timestamp = _timestamp.isoformat()
            return {
                'role': 'model',
                'timestamp': _timestamp,
                'content': first_part.content,
            }
    raise UnexpectedModelBehavior(f'Unexpected message type for chat app: {m}')

# _mode_setting = settings.get_setting("agents")["chat_agent"]
# agent = Agent(models.infer_model(_mode_setting["model_name"], _mode_setting["api_key"]),
#               result_type=str,
#               system_prompt="根据问题和查询结果，用中文回答做简单表述以Markdown格式输出。不要额外增加不存在的内容。查询结果如果包含SQL则直接返回SQL。",)


async def get_graph(request: Request) -> AGEGraph:
    """get the metadata graph"""
    return request.state.metadata_graph

@app.post('/chat/')
async def post_chat(
    prompt: Annotated[str, fastapi.Form()],
    metadata_graph: AGEGraph = Depends(get_graph)
) -> StreamingResponse:
    """post_chat"""

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
        # SupportResponse
        result = await dg_support_agent.run(prompt, deps=metadata_graph)
        m = ModelResponse(parts=[TextPart(result.data)], 
                          timestamp=datetime.now(tz=timezone.utc).isoformat())
        yield json.dumps(to_chat_message(m)).encode('utf-8') + b'\n'

        # async with dg_support_agent.run_stream(
        #     prompt, deps=metadata_graph
        # ) as result:
        #     async for text in result.stream(debounce_by=0.01):
        #         # text here is a `str` and the frontend wants
        #         # JSON encoded ModelResponse, so we create one
        #         m = ModelResponse(parts=[TextPart(text)], timestamp=result.timestamp())
        #         yield json.dumps(to_chat_message(m)).encode('utf-8') + b'\n'
    return StreamingResponse(stream_messages(), media_type='text/plain')


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(
        'bot.chat_app:app', reload=True, reload_dirs=[str(THIS_DIR)]
    )
