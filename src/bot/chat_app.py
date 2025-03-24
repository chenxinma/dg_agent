"""交互服务端"""
from __future__ import annotations as _annotations
import asyncio
import sys
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Literal
from contextlib import asynccontextmanager
import fastapi
from fastapi import Depends, Request
from fastapi.responses import StreamingResponse, FileResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from typing_extensions import TypedDict

import logfire
from pydantic_ai import Agent
from pydantic_ai.usage import UsageLimits
from pydantic_ai.exceptions import UnexpectedModelBehavior
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    UserPromptPart,
    PartDeltaEvent,
    TextPartDelta,
    FunctionToolResultEvent,
    FunctionToolCallEvent
)

BOT_DIR = Path(__file__).parent.parent
sys.path.append(str(BOT_DIR))
# pylint: disable=C0413
from bot.agent.dg_support import dg_support_agent
from bot.graph.age_graph import AGEGraph
from bot.settings import settings


# 配置日志
logfire.configure(environment='local', send_to_logfire=False,)

origins = [
    "*"
]

WEBROOT_DIR = Path(__file__).parent.joinpath('web')
usage_limits = UsageLimits(request_limit=10, total_tokens_limit=32768)

@asynccontextmanager
async def lifespan(_app: fastapi.FastAPI):
    """资源初始化"""
    _metadata_graph = \
        AGEGraph(graph_name=settings.get_setting("age")["graph"],
                dsn=settings.get_setting("age")["dsn"])
    yield {'metadata_graph': _metadata_graph}

app = fastapi.FastAPI(lifespan=lifespan)
logfire.instrument_fastapi(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get('/')
async def index() -> FileResponse:
    """显示对话主页面"""
    return FileResponse((WEBROOT_DIR / 'chat_app.html'), media_type='text/html')


@app.get('/chat_app.ts')
async def main_ts() -> FileResponse:
    """对话交互Typescript脚本"""
    return FileResponse((WEBROOT_DIR / 'chat_app.ts'), media_type='text/plain')

@app.get('/chat/')
async def get_chat() -> Response:
    """
    返回历史对话数据 :TODO
    """
    logfire.info('get_chat')
    return Response(
        b'\n',
        media_type='text/plain',
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
                    'timestamp': datetime.now(tz=timezone.utc).isoformat()+":Q",
                    'content': prompt,
                }
            ).encode('utf-8')
            + b'\n'
        )
        try:
            # SupportResponse
            async with dg_support_agent.iter(prompt,
                                            deps=metadata_graph,
                                            usage_limits=usage_limits) as run:
                output_messages: list[str] = []
                _timestamp = datetime.now(tz=timezone.utc).isoformat()
                async for node in run:
                    if Agent.is_model_request_node(node):
                        async with node.stream(run.ctx) as request_stream:
                            async for event in request_stream:
                                if isinstance(event, PartDeltaEvent):
                                    if isinstance(event.delta, TextPartDelta):
                                        output_messages.append(event.delta.content_delta)
                                        m = ModelResponse(parts=[
                                            TextPart("".join(output_messages))],
                                            timestamp=_timestamp)
                                        yield json.dumps(to_chat_message(m)).encode('utf-8') + b'\n'
                    elif Agent.is_call_tools_node(node):
                        # A handle-response node => The model returned some data,
                        # potentially calls a tool
                        async with node.stream(run.ctx) as handle_stream:
                            async for event in handle_stream:
                                if isinstance(event, FunctionToolCallEvent):
                                    output_messages.append(
                                        f'\n\n [Tools] {event.part.tool_name!r} '+
                                        f'开始 ID={event.part.tool_call_id!r} \n\n'
                                    )
                                    m = ModelResponse(parts=[TextPart("".join(output_messages))],
                                                    timestamp=_timestamp)
                                    yield json.dumps(to_chat_message(m)).encode('utf-8') + b'\n'
                                elif isinstance(event, FunctionToolResultEvent):
                                    output_messages.append(
                                        f'[Tools] ID={event.tool_call_id!r} 完成。\n\n'
                                    )
                                    m = ModelResponse(parts=[TextPart("".join(output_messages))],
                                                    timestamp=_timestamp)
                                    yield json.dumps(to_chat_message(m)).encode('utf-8') + b'\n'
        except asyncio.CancelledError:
            print("Stream cancelled.")

    return StreamingResponse(stream_messages(), media_type='text/plain')


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(
        'bot.chat_app:app',
    )
