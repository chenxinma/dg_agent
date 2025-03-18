from __future__ import annotations as _annotations

from dataclasses import dataclass
from typing import Literal, Union
from itertools import chain

from pydantic_ai.models import (ModelRequestParameters, 
                                ModelMessage)
from pydantic_ai.models.openai import (
    OpenAIModel,    
    OpenAIModelSettings,)

from openai import NOT_GIVEN, AsyncStream
from openai.types import chat
from openai.types.chat import ChatCompletionChunk


SiliconFlowModelNames = Literal[
    'deepseek-ai/DeepSeek-V3',
    'meta-llama/Llama-3.3-70B-Instruct',
    'Qwen/Qwen2.5-72B-Instruct-128K',
]

SiliconFlowModelName = Union[SiliconFlowModelNames, str]
        
@dataclass(init=False)
class SiliconFlowModel(OpenAIModel):
    def name(self) -> str:
        return f'siliconflow:{self.model_name}'

    async def _completions_create(
        self,
        messages: list[ModelMessage],
        stream: bool,
        model_settings: OpenAIModelSettings,
        model_request_parameters: ModelRequestParameters,
    ) -> chat.ChatCompletion | AsyncStream[ChatCompletionChunk]:
        tools = self._get_tools(model_request_parameters)

        # standalone function to make it easier to override
        if not tools:
            tool_choice: Literal['none', 'required', 'auto'] | None = None
        elif not model_request_parameters.allow_text_result:
            tool_choice = 'auto'
        else:
            tool_choice = 'auto'

        openai_messages = list(chain(*(self._map_message(m) for m in messages)))

        return await self.client.chat.completions.create(
            model=self._model_name,
            messages=openai_messages,
            n=1,
            parallel_tool_calls=model_settings.get('parallel_tool_calls', NOT_GIVEN),
            tools=tools or NOT_GIVEN,
            tool_choice=tool_choice or NOT_GIVEN,
            stream=stream,
            stream_options={'include_usage': True} if stream else NOT_GIVEN,
            max_tokens=model_settings.get('max_tokens', NOT_GIVEN),
            temperature=model_settings.get('temperature', NOT_GIVEN),
            top_p=model_settings.get('top_p', NOT_GIVEN),
            timeout=model_settings.get('timeout', NOT_GIVEN),
            seed=model_settings.get('seed', NOT_GIVEN),
            presence_penalty=model_settings.get('presence_penalty', NOT_GIVEN),
            frequency_penalty=model_settings.get('frequency_penalty', NOT_GIVEN),
            logit_bias=model_settings.get('logit_bias', NOT_GIVEN),
            reasoning_effort=model_settings.get('openai_reasoning_effort', NOT_GIVEN),
        )



