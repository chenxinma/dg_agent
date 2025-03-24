"""本地定制模型配置"""

from typing import Literal, Any

from pydantic_ai.providers import Provider
import pydantic_ai.models as pydanticai_models
from pydantic_ai.models.openai import OpenAIModel

from bot.provider.vllm import VllmProvider
from bot.provider.ollama import OllamaProvider
from bot.provider.bailian import BailianProvider
from bot.provider.siliconflow import SiliconFlowProvider
from bot.models.siliconflow import SiliconFlowModel

KnownModelName = Literal[
    "siliconflow:deepseek-ai/DeepSeek-V3",
    "siliconflow:meta-llama/Llama-3.3-70B-Instruct",
    "siliconflow:Qwen/Qwen2.5-72B-Instruct-128K",
    "ollama:qwen2.5:14b",
    "ollama:qwen2.5:32b",
    "ollama:deepseek-r1:8b",
    "vllm:qwq",
    "bailian:deepseek-v3",
    "bailian:qwen-max-latest",
    "bailian:qwen-max",
    "bailian:qwen-coder",
]

def infer_provider(provider: str, api_key:str=None) -> Provider[Any]:
    """Infer the provider from the provider name."""
    if provider == 'vllm':
        return VllmProvider(api_key=api_key)
    elif provider == 'ollama':
        return OllamaProvider()
    elif provider == 'slilconflow':
        return SiliconFlowProvider(api_key=api_key)
    elif provider == 'bailian':
        return BailianProvider(api_key=api_key)

def infer_model(model: pydanticai_models.Model | KnownModelName,
                api_key:str=None) -> pydanticai_models.Model:
    """Infer the model from the model name."""
    if isinstance(model, pydanticai_models.Model):
        return model

    provider_name = model.lower().split(":")[0]
    if provider_name in ["siliconflow", "ollama", "bailian", "vllm"]:
        provider = infer_provider(provider_name, api_key)
        model_name = model.split(":")[1]
        if provider_name == "siliconflow":
            return SiliconFlowModel(model_name,
                                    provider=provider)
        return OpenAIModel(model_name,
                            provider=provider)
