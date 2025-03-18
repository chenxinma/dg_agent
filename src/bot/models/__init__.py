from dataclasses import dataclass
from typing import Literal
import  pydantic_ai.models as pydanticai_models

KnownModelName = Literal[
    "deepseek:v3",
    "siliconflow:deepseek-ai/DeepSeek-V3",
    "siliconflow:meta-llama/Llama-3.3-70B-Instruct",
    "siliconflow:Qwen/Qwen2.5-72B-Instruct-128K",
    "ollama:qwen2.5:14b",
    "ollama:qwen2.5:32b",
    "ollama:deepseek-r1:8b",
    "bailian:deepseek-v3",
    "bailian:qwen-max-latest",
    "bailian:qwen-max",
    "bailian:qwen-coder",
]

@dataclass(init=False)
class ModelFactory:
    def fit(self, model: pydanticai_models.Model | KnownModelName)-> bool:
        pass
    
    def create(self, model: pydanticai_models.Model | KnownModelName, api_key:str=None)-> pydanticai_models.Model:
        pass

class DeepseekModelFactory(ModelFactory):
    def fit(self, model: pydanticai_models.Model | KnownModelName)-> bool:
        return model.startswith('deepseek:')
    
    def create(self, model: pydanticai_models.Model | KnownModelName, api_key:str=None)-> pydanticai_models.Model:
        from pydantic_ai.models.openai import OpenAIModel

        return OpenAIModel("deepseek-chat", 
                            base_url="https://api.deepseek.com/v1",
                            api_key=api_key)

class SiliconFlowModelFactory(ModelFactory):
    def fit(self, model: pydanticai_models.Model | KnownModelName)-> bool:
        return model.startswith('siliconflow:')
    
    def create(self, model: pydanticai_models.Model | KnownModelName, api_key:str=None)-> pydanticai_models.Model:
        from bot.models.siliconflow import SiliconFlowModel
        _model_name = model[12:]
        return SiliconFlowModel(_model_name, 
                            base_url="https://api.siliconflow.cn/v1",
                            api_key=api_key)

class OllamaModelFactory(ModelFactory):
    def fit(self, model: pydanticai_models.Model | KnownModelName)-> bool:
        return model.startswith('ollama:')
    
    def create(self, model: pydanticai_models.Model | KnownModelName, api_key:str=None)-> pydanticai_models.Model:
        from pydantic_ai.models.openai import OpenAIModel
        ollama_model = OpenAIModel(model_name=model[7:], 
                                   base_url='http://172.16.37.21:11434/v1',
                                   api_key='1234')
        return ollama_model
    
class BailianModelFactory(ModelFactory):
    def fit(self, model: pydanticai_models.Model | KnownModelName)-> bool:
        return model.startswith('bailian:')
    
    def create(self, model: pydanticai_models.Model | KnownModelName, api_key:str=None)-> pydanticai_models.Model:
        from pydantic_ai.models.openai import OpenAIModel
        _model_name = model[8:]
        return OpenAIModel(_model_name, 
                            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
                            api_key=api_key)
    
class OtherModelFactory(ModelFactory):
    def fit(self, model: pydanticai_models.Model | KnownModelName)-> bool:
        return True
    
    def create(self, model: pydanticai_models.Model | KnownModelName, api_key:str=None)-> pydanticai_models.Model:
        return pydanticai_models.infer_model(model)


model_factories = [
    DeepseekModelFactory(),
    SiliconFlowModelFactory(),
    OllamaModelFactory(),
    BailianModelFactory(),
    OtherModelFactory(),
]

def infer_model(model: pydanticai_models.Model | KnownModelName, api_key:str=None) -> pydanticai_models.Model:
    if isinstance(model, pydanticai_models.Model):
        return model
    
    for factory in model_factories:
        if factory.fit(model):
            return factory.create(model, api_key)