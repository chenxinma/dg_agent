from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.models import cached_async_http_client
import httpx
def test_01():
    ollama_model = OpenAIModel(model_name='qwen2.5:32b', 
                               base_url='http://172.16.37.21:11434/v1', 
                               api_key='123')
    agent = Agent(ollama_model, result_type=str)
    result = agent.run_sync('Where were the olympics held in 2012?')
    print(result.data)