from __future__ import annotations as _annotations

from pydantic_ai import Agent

try:
    import bot.models as models
    from . import SQLResponse, AgentFactory
finally:
    pass
from bot.settings import settings

class SqlAgentFactory(AgentFactory):
    """SQL生成器"""
    @staticmethod
    def get_agent() -> Agent:
        _mode_setting = settings.get_setting("agents")["sql_agent"]
        agent = Agent(
            models.infer_model(_mode_setting["model_name"], _mode_setting["api_key"]),
            model_settings={'temperature': 0.0},
            result_type=SQLResponse,
            system_prompt=(
                "参考工具metadata获得的元数据，使用其中Table·full_table_name·属性作为表名 和 columns定义生产查询SQL。",
                "约束使用Trino的SQL语法，中文别名需要使用双引号,不包含结尾分号。结果内容仅包含SQL语句。",
            )
        )
        return agent
    
sql_agent = SqlAgentFactory.get_agent()