from __future__ import annotations as _annotations

from pydantic_ai import Agent

try:
    import bot.models as models
    from . import PlanResponse, AgentFactory
finally:
    pass
from bot.settings import settings

class PlanAgentFactory(AgentFactory):
    @staticmethod
    def get_agent() -> Agent:
        _mode_setting = settings.get_setting("agents")["plan_agent"]
        agent = Agent(
            models.infer_model(_mode_setting["model_name"], _mode_setting["api_key"]),
            model_settings={'temperature': 0.0},
            result_type=PlanResponse,
            system_prompt=(
                "工具 *age_agent* : 通过图数据库查询Application（应用程序）、Domain（业务域）、Entity（数据实体）、Table（物理表）、Column（列|字段）",
                "工具 *sql_agent* : 根据提供的Table（物理表）的定义生成SQL",
                "根据用户问题做一个利用这些工具的执行计划。sql_agent执行的前提是通过age_agent询问数据实体相关的信息获得所需数据实体的定义以此为基础生成SQL。",
                "*仅生成执行计划，不做实际工具的执行。*"
            ),
        )
        return agent
    
plan_agent = PlanAgentFactory.get_agent()