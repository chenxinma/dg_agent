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
                "步骤 *age_agent* : 将问题提供给此步骤可以生成查询。 此步骤可以查询 数据实体、物理表、应用程序、业务域和关联的定义。",
                "步骤 *sql_agent* : 根据提供的PhysicalTable (物理表)和RELATED_TO的关联的定义生成SQL",
                "根据用户问题做编排合理的步骤执行计划。",
                "**约束：**: 仅生成执行计划，不做实际工具的执行。"
                "**约束：**: 执行sql_agent前需要先执行age_agent获得PhysicalTable和RELATED_TO的定义。"
            ),
        )
        return agent
    
plan_agent = PlanAgentFactory.get_agent()