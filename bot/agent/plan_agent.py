"""根据用户意图构建执行计划"""
from __future__ import annotations as _annotations

from pydantic_ai import Agent

try:
    import bot.models as models
    from . import PlanResponse, AgentFactory
finally:
    pass
from bot.settings import settings

class PlanAgentFactory(AgentFactory):
    """构造意图识别Agent"""
    @staticmethod
    def get_agent() -> Agent:
        _mode_setting = settings.get_setting("agents")["plan_agent"]
        agent = Agent(
            models.infer_model(_mode_setting["model_name"], _mode_setting["api_key"]),
            model_settings={'temperature': 0.0},
            result_type=PlanResponse,
            system_prompt=(
                "步骤 *age_agent* : 将问题提供给此步骤可以生成查询。 此步骤可以查询以下种类的元数据定义：数据实体、物理表、应用程序、业务域、数据实体间关联的属性。",
                "步骤 *sql_agent* : 根据提示词里的参考物理表和物理表关联定义生成SQL",
                """根据用户问题做编排合理的步骤执行计划。
                执行说明：
                  如果问题是关于统计、查询的逻辑，请按照age_agent->sql_agent的顺序生成SQL。
                  age_agent步骤获得所需的数据实体信息和数据实体间关联属性，sql_agent步骤根据age_agent步骤生成的SQL执行。""",
                "**约束：**: 仅生成执行计划，不做实际工具的执行。"
            ),
        )
        return agent

plan_agent = PlanAgentFactory.get_agent()
