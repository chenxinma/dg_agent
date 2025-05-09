from __future__ import annotations as _annotations

from pydantic_ai import Agent, RunContext

from chromadb.api import ClientAPI
try:
    import bot.models as models
    from . import AgentFactory
finally:
    pass

from bot.settings import settings
from bot.models.embedding import GTEEmbeddingFunction

emb_fn = GTEEmbeddingFunction()


class NERAgentFactory(AgentFactory):
    @staticmethod
    def get_agent() -> Agent[ClientAPI, str]:
        """获取NER Agent"""

        def cypher_search(ctx: RunContext[ClientAPI]) -> str:
            """查询相关的Cypher。

            Args:
                ctx (RunContext[ClientAPI]): 运行上下文
            Returns:
                str: relevant_cypher 文本
            """
            c_cypher = ctx.deps.get_collection(
                name=settings.get_setting("chromadb.cypher_collection"),
                embedding_function=emb_fn)

            relevant = c_cypher.query(
                    query_texts=[ctx.prompt], # pyright: ignore[reportArgumentType]
                    n_results=3,
                    include=['documents'], # pyright: ignore[reportArgumentType]
                )
            relevant_cypher_text = ''
            if relevant is not None and 'documents' in relevant and relevant["documents"]:
                relevant_cypher_text = '\n'.join(relevant["documents"][0])
            
            return relevant_cypher_text

        def names_search(ctx: RunContext[ClientAPI], entities: list[str]) -> str:
            """查询相关标准名称。

            Args:
                ctx (RunContext[ClientAPI]): 运行上下文
                entities (list[str]): 实体列表
            Returns:
                str: names 标准名称列表
            """
            c_names = ctx.deps.get_collection(
                name=settings.get_setting("chromadb.names_collection"),
                embedding_function=emb_fn)
            
            relevant_names = c_names.query(
                query_texts=entities, # pyright: ignore[reportArgumentType]
                n_results=10,
                include=['documents'], # pyright: ignore[reportArgumentType]
            )
            relevant_names_text = ''
            if relevant_names is not None and 'documents' in relevant_names and relevant_names["documents"]:
                relevant_names_text = '\n'.join(relevant_names["documents"][0])

            return relevant_names_text


        model_name = settings.get_setting("agents.ner_agent.model_name")
        api_key = settings.get_setting("agents.ner_agent.api_key")
        agent = Agent(
            models.infer_model(model_name, api_key=api_key),
            model_settings = {'temperature': 0.0},
            deps_type = ClientAPI,
            result_type = str,
            system_prompt = (
                "1.识别提示文本中的实体。"
                "  使用names_search工具查询相关标准名称。"
                "  仅以识别出的实体列表给出<实体>和<标准名称>有关的对照。(不要改变<标准名称>的内容)。\n"
                "2.使用cypher_search工具查询相关Cypher，依据提示文本给出最为相关的Cypher参考。\n"
                "注意：严格按照以下格式输出：\n"
                "## 标准名称对照：\n"
                "- <实体名称1>: (BusinessDomain {name:'<标准名称1>'})\n"
                "- <实体名称2>: (Application {name:'<标准名称2>'})\n"
                "- <实体名称3>: (DataEntity {name:'<标准名称3>'})\n"
                "## Cypher参考建议：\n"
                "- <Cypher>\n"
            )
        )

        agent.tool(cypher_search)
        agent.tool(names_search)

        return agent

ner_agent = NERAgentFactory.get_agent()
