"""命令行模式"""
import asyncio

import logfire
from rich.console import Console, ConsoleOptions, RenderResult
from rich.live import Live
from rich.markdown import CodeBlock, Markdown
from rich.syntax import Syntax
from rich.text import Text
from pydantic_ai import Agent
from pydantic_ai.messages import (
    FinalResultEvent,
    FunctionToolCallEvent,
    FunctionToolResultEvent,
    PartDeltaEvent,
    PartStartEvent,
    TextPartDelta,
    ToolCallPartDelta,
)
from pydantic_ai.usage import UsageLimits

from bot.agent.dg_support import dg_support_agent
from bot.graph.age_graph import AGEGraph
from bot.settings import settings

# 配置日志
logfire.configure(environment='local', send_to_logfire=False,)

metadata_graph = \
        AGEGraph(graph_name=settings.get_setting("age")["graph"],
                dsn=settings.get_setting("age")["dsn"])
async def main():
    """main"""
    prettier_code_blocks()
    console = Console()
    prompt = "统计本月'资金账户收入流水'的总金额"
    console.log(f'Asking: {prompt}...', style='cyan')

    with Live('', console=console, vertical_overflow='visible') as live:
        async with dg_support_agent.iter(prompt, deps=metadata_graph) as run:
            async for node in run:
                if Agent.is_user_prompt_node(node):
                    live.update(Markdown(f'=== UserPromptNode: {node.user_prompt} ==='))
                elif Agent.is_model_request_node(node):
                    async with node.stream(run.ctx) as request_stream:
                        async for event in request_stream:
                            if isinstance(event, PartStartEvent):
                                live.update(Markdown( f'[Request] Starting part {event.index}: {event.part!r}' ))
                            elif isinstance(event, PartDeltaEvent):
                                if isinstance(event.delta, TextPartDelta):
                                    live.update(Markdown( f'[Request] Part {event.index} text delta: {event.delta.content_delta!r}' ))
                                elif isinstance(event.delta, ToolCallPartDelta):
                                    live.update(Markdown( f'[Request] Part {event.index} args_delta={event.delta.args_delta}' ))
                            elif isinstance(event, FinalResultEvent):
                                live.update(Markdown( f'[Result] The model produced a final result (tool_name={event.tool_name})'))
                elif Agent.is_call_tools_node(node):
                    live.update(Markdown('=== CallToolsNode: streaming partial response & tool usage ===' ))
                    async with node.stream(run.ctx) as handle_stream:
                        async for event in handle_stream:
                            if isinstance(event, FunctionToolCallEvent):
                                live.update(Markdown(
                                    '[Tools] The LLM calls tool={event.part.tool_name!r} with args={event.part.args} (tool_call_id={event.part.tool_call_id!r})'
                                ))
                            elif isinstance(event, FunctionToolResultEvent):
                                live.update(Markdown(
                                    f'[Tools] Tool call {event.tool_call_id!r} returned => {event.result.content}'
                                ))
                elif Agent.is_end_node(node):
                    assert run.result.data == node.data.data
                    # Once an End node is reached, the agent run is complete
                    live.update(Markdown(f'=== Final Agent Output: {run.result.data} ==='))

            console.log(run.result.usage())


def prettier_code_blocks():
    """Make rich code blocks prettier and easier to copy.

    From https://github.com/samuelcolvin/aicli/blob/v0.8.0/samuelcolvin_aicli.py#L22
    """

    class SimpleCodeBlock(CodeBlock):
        """SimpleCodeBlock"""
        def __rich_console__(
            self, console: Console, options: ConsoleOptions
        ) -> RenderResult:
            code = str(self.text).rstrip()
            yield Text(self.lexer_name, style='dim')
            yield Syntax(
                code,
                self.lexer_name,
                theme=self.theme,
                background_color='default',
                word_wrap=True,
            )
            yield Text(f'/{self.lexer_name}', style='dim')

    Markdown.elements['fence'] = SimpleCodeBlock


if __name__ == '__main__':
    asyncio.run(main())
