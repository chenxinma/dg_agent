"""bailian provider"""
import os
from typing import overload

from openai import AsyncOpenAI
from httpx import AsyncClient as AsyncHTTPClient

from pydantic_ai.providers import Provider
from pydantic_ai.models import cached_async_http_client

class BailianProvider(Provider[AsyncOpenAI]):
    """Provider for bailian API."""

    @property
    def name(self) -> str:
        """provider name"""
        return 'bailian'

    @property
    def base_url(self) -> str:
        """base url for SlilconFlow"""
        return 'https://dashscope.aliyuncs.com/compatible-mode/v1'

    @property
    def client(self) -> AsyncOpenAI:
        """client"""
        return self._client

    @overload
    def __init__(self) -> None: ...

    @overload
    def __init__(self, *, api_key: str) -> None: ...

    @overload
    def __init__(self, *, api_key: str, http_client: AsyncHTTPClient) -> None: ...

    @overload
    def __init__(self, *, openai_client: AsyncOpenAI | None = None) -> None: ...

    def __init__(
        self,
        *,
        api_key: str | None = None,
        openai_client: AsyncOpenAI | None = None,
        http_client: AsyncHTTPClient | None = None,
    ) -> None:
        api_key = api_key or os.getenv('BAILIAN_API_KEY')
        if api_key is None and openai_client is None:
            raise ValueError(
                'Set the `BAILIAN_API_KEY` environment variable or '+
                ' pass it via `BailianProvider(api_key=...)`'
                'to use the Bailian provider.'
            )

        if openai_client is not None:
            self._client = openai_client
        elif http_client is not None:
            self._client = AsyncOpenAI(base_url=self.base_url,
                                       api_key=api_key,
                                       http_client=http_client)
        else:
            self._client = AsyncOpenAI(base_url=self.base_url,
                                       api_key=api_key,
                                       http_client=cached_async_http_client())