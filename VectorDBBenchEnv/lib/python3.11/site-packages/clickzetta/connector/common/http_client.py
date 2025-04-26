import aiohttp
import asyncio
from typing import Dict, Optional
import logging
from concurrent.futures import TimeoutError

from clickzetta.connector.v0.exceptions import CZException

_log = logging.getLogger(__name__)


class HttpClient:
    def __init__(self, timeout: int = 120, read_timeout: int = 120, show_debug_log: bool = False):
        self.timeout = aiohttp.ClientTimeout(total=timeout, sock_read=read_timeout)
        self.show_debug_log = show_debug_log
        self.session = None
        self._timeout_tasks = set()

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def call_rest_api_async(self,
                                  url: str,
                                  target_url: str,
                                  method: str,
                                  headers: Dict[str, str],
                                  json_data: str,
                                  timeout: Optional[int] = None) -> str:
        """Async HTTP request with timeout control"""
        if self.show_debug_log:
            _log.warning(f"Call async rest api in {url}{target_url}, method: {method}, jsonData: {json_data}")

        full_url = url + target_url

        # Create timeout task
        timeout_task = asyncio.create_task(
            asyncio.sleep(timeout or self.timeout.total)
        )
        self._timeout_tasks.add(timeout_task)

        try:
            async with self.session.request(
                    method=method,
                    url=full_url,
                    headers=headers,
                    json=json_data,
                    timeout=self.timeout
            ) as response:
                # Cancel timeout task if request completes
                timeout_task.cancel()

                if 200 <= response.status < 300:
                    return await response.text()
                else:
                    error_text = await response.text()
                    raise CZException(
                        f"Error response with url: {full_url}, "
                        f"method: {method}, json {json_data}, "
                        f"response {response.status}: {error_text}"
                    )

        except asyncio.TimeoutError:
            raise TimeoutError(
                f"Request timeout for {full_url}"
            )
        except Exception as e:
            raise CZException(f"Request failed: {str(e)}")
        finally:
            self._timeout_tasks.discard(timeout_task)

    def close(self):
        """Cancel any pending timeout tasks"""
        for task in self._timeout_tasks:
            task.cancel()
