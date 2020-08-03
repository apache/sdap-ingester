import asyncio
from datetime import datetime
from unittest import mock


class AsyncMock(mock.MagicMock):
    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


class AsyncAssert:
    @staticmethod
    async def assert_called_within_timeout(mock_func, timeout_sec=1.0, call_count=1):
        start = datetime.now()

        while (datetime.now() - start).total_seconds() < timeout_sec:
            await asyncio.sleep(0.01)
            if mock_func.call_count >= call_count:
                return
        raise AssertionError(f"Mock did not reach {call_count} calls called within {timeout_sec} sec")


def async_test(coro):
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        return loop.run_until_complete(coro(*args, **kwargs))

    return wrapper
