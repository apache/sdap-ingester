import asyncio
from urllib.parse import urlparse
import datetime
import os
import time
from dataclasses import dataclass
from typing import Set, Dict, Optional, Callable, Awaitable

import aioboto3

os.environ['AWS_PROFILE'] = "saml-pub"
os.environ['AWS_DEFAULT_REGION'] = "us-west-2"


@dataclass
class S3Event:
    src_path: str
    modified_time: datetime.datetime


class S3FileModifiedEvent(S3Event):
    pass


class S3FileCreatedEvent(S3Event):
    pass


class S3Watch(object):
    def __init__(self, path: str, event_handler) -> None:
        self.path = path
        self.event_handler = event_handler


class S3Observer:

    def __init__(self, bucket, initial_scan=False) -> None:
        self._bucket = bucket
        self._cache: Dict[str, datetime.datetime] = {}
        self._initial_scan = initial_scan
        self._watches: Set[S3Watch] = set()

        self._has_polled = False

    async def start(self):
        await self._run_periodically(loop=None,
                                     wait_time=30,
                                     func=self._poll)

    def unschedule(self, watch: S3Watch):
        self._watches.remove(watch)

    def schedule(self, event_handler, path: str):
        watch = S3Watch(path=path, event_handler=event_handler)
        self._watches.add(watch)
        return watch

    @classmethod
    async def _run_periodically(cls,
                                loop: Optional[asyncio.AbstractEventLoop],
                                wait_time: float,
                                func: Callable[[any], Awaitable],
                                *args,
                                **kwargs):
        """
        Call a function periodically. This uses asyncio, and is non-blocking.
        :param loop: An optional event loop to use. If None, the current running event loop will be used.
        :param wait_time: seconds to wait between iterations of func
        :param func: the async function that will be awaited
        :param args: any args that need to be provided to func
        """
        if loop is None:
            loop = asyncio.get_running_loop()
        await func(*args, **kwargs)
        loop.call_later(wait_time, loop.create_task, cls._run_periodically(loop, wait_time, func, *args, **kwargs))

    async def _poll(self):
        new_cache = {}
        watch_index = {}

        for watch in self._watches:
            new_cache_for_watch = await self._get_s3_files(watch.path)
            new_index = {file: watch for file in new_cache_for_watch}

            new_cache = {**new_cache, **new_cache_for_watch}
            watch_index = {**watch_index, **new_index}
        difference = set(new_cache.items()) - set(self._cache.items())

        if self._has_polled or self._initial_scan:
            for (file, modified_date) in difference:
                watch = watch_index[file]
                file_is_new = file not in self._cache

                if file_is_new:
                    watch.event_handler.on_created(S3FileCreatedEvent(src_path=file, modified_time=modified_date))
                else:
                    watch.event_handler.on_modified(S3FileModifiedEvent(src_path=file, modified_time=modified_date))

        self._cache = new_cache
        self._has_polled = True

    async def _get_s3_files(self, path: str):
        new_cache = {}

        start = time.perf_counter()
        async with aioboto3.resource("s3") as s3:
            bucket = await s3.Bucket(self._bucket)

            object_key = S3Observer._get_object_key(path)
            async for file in bucket.objects.filter(Prefix=object_key):
                new_cache[f"s3://{file.bucket_name}/{file.key}"] = await file.last_modified
        end = time.perf_counter()
        duration = end - start

        print(f"Retrieved {len(new_cache)} objects in {duration}")

        return new_cache

    def _get_object_key(full_path: str):
        key = urlparse(full_path).path.strip("/")
        return key


async def test():
    observer = S3Observer(bucket="nexus-ingest", initial_scan=False)
    handler = Handler()
    observer.schedule(handler, 'avhrr/2012')
    observer.schedule(handler, 'avhrr/2013')

    await observer.start()

    while True:
        try:
            await asyncio.sleep(1)
        except KeyboardInterrupt:
            return


class Handler:
    def on_created(self, event: S3Event):
        print(f"File created: {event.src_path}")

    def on_modified(self, event: S3Event):
        print(f"File modified: {event.src_path}")


if __name__ == "__main__":
    asyncio.run(test())
