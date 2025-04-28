import asyncio
import json
import logging
import time

import anyio
import fastapi
import sse_starlette

from backathon.api.params import RepoDependency
from backathon.asyncutils import Bus

logger = logging.getLogger("backathon.api.events")

api = fastapi.APIRouter()


_config_change_bus = Bus[str]()


def send_config_change_event(url: str):
    _config_change_bus.send(url)


@api.get("/events")
async def events(repo: RepoDependency) -> sse_starlette.EventSourceResponse:
    send_stream, recv_stream = anyio.create_memory_object_stream(0)

    logger.info("Starting SSE event stream task")

    async def config_change_watcher():
        async for url in _config_change_bus.listen():
            await send_stream.send(
                sse_starlette.ServerSentEvent(
                    json.dumps({"url": url}),
                    event="configChange",
                )
            )

    async def job_status_watcher():
        logger.debug("Starting job status watcher")
        try:
            # Push an initial status into the object stream so the client gets an
            # immediate status
            await send_stream.send(json.dumps(repo.jobs.make_status_message()))

            last_updated = 0
            while True:
                await repo.jobs.wait_for_change()

                now = time.monotonic()
                if now < last_updated + 0.1:
                    await asyncio.sleep(last_updated + 0.1 - now)

                status_msg = repo.jobs.make_status_message()
                await send_stream.send(
                    sse_starlette.ServerSentEvent(
                        json.dumps(status_msg), event="statusUpdate"
                    )
                )
                last_updated = now

        except asyncio.CancelledError:
            logger.debug("SSE event listener cancelled and closing")
            raise

    async def data_sender_task():
        async with asyncio.TaskGroup() as tg:
            tg.create_task(job_status_watcher())
            tg.create_task(config_change_watcher())

    return sse_starlette.EventSourceResponse(
        recv_stream, data_sender_callable=data_sender_task
    )
