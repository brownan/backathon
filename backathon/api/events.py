import asyncio
import json
import logging
import time
from functools import cache

import anyio.streams.memory
import fastapi
import sse_starlette
from starlette.routing import Route

from backathon.api.params import RepoDependency
from backathon.signals import ConfigChange
from backathon.signals import JobStatusChange

logger = logging.getLogger("backathon.api.events")

api = fastapi.APIRouter()


@cache
def url_from_route_name(name: str) -> str:
    from backathon.api.main import api

    for route in api.routes:
        if isinstance(route, Route) and route.name == name:
            return route.path
    raise ValueError(f"Unknown route {name}")


@api.get("/events")
async def events(repo: RepoDependency) -> sse_starlette.EventSourceResponse:
    send_stream, recv_stream = anyio.create_memory_object_stream[
        sse_starlette.ServerSentEvent
    ](0)

    logger.info("Starting SSE event stream task")

    async def config_change_watcher():
        async for event in repo.signals.listen(ConfigChange):
            url = url_from_route_name(event.key)
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
            await send_stream.send(
                sse_starlette.ServerSentEvent(
                    json.dumps(repo.jobs.make_status_message()),
                    event="statusUpdate",
                )
            )

            last_updated = 0
            while True:
                await repo.signals.wait(JobStatusChange)

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
