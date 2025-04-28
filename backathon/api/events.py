import asyncio
import json
import logging
import time

import anyio
import fastapi
import sse_starlette

from backathon.api.params import RepoDependency

logger = logging.getLogger("backathon.api.events")

_config_change_listeners: list[asyncio.Queue[str]] = []

api = fastapi.APIRouter()


def send_config_change_event(url: str):
    logger.debug(
        "Sending config change event for %s to %s listeners",
        url,
        len(_config_change_listeners),
    )
    for q in _config_change_listeners:
        q.put_nowait(url)


@api.get("/events")
async def events(repo: RepoDependency) -> sse_starlette.EventSourceResponse:
    send_stream, recv_stream = anyio.create_memory_object_stream(0)

    logger.info("Starting SSE event stream task")

    async def config_change_watcher():
        my_queue = asyncio.Queue()
        _config_change_listeners.append(my_queue)
        logger.debug("Starting config change watcher")
        event_id = 0
        try:
            while True:
                url = await my_queue.get()
                await send_stream.send(
                    sse_starlette.ServerSentEvent(
                        json.dumps({"url": url}),
                        event="configChange",
                        id=str(event_id),
                    )
                )
                event_id += 1
        except asyncio.CancelledError:
            logger.debug("event status watcher canceled and closing")
            raise
        finally:
            _config_change_listeners.remove(my_queue)

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
