import asyncio
import os
from asyncio import Task
from collections.abc import Coroutine
from contextvars import Context
from typing import Any, AsyncGenerator, Iterable, TypeVar

_T = TypeVar("_T")


async def bounded_as_completed(
    coros: Iterable[Coroutine[Any, Any, _T]], max_tasks: int | None = None
) -> AsyncGenerator[Task[_T], None]:
    """Submits the given iterable of coroutines as tasks, yielding the task objects
    as they are submitted

    A maximum of max_tasks are submitted at any one time. Once the maximum number of
    tasks is running, the for-loop will block until a task finishes, at which point a
    new task will be submitted and yielded.
    """
    q = asyncio.Queue()

    async def inner():
        async with BoundedTaskGroup(max_tasks) as tg:
            for c in coros:
                t = await tg.create_task(c)
                q.put_nowait(t)
        q.put_nowait(None)

    async with asyncio.TaskGroup() as outer_tg:
        outer_tg.create_task(inner())
        while (ret_task := await q.get()) is not None:
            yield ret_task


class BoundedTaskGroup:
    """A task group but there's a maximum number of tasks allowed any any one time

    The create_task() method is now a coroutine, which may block if the task group
    is full.

    This is useful when a routine is dispatching a large number of tasks, but doesn't
    want to submit them all at once. Reasons this is useful:
    * Each task has memory requirements and realizing all tasks at once will take more
      memory than desired.
    * The work to create each task isn't insignificant and the calling routine wants
      to yield to the event loop to let tasks process

    Exception handling and task cancelling work the same way as asyncio.TaskGroup. For
    reference:

    * If any task in the group raises an exception (other than CancelledError), then
      all other tasks are immediately cancelled, the task group context exits, tasks
      are waited for, and all task exceptions are re-raised as an ExceptionGroup.
      - Exception: SystemExit and KeyboardInterrupt are re-raised as-is instead of wrapped
        in an ExceptionGroup

    * If the task group context itself raises an unhandled exception, all remaining tasks
      are cancelled and waited for. The original exception, along with any exceptions from
      tasks, are all wrapped in an ExceptionGroup and re-raised.
      - Same exception as above applies

    * If a task is canceled, no special action is taken. The parent task will receive
      the CancelledError when it awaits the sub-task, at which point the next bullet point
      will apply.

    * If the task group's parent task is canceled, then all remaining tasks are canceled
      and waited for. If any task raises an exception, those are re-raised as an
      ExceptionGroup. Otherwise, the CancelledError is propagated.
    """

    def __init__(self, max_tasks: int | None = None):
        if not max_tasks:
            # A few more than the default thread pool workers. Common case is for the
            # bounded_coroutine_gather() method to dispatch something to a thread pool,
            # and we'd want to be able to fill it up, plus have a few tasks in the queue
            # ready to go.
            max_tasks = min(36, (os.cpu_count() or 1) + 8)
        self.max_tasks = max_tasks
        self.tg = asyncio.TaskGroup()
        self._sem = asyncio.Semaphore(value=max_tasks)

    async def __aenter__(self):
        await self.tg.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.tg.__aexit__(exc_type, exc_val, exc_tb)

    async def create_task(
        self,
        coro: Coroutine[Any, Any, _T],
        *,
        name: str | None = None,
        context: Context | None = None,
    ) -> asyncio.Task[_T]:
        await self._sem.acquire()
        task = self.tg.create_task(coro, name=name, context=context)
        task.add_done_callback(lambda _: self._sem.release())
        return task
