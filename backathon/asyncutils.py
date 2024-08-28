import asyncio
import os
from asyncio import Task
from collections.abc import Coroutine
from contextvars import Context
from typing import Any, Iterable, Sequence, TypeVar

_T = TypeVar("_T")


def parallel_coroutines(
    coros: Iterable[Coroutine[Any, Any, _T]], max_tasks: int | None = None
) -> Task[Sequence[_T]]:
    """Runs all given coroutines as tasks, limiting the number that can run at once

    Gathers all the results and returns a list

    This is useful when each task consumes resources and you want to limit
    parallelism. This function encapsulates the logic to launch tasks, waiting
    for them to complete before launching more.

    Tasks are always run in sequence, but no guarantees are made that any
    will finish before or after any others.

    All results are gathered in memory before returning.

    If any task errors, all other tasks are canceled and task exceptions
    are re-raised as an ExceptionGroup.

    If any sub-task is Canceled, all tasks are allowed to finish, and a
    CancelledError is re-raised when finished.

    If /this/ task is itself canceled, then all sub-tasks are immediately
    canceled and a CancelledError is re-raised.




    """

    async def inner() -> list[_T]:
        tasks: list[Task[_T]] = []
        async with BoundedTaskGroup(max_tasks) as tg:
            for c in coros:
                tasks.append(await tg.create_task(c))
        return [t.result() for t in tasks]

    return asyncio.create_task(inner())


class BoundedTaskGroup:
    def __init__(self, max_tasks: int | None = None):
        if not max_tasks:
            # A few more than the default thread pool workers. Common case is for the
            # parallel_coroutines() method to dispatch something to a thread pool,
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
