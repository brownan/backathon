import asyncio
import logging
import os
from asyncio import Task
from collections.abc import Callable
from collections.abc import Coroutine
from contextvars import Context
from functools import wraps
from typing import Any
from typing import AsyncGenerator
from typing import AsyncIterator
from typing import Awaitable
from typing import Generic
from typing import Iterable
from typing import ParamSpec
from typing import TypeVar

logger = logging.getLogger("backathon.asyncutils")

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
      all other tasks are immediately canceled, the task group context exits, tasks
      are waited for, and all task exceptions are re-raised as an ExceptionGroup.
      - Exception: SystemExit and KeyboardInterrupt are re-raised as-is instead of wrapped
        in an ExceptionGroup

    * If the task group context itself raises an unhandled exception, all remaining tasks
      are canceled and waited for. The original exception, along with any exceptions from
      tasks, are all wrapped in an ExceptionGroup and re-raised.
      - Same exception as above applies

    * If a task is canceled, no special action is taken. The parent task will receive
      the CancelledError when it awaits the sub-task, at which point the next bullet point
      will apply.

    * If a CancelledError exception propagates out of the task group context, then all
      remaining tasks are canceled and waited for. If any task raises an exception, those
      are re-raised as an ExceptionGroup. Otherwise, the CancelledError is propagated.
    """

    def __init__(self, max_tasks: int | None = None):
        if not max_tasks:
            # A few more than the default thread pool workers. Common case is for the
            # bounded_as_completed() method to dispatch something to a thread pool,
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


class HelperTaskGroup:
    """Wrapper for a regular task group, but all tasks are canceled when
    the context exits without error.

    Contrast this with the regular asyncio.TaskGroup behavior which waits
    for all tasks to exit when the context exits without error.

    Useful for launching "helper" tasks while a main task does something, but
    the helper tasks shouldn't outlive the main task nor should they block
    the main task from exiting.

    """

    def __init__(self):
        self._tg = asyncio.TaskGroup()
        self._tasks = set[asyncio.Task]()

    async def __aenter__(self):
        await self._tg.__aenter__()
        return self

    def create_task(
        self,
        coro: Coroutine[Any, Any, _T],
        *,
        name: str | None = None,
        context: Context | None = None,
    ) -> asyncio.Task[_T]:
        task = self._tg.create_task(coro, name=name, context=context)
        self._tasks.add(task)
        task.add_done_callback(lambda _: self._tasks.discard(task))
        return task

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # If an exception was raised, let the task group handle the task
        # cancellation itself.
        if exc_type is None:
            for task in self._tasks:
                if not task.done():
                    task.cancel()
        await self._tg.__aexit__(exc_type, exc_val, exc_tb)


R = TypeVar("R")
P = ParamSpec("P")


def non_reentrant(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    """Decorator for an async function such that only one execution
    is running at a time. If a second call comes in, it will wait on the
    first call's execution to finish and both invocations get the same
    return value

    """
    values: dict[Any, asyncio.Future[R]] = {}

    # Sneak into functools to steal their make_key routine
    # noinspection PyUnresolvedReferences,PyProtectedMember
    from functools import _make_key as make_key

    @wraps(func)
    async def new_func(*args: P.args, **kwargs: P.kwargs) -> R:
        key = make_key(args, kwargs, False)
        if fut := values.get(key):
            logger.debug(
                "non-reentrant function waiting on existing invocation for %s", key
            )
            return await fut

        def on_finish(_):
            del values[key]

        logger.debug("non-reentrant function calling routine with key %s", key)
        fut = asyncio.ensure_future(func(*args, **kwargs))
        values[key] = fut
        fut.add_done_callback(on_finish)
        return await fut

    return new_func


class Bus(Generic[_T]):
    def __init__(self):
        self.listeners: list[asyncio.Queue[_T]] = []

    def send(self, obj: _T):
        for listener in self.listeners:
            listener.put_nowait(obj)

    async def listen(self) -> AsyncIterator[_T]:
        q: asyncio.Queue[_T] = asyncio.Queue()
        try:
            self.listeners.append(q)
            while True:
                value = await q.get()
                yield value
        finally:
            self.listeners.remove(q)
