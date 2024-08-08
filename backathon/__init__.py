import asyncio


class BackathonEventLoopPolicy(asyncio.DefaultEventLoopPolicy):
    """Our usage of asyncio is a bit different than typical

    Typical usage is to respond to events from external sources. Each event loop task should
    be quick so that the event loop stays ready to react to events which may happen at any time.

    In our backup code, the event loop is used as a mechanism to queue work for
    the main thread to do when called by worker threads. The main thread may block
    doing IO work or database queries, but that's generally okay since we treat asyncio
    more as a work queue than an event handler.

    (We do all database queries from the main thread to avoid any locking problems with
    multiple database connections)

    This policy sets the slow callback duration to some high number, to silence warnings
    when in debug mode about tasks taking too long.
    """

    def new_event_loop(self):
        loop = super().new_event_loop()
        loop.slow_callback_duration = 1e10
        return loop


asyncio.set_event_loop_policy(BackathonEventLoopPolicy())
