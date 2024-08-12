import asyncio
import importlib.metadata
import pathlib


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


def get_version():
    try:
        return importlib.metadata.version("backathon")
    except importlib.metadata.PackageNotFoundError:
        pass

    # Not installed? We're probably running out of a dev environment. Find
    # and read the pyproject.tmol file
    # Note that toml isn't a runtime dependency so it may not be installed in
    # the dev environment. In that case, it's not a big deal to find the real version,
    # since it implies it's not being used in production, just for dev/testing.
    try:
        import toml
    except ModuleNotFoundError:
        return "unknown"

    tomlpath = pathlib.Path(__file__).parent.parent / "pyproject.toml"
    with tomlpath.open("r") as tomlfile:
        data = toml.load(tomlfile)
    return data["project"]["version"]


__version__ = get_version()
