from functools import cache

from starlette.routing import Route


@cache
def url_for_func(f) -> str:
    from backathon.api.main import api

    for x in api.routes:
        if isinstance(x, Route) and x.endpoint is f:
            return x.path
    raise ValueError(f"Function {f} is not a route")
