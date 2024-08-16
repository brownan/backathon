import mcon.builders.python
from mcon import Environment, register_alias

env = Environment()

tag = mcon.builders.python.get_pure_tag()
dist = mcon.builders.python.Distribution(env)

wheel = dist.wheel(tag)
wheel.add_sources("backathon")
register_alias("wheel", wheel)

sdist = dist.sdist()
sdist.add_sources(["backathon", "pyproject.toml", "README.md", "construct.py"])
register_alias("sdist", sdist)

register_alias("editable", dist.editable(tag, "."))
