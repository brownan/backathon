"""
A really simple status and progress line printing library

Inspired by tqdm, this features nested status lines and progress bars that
can be updated independently. It's extensible for easy customized status
lines and progress bars.

It also features somewhat passable interleaving of lines printed by other
libraries directly to stderr. This is achieved by leaving the cursor on a
blank line above all the status lines, and by clearing all status lines
before writing them out. This way, any rogue lines printed between updates
will go to the blank line above the status lines, at which point the cursor
will move down on top of the status lines. Next update, the current line and
all below it will be cleared out and re-drawn. So rogue lines appear to be
drawn above the status bars.

Currently not threading or multiprocessing safe. Also probably not very
portable.
"""
import sys
import io
import time

_instances = []

CURSOR_UP = '\x1b[A'
CURSOR_DOWN = '\n'
CURSOR_HOME = '\r'
CLEAR_LINE = '\x1b[2K'

FP = sys.stderr

_last_draw = 0
MIN_DRAW_DELAY = 0.1

def draw_all():
    global _last_draw
    now = time.monotonic()
    if now - _last_draw < MIN_DRAW_DELAY:
        return
    _last_draw = now

    buf = io.StringIO()
    buf.write(CLEAR_LINE)
    for instance in _instances:
        buf.write(CURSOR_DOWN)
        buf.write(CLEAR_LINE)
        if instance._closed:
            buf.write(instance._final_line)
        else:
            buf.write(instance.get_status())
        buf.write(CURSOR_HOME)

    buf.write(CURSOR_UP * len(_instances))

    FP.write(buf.getvalue())
    FP.flush()

class StatusLineBase:

    def __init__(self):
        self._pos = len(_instances)
        _instances.append(self)
        self._closed = False
        self._final_line = ""
        self.refresh()

    def refresh(self):
        draw_all()

    def close(self):
        if self._closed:
            return

        self._closed = True
        self._final_line = self.get_final_status()

        self.refresh()

        if all(i._closed for i in _instances):
            FP.write(CURSOR_DOWN*(len(_instances)+1))
            FP.flush()

            _instances.clear()

    def get_status(self):
        raise NotImplementedError()

    def get_final_status(self):
        return self.get_status()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __del__(self):
        self.close()

class StaticStatus(StatusLineBase):
    """Just prints a line that you can update"""
    def __init__(self, initial="", final="", prefix=""):
        self.status = initial
        self.final = final
        self.prefix = prefix
        super().__init__()

    def get_status(self):
        return self.prefix + self.status

    def get_final_status(self):
        return self.prefix + self.final

    def update(self, status):
        self.status = status
        self.refresh()
