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
import threading
import time
import weakref

_instances = []
_tlock = threading.RLock()

CURSOR_UP = '\x1b[A'
CURSOR_DOWN = '\n'
CURSOR_HOME = '\r'
CLEAR_LINE = '\x1b[2K'

FP = sys.stderr

def draw_all():
    with _tlock:
        buf = io.StringIO()
        buf.write(CLEAR_LINE)
        for instance in _instances:
            buf.write(CURSOR_DOWN)
            buf.write(CLEAR_LINE)
            if instance.closed:
                buf.write(instance.final_line)
            else:
                buf.write(instance.get_status())
            buf.write(CURSOR_HOME)

        buf.write(CURSOR_UP * len(_instances))

        FP.write(buf.getvalue())
        FP.flush()


def stderr_write(s):
    with _tlock:
        buf = io.StringIO()
        buf.write(CLEAR_LINE)
        buf.write((CURSOR_DOWN+CLEAR_LINE)*len(_instances))
        buf.write(CURSOR_UP*len(_instances))
        buf.write(s)
        if not s.endswith(CURSOR_DOWN):
            buf.write(CURSOR_DOWN)
        FP.write(buf.getvalue())
        draw_all()

class StatusLineBase:
    def __init__(self):
        self.closed = False
        self.final_line = ""

        with _tlock:
            self._pos = len(_instances)
            _instances.append(self)
            self.refresh()

    def refresh(self):
        draw_all()

    def close(self):
        if self.closed:
            return

        with _tlock:
            self.closed = True
            self.final_line = self.get_final_status()

            self.refresh()

            if all(i.closed for i in _instances):
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
    def __init__(self, initial="", final=None, prefix=""):
        self.status = initial
        self.final = final
        self.prefix = prefix
        super().__init__()

    def get_status(self):
        return self.prefix + self.status

    def get_final_status(self):
        if self.final is not None:
            return self.prefix + self.final
        return self.get_status()

    def update(self, status):
        self.status = status
        self.refresh()

class Spinner(StatusLineBase):
    """Prints a spinner that spins if progress is being made

    The caller must call "spin" once in a while or the spinner won't spin.
    This way there is feedback whether progress is being made. This is useful
    in iterative processes where the total amount isn't known. See
    AutoSpinner for use with blocking routines (such as database queries).
    """
    spinner_chars = r'-\|/'
    max_spin_rate = 0.1

    def __init__(self, prefix, final="Done"):
        self.prefix = prefix
        self.final = final
        self._pos = 0
        self._last_spin = 0
        super().__init__()

    def get_status(self):
        return self.prefix + self.spinner_chars[self._pos]

    def get_final_status(self):
        return self.prefix + self.final

    def spin(self):
        now = time.monotonic()
        if now - self._last_spin >= self.max_spin_rate:
            self._pos = (self._pos + 1) % len(self.spinner_chars)
            self._last_spin = now
            self.refresh()

class IteratorSpinner(Spinner):
    """A spinner that wraps an iterator and spins each iteration"""

    def __init__(self, it, prefix, final="Done"):
        self.it = it
        super().__init__(prefix, final)

    def __iter__(self):
        with self:
            for item in self.it:
                yield item
                self.spin()

def _auto_spinner_thread(spinner_weakref):
    spinner = spinner_weakref()
    if spinner is None or spinner.closed:
        return
    rate = spinner.max_spin_rate
    del spinner

    while True:
        time.sleep(rate)
        spinner = spinner_weakref()
        if spinner is None or spinner.closed:
            return
        try:
            spinner.spin()
        finally:
            del spinner

class AutoSpinner(Spinner):
    """A spinner that keeps spinning even if nothing calls spin()

    Useful for blocking calls and such, but may indicate progress even if
    code has deadlocked/frozen
    """
    def __init__(self, prefix, final="Done"):
        super().__init__(prefix, final)
        threading.Thread(
            target=_auto_spinner_thread,
            args=(weakref.ref(self),),
        ).start()

class ProgressBar(StatusLineBase):
    format = "{prefix}{percent}{bar} {count}/{total} {elapsed}<{remaining} " \
             "{rate}{unit}/s"

    def __init__(self, total, prefix="", unit=""):
        if not prefix.endswith(" "):
            prefix = prefix + " "
        self.prefix = prefix
        self.n = 0
        self.total = total
        self.unit = unit

        self._last_update_t = 0
        self._last_update_n = 0


        super().__init__()

    def _update(self):
        # Updates the prediction
        now = time.monotonic()
        if self.n != self._last_update_n or \
                now - self._last_update_t > 5:
            # Perform an update
            delta_t = now - self._last_update_t

            # TODO

            self._last_update_t = now
            self._last_update_n = self.n

    def increment(self):
        self.n += 1
        self.refresh()
