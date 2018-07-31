"""
This module makes some modifications to Python's
concurrent.futures.ThreadPoolExecutor implementation.

Mainly, we have to clean up open database connections after a worker thread
exits. I wish there were a cleaner way to do this, but the ThreadPoolExecutor
doesn't really provide a good mechanism to hook in cleanup methods.
"""

import concurrent.futures.thread
import concurrent.futures._base
import threading
import weakref
from concurrent.futures.thread import _threads_queues

from django.db import connections

def _worker(executor_reference, work_queue):
    try:
        concurrent.futures.thread._worker(executor_reference, work_queue)
    finally:
        try:
            for alias in connections:
                try:
                    connection = getattr(connections._connections, alias)
                except AttributeError:
                    continue

                print("Cleaning up connection from thread {}".format(
                    threading.current_thread().name
                ))
                connection.close()
                delattr(connections._connections, alias)
        except BaseException:
            concurrent.futures._base.LOGGER.error(
                "Exception in thread shutdown", exc_info=True
            )

class ThreadPoolExecutor(concurrent.futures.thread.ThreadPoolExecutor):

    # This method copy-pasted from the Python 3.6.5 implementation,
    # and modified according to our needs.
    def _adjust_thread_count(self):
        # When the executor gets lost, the weakref callback will wake up
        # the worker threads.
        def weakref_cb(_, q=self._work_queue):
            q.put(None)
        # TODO(bquinlan): Should avoid creating new threads if there are more
        # idle threads than items in the work queue.
        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            thread_name = '%s_%d' % (self._thread_name_prefix or self,
                                     num_threads)
            t = threading.Thread(name=thread_name, target=_worker,
                                 args=(weakref.ref(self, weakref_cb),
                                       self._work_queue))
            t.daemon = True
            t.start()
            self._threads.add(t)
            _threads_queues[t] = self._work_queue
