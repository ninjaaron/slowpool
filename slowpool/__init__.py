"""This is a module for a thread pool which blocks on submitting new jobs
until the job is actually running. I need it because reasons.

Due to this behavior, Futures can't be cancelled, and there is no need
to check if they've started. They have.
"""
import functools
import queue
import shutil
import time
import threading

kill = object()


class StillRunning(Exception):
    pass


class Future:
    def __init__(self):
        self.q = queue.Queue(1)
        self.exception = None

    def result(self, block=True):
        try:
            out = self.q.get(block)
        except queue.Empty:
            raise StillRunning("job is still running")
        if self.exception:
            raise self.exception
        return out


def worker(jobs: queue.Queue):
    while True:
        item = jobs.get()
        if item is kill:
            return
        future, fn, args, kwargs = item
        try:
            future.q.put(fn(*args, **kwargs))
        except Exception as e:
            future.exception = e
            future.q.put(None)


def yield_complete(futures):
    """has side effects on the futures list"""
    prune = []
    for i, future in enumerate(futures):
        try:
            yield future.result(False)
            prune.append(i)
        except StillRunning:
            pass
    for i in reversed(prune):
        del futures[i]


class Pool:
    def __init__(self, maxworkers=1):
        self.max = maxworkers
        self.workers = []
        self.jobs = queue.Queue(1)

    def submit(self, fn, *args, **kwargs):
        if len(self.workers) < self.max:
            thread = threading.Thread(target=worker, args=(self.jobs,))
            thread.start()
            self.workers.append(thread)
        future = Future()
        self.jobs.put((future, fn, args, kwargs))
        return future

    def map(self, fn, iterable):
        jobs = [self.submit(fn, item) for item in iterable]
        return (future.result() for future in jobs)

    def amap(self, fn, iterable):
        futures = []
        for item in iterable:
            futures.append(self.submit(fn, item))
            yield from yield_complete(futures)
        while futures:
            yield from yield_complete(futures)

    def empty(self):
        while self.workers:
            thread = self.workers.pop()
            while thread.is_alive():
                self.jobs.put(kill)
            thread.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.empty()


def needs_space(func=None, space=0.5):
    if func is None:
        return functools.partial(needs_space, space=space)

    lock = threading.Semaphore()
    reserved_space = 0.0  # reserved_space in GB

    def get_space():
        lock.acquire()
        nonlocal reserved_space
        while reserved_space + 1.5 > shutil.disk_usage(".").free / (1024 ** 3):
            time.sleep(0.3)
        reserved_space += space
        lock.release()

    def free_space():
        nonlocal reserved_space
        reserved_space -= space

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        get_space()
        try:
            return func(*args, **kwargs)
        finally:
            free_space()

    return wrapper
