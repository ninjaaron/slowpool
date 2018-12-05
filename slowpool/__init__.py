"""This is a module for a thread pool which blocks on submitting new jobs
until the job is actually running. I need it because reasons.

Due to this behavior, Futures can't be cancelled, and there is no need
to check if they've started. They have.
"""
import collections
import functools
import queue
import shutil
import time
import threading
import typing as t

# a dummy object that signals to kill a thread. see `worker` function
kill = object()
ALL_COMPLETED = 'ALL_COMPLETED'
FIRST_COMPLETED = 'FIRST_COMPLETED'
FIRST_EXCEPTION = 'FIRST_EXCEPTION'


class StillRunning(Exception):
    pass


class Future:
    def __init__(self):
        """Futures are returned by jobs in the pool. You're not supposed to
        make thems yourself.
        """
        self.q = queue.Queue(1)  # worker thread puts job output here.
        self.exception = None  # worker thread puts unhandled exceptions here.

    def result(self, block=True):
        """Get the result from the job, if it's ready.

        If the result is not ready:

        - if block is True, block until the result is available
        - otherwise, raise a `StillRunning` exception.

        Exceptions not handled in the job will be raised here.
        """
        try:
            out = self.q.get(block)
        except queue.Empty:
            raise StillRunning("job is still running")

        if self.exception:
            raise self.exception
        return out

    def done(self):
        return self.q.full()

    def __hash__(self):
        return id(self)


def worker(jobs: queue.Queue):
    """Target function of a worker thread. Loops forever, running jobs
    until the `kill` object is recieved. This is an implementation
    detail.
    """
    while True:
        job = jobs.get()
        if job is kill:
            return
        # This same future is handed to the user in the main thread when
        # the job is submitted. This is how we synchronise output. and
        # exception handling.
        future, fn, args, kwargs = job
        try:
            future.q.put(fn(*args, **kwargs))
        except Exception as e:
            future.exception = e
            future.q.put(None)


class Map:
    __slots__ = 'fn', 'iter', 'pool', 'jobs', 'fin'

    def __init__(self, fn, iterable, pool):
        self.fn = fn
        self.iter = iter(iterable)
        self.pool = pool
        self.jobs = collections.deque()
        self.fin = False

    def __iter__(self):
        return self

    def __next__(self):
        while not self.fin:
            try:
                item = next(self.iter)
                self.jobs.append(self.pool.submit(self.fn, item))
            except StopIteration:
                self.fin = True
            future = self.jobs.popleft()
            try:
                return future.result(False)
            except StillRunning:
                self.jobs.appendleft(future)
        try:
            return self.jobs.popleft().result()
        except IndexError:
            raise StopIteration


class AsyncMap:
    __slots__ = 'fn', 'iter', 'pool', 'jobs', 'fin'

    def __init__(self, fn, iterable, pool):
        self.fn = fn
        self.iter = iter(iterable)
        self.pool = pool
        self.jobs = set()
        self.fin = False

    __iter__ = Map.__iter__

    def _check_jobs(self):
        for future in self.jobs:
            if future.done():
                self.jobs.remove(future)
                return future

    def __next__(self):
        while not self.fin:
            try:
                item = next(self.iter)
                self.jobs.add(self.pool.submit(self.fn, item))
            except StopIteration:
                self.fin = True
            done =  self._check_jobs()
            if done:
                return done.result()

        while self.jobs:
            done =  self._check_jobs()
            if done:
                return done.result()
            time.sleep(0.1)
        raise StopIteration


class Pool:
    """a thread pool that blocks new jobs when all workers are busy."""

    def __init__(self, maxworkers=1):
        """maxworkers = number of worker threads."""
        self.max = maxworkers
        self.workers = set()
        self.jobs = queue.Queue(1)

    def submit(self, fn, *args, **kwargs):
        """submit a new job to the pool. block if workers are busy"""
        if len(self.workers) < self.max:
            thread = threading.Thread(target=worker, args=(self.jobs,))
            thread.start()
            self.workers.add(thread)
        future = Future()
        self.jobs.put((future, fn, args, kwargs))
        return future

    def map(self, fn, iterable):
        """map a function to an iterable. runs the functions in the
        thread pool
        """
        return Map(fn, iterable, self) 

    def amap(self, fn, iterable):
        """map a function to an iterable. results are yielded
        asynchronously as they are finished.
        """
        return AsyncMap(fn, iterable, self)

    def empty(self):
        """stops all worker threads, waiting for them to finish."""
        while self.workers:
            thread = self.workers.pop()
            while thread.is_alive():
                self.jobs.put(kill)
                time.sleep(0.1)
            thread.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.empty()


lock = threading.Lock()
reserved_space = 0.0  # reserved_space in GB


def needs_space(func=None, space=0.5):
    """decorator for job functions which will reserve space on the disk before
    they run. If there is not enough space, they wait until there is.

    - space: space to reserve in GiB
    """
    if func is None:
        return functools.partial(needs_space, space=space)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        lock.acquire()
        global reserved_space
        while reserved_space + 1.5 > shutil.disk_usage(".").free / (1024 ** 3):
            time.sleep(0.1)
        reserved_space += space
        lock.release()
        try:
            return func(*args, **kwargs)
        finally:
            reserved_space -= space

    return wrapper
