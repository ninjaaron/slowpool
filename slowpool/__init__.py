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


def yield_complete(futures: t.Set[Future]):
    """takes a set of futures.
    yields results from finished jobs and removes them from the list.

    side effects!
    """
    prune = []
    for future in futures:
        try:
            yield future.result(False)
            prune.append(future)
        except StillRunning:
            pass

    for future in prune:
        futures.remove(future)


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
        jobs = collections.deque()
        for item in iterable:
            jobs.append(self.submit(fn, item))
            try:
                yield jobs[0].result(False)
                jobs.popleft()
            except StillRunning:
                pass
        yield from (future.result() for future in jobs)

    def amap(self, fn, iterable):
        """map a function to an iterable. results are yielded
        asynchronously as they are finished.
        """
        futures = set()
        for item in iterable:
            futures.add(self.submit(fn, item))
            yield from yield_complete(futures)
            time.sleep(0)
        while futures:
            yield from yield_complete(futures)
            time.sleep(0)

    def empty(self):
        """stops all worker threads, waiting for them to finish."""
        while self.workers:
            thread = self.workers.pop()
            while thread.is_alive():
                self.jobs.put(kill)
                time.sleep(0)
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

    def get_space():
        lock.acquire()
        global reserved_space
        while reserved_space + 1.5 > shutil.disk_usage(".").free / (1024 ** 3):
            time.sleep(0.1)
        reserved_space += space
        lock.release()

    def free_space():
        global reserved_space
        reserved_space -= space

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        get_space()
        try:
            return func(*args, **kwargs)
        finally:
            free_space()

    return wrapper
