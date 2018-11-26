from libaaron import aio
import typing as t


class NoCompleteTasks(Exception):
    pass


class Pool:
    """a async interface that mimics a slowpool.Pool -- to some extent"""

    def __init__(self, maxworkers=1):
        """create a new blocking task pool.

        - maxworkers: maximum number simultanious tasks.
        """
        self.max = maxworkers
        self.tasks = set()

    async def submit(self, coro):
        """add a new coroutine to the pool. block while the maximum
        number of jobs is still running.
        """
        if len(self.tasks) >= self.max:
            _, self.tasks = await aio.wait(self.tasks, return_when=aio.FIRST_COMPLETED)
        task = aio.create_task(coro)
        self.tasks.add(task)
        return task

    def _map_queuer(self, fn, iterable, taskcallback):
        """returns a task that will submit new tasks to the pool by
        mapping function to iterable.

        - fn: coroutine function.
        - iterable: iterable that gets mapped to the function
        - taskcallback: after each task is created taskcallback is called
          on it. taskcallback is an async function. Normally used for
          sending the task somewhere interesting, like into a queue.
        """
        iterator = iter(iterable)

        async def queueall():
            while True:
                try:
                    task = await self.submit(fn(next(iterator)))
                    await taskcallback(task)
                except StopIteration:
                    return

        return aio.create_task(queueall())

    def map(self, fn, iterable):
        """map the coroutine function on to the iterable, submitting
        them as tasks too the pool.

        returns an AsyncIterator. Results appear as they become available, but
        in the original order.
        """
        q = aio.Queue()
        return SyncMap(self._map_queuer(fn, iterable, q.put), q)

    def amap(self, fn, iterable):
        """map the coroutine function on to the iterable, submitting
        them as tasks too the pool.

        returns an AsyncIterator. Results appear in the order the finish, not
        in the order they were submitted.
        """
        tasks = set()

        async def add(task):
            tasks.add(task)

        return AsyncMap(self._map_queuer(fn, iterable, add), tasks)

    async def empty(self):
        """wait for all tasks to complete"""
        try:
            _, self.tasks = await aio.wait(self.tasks)
        except ValueError:
            pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.empty()


class SyncMap:
    """Return type of Pool.map"""

    def __init__(self, queuer: aio.Task, q: aio.Queue):
        """queuer is the task which puts submitted tasks in the q"""
        self.queuer = queuer
        self.q = q

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            try:
                return await self.q.get_nowait()
            except aio.QueueEmpty:
                if self.queuer.done():
                    raise StopAsyncIteration
                await aio.sleep(0)
                continue


class AsyncMap:
    """return type of Pool.amap"""

    def __init__(self, queuer: aio.Task, tasks: t.Set[aio.Task]):
        """queuer is the task which adds submitted tasks to the tasks set."""
        self.queuer = queuer
        self.tasks = tasks

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            try:
                done, _ = await aio.wait(self.tasks, return_when=aio.FIRST_COMPLETED)
            # raised if self.tasks is empty.
            except ValueError:
                if self.queuer.done():
                    raise StopAsyncIteration
                await aio.sleep(0)
                continue

            out = done.pop()
            self.tasks.remove(out)
            return await out
