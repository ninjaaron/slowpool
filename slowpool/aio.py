from libaaron import aio


class NoCompleteTasks(Exception):
    pass


class Pool:
    """a async interface that mimics a slowpool.Pool"""

    def __init__(self, maxworkers=1):
        self.max = maxworkers
        self.tasks = set()

    async def submit(self, coro):
        if len(self.tasks) >= self.max:
            _, self.tasks = await aio.wait(self.tasks, return_when=aio.FIRST_COMPLETED)
        task = aio.spawn(coro)
        self.tasks.add(task)
        return task

    def _map_queue(self, fn, iterable):
        iterator = iter(iterable)
        q = aio.Queue()

        async def queueall():
            while True:
                try:
                    task = await self.submit(fn(next(iterator)))
                    await q.put(task)
                except StopIteration:
                    await q.put(None)
                    return

        aio.spawn(queueall())
        return q

            

    async def map(self, fn, iterable):
        tasks = [await self.submit(fn(item)) for item in iterable]
        return await aio.gather(*tasks)

    def amap(self, fn, iterable):
        
        return AsyncMap(self._map_queue(fn, iterable))

    async def empty(self):
        try:
            _, self.tasks = await aio.wait(self.tasks)
        except ValueError:
            pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.empty()
        

async def next_complete(tasks):
    if not tasks:
        raise StopAsyncIteration
    for i, task in enumerate(tasks):
        await aio.sleep(0)
        if task.done():
            del tasks[i]
            return await task
    raise NoCompleteTasks


class AsyncMap:
    def __init__(self, q):
        self.tasks = set()

        async def keepqueuing():
            while True:
                task = await q.get()
                if task is None:
                    return
                else:
                    self.tasks.add(task)

        self.queuer = aio.spawn(keepqueuing())

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            try:
                done, _ = await aio.wait(self.tasks, return_when=aio.FIRST_COMPLETED)
            except ValueError:
                if self.queuer.done():
                    raise StopAsyncIteration
                await aio.sleep(0)
                continue

            out = done.pop()
            self.tasks.remove(out)
            return await out
