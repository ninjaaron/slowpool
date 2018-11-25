from libaaron import aio
import random
import slowpool.aio as slowpool


async def rec(n):
    await aio.sleep(random.random() * 1)
    return 1/n


async def main():

    count = 0
    async with slowpool.Pool(10) as pool:
        async for output in pool.amap(rec, range(1, 100)):
            print(output)
            count += 1

        print(count)
            
        print()

        for output in await pool.map(rec, range(1, 20)):
            print(output)


aio.run(main())
