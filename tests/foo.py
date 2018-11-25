import random
import slowpool
import time


def rec(n):
    time.sleep(random.random())
    return 1 / n


with slowpool.Pool(5) as pool:
    for output in pool.amap(rec, range(1, 20)):
        print(output)

    print()

    for output in pool.map(rec, range(10, -1, -1)):
        print(output)
