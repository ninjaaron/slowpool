import random
import slowpool
import time


def rec(n):
    time.sleep(random.random())
    return 1/n


pool = slowpool.Pool(5)
for output in pool.amap(rec, range(1, 100)):
    print(output)

pool.empty()
