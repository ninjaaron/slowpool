import random
import slowpool
import time


def sleepy_reciprocal(n):
    """a function that requires time to execute"""
    time.sleep(random.random())
    return 1 / n


# context manager will shutdown all workers in the pool when it exits.
# same as running pool.empty()
with slowpool.Pool(5) as pool:

    # submit takes the target function as the first argument and any
    # *args and **kwargs are passed to it when it runs.
    future = pool.submit(sleepy_reciprocal, 4)
    print("the reciprocal of 4 =", future.result())

    # we'll come back to this later...
    zero_division = pool.submit(sleepy_reciprocal, 0)

    # map, but concurrent.
    print()
    for num in pool.map(sleepy_reciprocal, range(1, 9)):
        print(f"reciprocal of {int(1/num)} is", num)

    # same as above, but results are returned as they finish, not in
    # not in the order they are submitted.
    print()
    for num in pool.amap(sleepy_reciprocal, range(1, 9)):
        print(f"reciprocal of {int(1/num)} is", num)


# exceptions are raised when results are fetched.
print()
try:
    zero_division.result()
except Exception as e:
    print("caught", type(e))
