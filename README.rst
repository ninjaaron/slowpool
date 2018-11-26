slowpool: job pools that block when all workers are busy
========================================================
This is library written for me and may not interest anyone else. With
the normal thread pools in the standard library (concurrent.futures_ and
multiprocessing.dummy_), adding new jobs never blocks. This library
bocks when all workers are busy. This is useful in cases where tighter
synchronization is needed between the main thread and the workers. In my
case, I am making requests to a server that returns links with expiring
tokens. If the links get too old, they aren't useful anymore. Therefore,
the links are fetched lazily and fetching is paused when all workers are
busy.

For added fun, I tried to implement a similar interface based on
`asyncio`_. Coroutines are a close-to-zero-cost alternative to threads
in the sense that they provide a thread-like interface to multiplexing
I/O, and they provide one reasonable solution the 10K problem by blowing
the top off of the density limitations of threads and multiprocessing.

If there isn't a density limitation, why use a pool? On a server, you
wouldn't. This library is aimed more at clients. When scraping data,
you, typically want to avoid the appearance of DoS attacking the server.
It's not very polite to open 1000 concurrent connections to someone's
server. Using a pool allows you to keep the number of connections to a
friendly, pre-determined level.

If you're trying to limit the number of connections, why not just use
threads? Good question, and I don't know if I have an answer. Part of it
is because Python's threads are fake and kind of terrible (i.e. it's
just task switching, not true parallel execution across cores), but they
are fine for I/O multiplexing on a small scale. I guess I'd just say
that I've felt that async/await programs feel snappier than
Python-threaded programs. They can also be more flexible. For example,
you may only want to have a few concurrent connections to any given
server, but with async, you could efficiently scrape from hundreds of
different sites, spinning up a small new pool for each.

.. _multiprocessing.dummy:
  https://docs.python.org/3/library/multiprocessing.html#module-multiprocessing.dummy

.. _concurrent.futures:
  https://docs.python.org/3/library/concurrent.futures.html

.. _asyncio: https://docs.python.org/3/library/asyncio.html

Thread Pool
-----------
Let's start with some examples:

.. code:: python

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

Output:

.. code::

  the reciprocal of 4 = 0.25

  reciprocal of 1 is 1.0
  reciprocal of 2 is 0.5
  reciprocal of 3 is 0.3333333333333333
  reciprocal of 4 is 0.25
  reciprocal of 5 is 0.2
  reciprocal of 6 is 0.16666666666666666
  reciprocal of 7 is 0.14285714285714285
  reciprocal of 8 is 0.125

  reciprocal of 5 is 0.2
  reciprocal of 1 is 1.0
  reciprocal of 4 is 0.25
  reciprocal of 6 is 0.16666666666666666
  reciprocal of 8 is 0.125
  reciprocal of 7 is 0.14285714285714285
  reciprocal of 2 is 0.5
  reciprocal of 3 is 0.3333333333333333

  caught <class 'ZeroDivisionError'>

... more later...
