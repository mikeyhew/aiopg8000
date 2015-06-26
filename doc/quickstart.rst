Quick Start
===========

Key Points
----------

- Runs on Python version 3.4, 2.5, 2.6, 2.7, 3.2, 3.3 are not supported, because
        they lack asyncio (though presumably the project could be modified to support
        [trollius](https://pypi.python.org/pypi/trollius))
- Runs on CPython, not tested on Jython and PyPy
- Although it's possible for threads to share cursors and connections, for
  performance reasons it's best to use one thread per connection.
- Internally, all queries use prepared statements. aiopg8000 remembers that a
  prepared statement has been created, and uses it on subsequent queries.

Installation
------------

**NOTE: Not on pypi yet so pip probably won't work for aiopg8000, therefore the following
will not work.**

To install aiopg8000 using `pip <https://pypi.python.org/pypi/pip>`_ type:

``pip3 install aiopg8000``

To install aiopg8000 using git:

.. code-block:: bash

    cd ~
    git clone https://github.com/realazthat/aiopg8000.git
    cd aiopg8000
    # as root
    python setup.py install



Example
-------------------

Import aiopg8000, connect to the database, create a table, add some rows and then
query the table:

.. code-block:: python

    import aiopg8000, asyncio


    @asyncio.coroutine
    def example():
        @asyncio.coroutine
        def stream_generator():
            return (yield from asyncio.open_connection(host='localhost', port=5432, ssl=False))


        conn = yield from aiopg8000.connect(  stream_generator=stream_generator
                                            , user="postgres"
                                            , password="C.P.Snow"
                                            , database="my_example_db")
        cursor = yield from conn.cursor()
        yield from cursor.execute("CREATE TEMPORARY TABLE book (id SERIAL, title TEXT)")
        yield from cursor.execute(
            "INSERT INTO book (title) VALUES (%s), (%s) RETURNING id, title",
            ("Ender's Game", "Speaker for the Dead"))
        results = yield from cursor.fetchall()
        for row in results:
            id, title = row
            print("id = %s, title = %s" % (id, title))
        yield from conn.commit()
    asyncio.get_event_loop().run_until_complete(example())

Another query, using some PostgreSQL functions (must run in an async function to use ``yield from``):

.. code-block:: python

    yield from cursor.execute("SELECT extract(millennium from now())")
    print ((yield from cursor.fetchone())
    #[3.0]

A query that returns the PostgreSQL interval type:

.. code-block:: python

    import datetime
    yield from cursor.execute("SELECT timestamp '2013-12-01 16:06' - %s",
    # (datetime.date(1980, 4, 27),))
    print ((yield from cursor.fetchone()))
    # [datetime.timedelta(12271, 57960)]

aiopg8000 supports all the DB-API parameter styles. Here's an example of using
the 'numeric' parameter style:

.. code-block:: python

    aiopg8000.paramstyle = "numeric"
    yield from cursor.execute("SELECT array_prepend(:1, :2)", ( 500, [1, 2, 3, 4], ))
    print ((yield from cursor.fetchone()))
    #[[500, 1, 2, 3, 4]]
    aiopg8000.paramstyle = "format"
    yield from conn.rollback()

Following the DB-API specification, autocommit is off by default. It can be
turned on by using the autocommit property of the connection.

.. code-block:: python

    conn.autocommit = True
    yield from cur = conn.cursor()
    yield from cur.execute("vacuum")
    conn.autocommit = False
    yield from cursor.yield_close()

When communicating with the server, aiopg8000 uses the character set that the
server asks it to use (the client encoding). By default the client encoding is
the database's character set (chosen when the database is created), but the
client encoding can be changed in a number of ways (eg. setting
CLIENT_ENCODING in postgresql.conf). Another way of changing the client
encoding is by using an SQL command. For example:

.. code-block:: python

    cur = yield from conn.cursor()
    yield from cur.execute("SET CLIENT_ENCODING TO 'UTF8'")
    yield from cur.execute("SHOW CLIENT_ENCODING")
    yield from cur.fetchone()
    #['UTF8']
    yield from cur.close()

JSON is sent to the server serialized, and returned de-serialized. Here's an
example:

.. code-block:: python

    import json
    cur = yield from conn.cursor()
    val = ['Apollo 11 Cave', True, 26.003]
    yield from cur.execute("SELECT cast(%s as json)", (json.dumps(val),))
    print ((yield from cur.fetchone()))
    #[['Apollo 11 Cave', True, 26.003]]
    yield from cur.close()
    yield from conn.close()
