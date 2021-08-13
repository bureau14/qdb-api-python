import logging
import quasardb
import threading
import functools
import weakref

logger = logging.getLogger('quasardb.pool')

def _create_conn(**kwargs):
    return quasardb.Cluster(**kwargs)

class SessionWrapper(object):
    def __init__(self, pool, conn):
        self._conn = conn
        self._pool = pool

    def __getattr__(self, attr):
        # This hack copies all the quasardb.Cluster() properties, functions and
        # whatnot, and pretends that this class is actually a quasardb.Cluster.
        #
        # The background is that when someone does this:
        #
        # with pool.connect() as conn:
        #   ...
        #
        # we don't want the the connection to be closed near the end, but rather
        # released back onto the pool.
        #
        # Now, my initial approach was to build a pool.Session class which inherited
        # from quasardb.Cluster, and just overload the __exit__ function there. But,
        # we want people to have the flexibility to pass in an external `get_conn` callback
        # in the pool, which establishes a connection, because they may have to look up
        # some dynamic security tokens. This function should then, in turn, return a vanilla
        # quasardb.Cluster() object.
        #
        # And this is why we end up with the solution below.
        if attr in self.__dict__:
            return getattr(self, attr)

        return getattr(self._conn, attr)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._pool.release(self._conn)

class Pool(object):
    """
    A connection pool. This class should not be initialized directly, but
    rather the subclass `SingletonPool` should be initialized.

    The constructor either accepts all regular `quasardb.Cluster()` connection parameters,
    or a `get_conn` parameter which is invoked any time a new connection should be
    created.

    Example usage:
    --------------

    Initialize the pool by passthrough of `quasardb.Cluster()` arguments:
    ```
    import quasardb.pool as pool

    pool.SingletonPool(uri='qdb://127.0.0.1:2836',
                       cluster_public_key='...',
                       user_private_key='...',
                       user_name='...')
    ```

    Initialize pool by providing a callback function
    ```
    import quasardb
    import quasardb.pool as pool

    def my_qdb_connection_create():
       # This function is invoked any time the pool needs to allocate
       # a new connection.
       return quasardb.Cluster(uri='qdb://127.0.0.1:2836',
                               cluster_public_key='...',
                               user_private_key='...',
                               user_name='...')

    pool.SingletonPool(get_conn=my_qdb_connection_create)
    ```

    """

    def __init__(self, get_conn=None, **kwargs):
        self._all_connections = []

        if get_conn is None:
            get_conn = functools.partial(_create_conn, **kwargs)

        if not callable(get_conn):
            raise TypeError("get_conn must be callable")

        self._get_conn = get_conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _create_conn(self):
        return SessionWrapper(self, self._get_conn())

    def close(self):
        """
        Close this connection pool, and all associated connections. This function
        is automatically invoked when used in a with-block or when using the global
        `instance()` singleton.
        """
        for conn in self._all_connections:
            logger.debug("closing connection {}".format(conn))
            conn.close()

    def connect(self) -> quasardb.Cluster:
        """
        Acquire a new connection from the pool. Returned connection must either
        be explicitly released using `pool.release()`, or should be wrapped in a
        with-block.

        Returns:
        --------
        `quasardb.Cluster`
        """
        logger.info("Acquiring connection from pool")
        return self._do_connect()

    def release(self, conn: quasardb.Cluster):
        """
        Put a connection back into the pool
        """
        logger.info("Putting connection back onto pool")
        return self._do_release(conn)

class SingletonPool(Pool):
    """
    Implementation of our connection pool that maintains just a single connection
    per thread.

    Example usage:
    --------

    Using pool.connect() in a with-block:
    ```
    import quasardb.pool as pool

    with pool.Pool(uri='qdb://127.0.0.1:2836') as pool:
      with pool.connect() as conn:
        conn.query(...)
    ```

    Explicitly releasing the connection using `Pool.release()`:
    ```
    import quasardb.pool as pool

    with pool.Pool(uri='qdb://127.0.0.1:2836') as pool:
      conn = pool.connect()
      try:
        conn.query(...)
      finally:
        pool.release(conn)
    ```
    """

    def __init__(self, **kwargs):
        Pool.__init__(self, **kwargs)
        self._conn = threading.local()

    def _do_connect(self):
        try:
            c = self._conn.current()
            if c:
                return c
        except AttributeError:
            pass

        c = self._create_conn()
        self._conn.current = weakref.ref(c)
        self._all_connections.append(c)

        return c

    def _do_release(self, conn):
        # Thread-local connections do not have to be 'released'.
        pass


__instance = None

def initialize(*args, **kwargs):
    """
    Initialize a new global SingletonPool. Forwards all arguments to the constructor of
    `SingletonPool()`.

    After initialization, the instance can be used by invoking `instance()`.

    Example usage:
    --------------

    ```
    import quasardb.pool as pool

    pool.initialize(cluster='qdb://127.0.0.1:2836')

    # ...

    def myfunction()
      with pool.instance().connect() as conn:
        conn.query(...)
    ```
    """
    global __instance
    __instance = SingletonPool(*args, **kwargs)

def instance() -> SingletonPool:
    """
    Singleton accessor. Instance must have been initialized using initialize()
    prior to invoking this function.

    Returns:
    --------
    `SingletonPool`

    """
    global __instance
    assert __instance is not None, "Global connection pool uninitialized: please initialize by calling the initialize() function."
    return __instance

def _inject_conn_arg(conn, arg, args, kwargs):
    """
    Decorator utility function. Takes the argument provided to the decorator
    that configures how we should inject the pool into the args to the callback
    function, and injects it in the correct place.
    """
    if isinstance(arg, int):
        # Invoked positional such as `@with_pool(arg=1)`, put the pool into the
        # correct position.
        #
        # Because positional args are always a tuple, and tuples don't have an
        # easy 'insert into position' function, we just cast to and from a list.
        args = list(args)
        args.insert(arg, conn)
        args = tuple(args)
    else:
        assert isinstance(arg, str) == True
        # If not a number, we assume it's a kwarg, which makes things easier
        kwargs[arg] = conn

    return (args, kwargs)

def with_conn(_fn=None, *, arg=0):
    """
    Decorator function that handles connection assignment, release and invocation for you.
    Should be used in conjuction with the global singleton accessor, see also: `initialize()`.

    By default, the decorator function injects the connection as the first argument to the
    function:
    ```
    import quasardb.pool as pool
    pool.initialize(...)

    @pool.with_conn()
    def myfunction(conn, arg1, arg2):
       conn.query(...)
    ```

    You can optionally provide an `arg` parameter to denote which named keyword to provide
    it as:
    ```
    import quasardb.pool as pool
    pool.initialize(...)

    @pool.with_conn(arg='conn')
    def myfunction(arg1, arg2, conn=None):
       conn.query(...)
    ```
    """
    def inner(fn):
        def wrapper(*args, **kwargs):
            pool = instance()

            with pool.connect() as conn:
                (args, kwargs) = _inject_conn_arg(conn, arg, args, kwargs)
                return fn(*args, **kwargs)

        return wrapper

    if _fn is None:
        return inner
    else:
        return inner(_fn)
