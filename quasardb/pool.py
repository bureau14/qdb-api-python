import logging
import quasardb

logger = logging.getLogger('quasardb.pool')

class Session(quasardb.Cluster):
    def __init__(self, pool, uri=None, user_name=None, user_private_key=None, cluster_public_key=None):
        self.pool = pool
        print("uri = {}".format(uri))
        super().__init__(uri=uri)

    def close(self):
        self.pool.release(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class Factory(object):
    def __init__(self, **kwargs):
        self.args = kwargs

    def create(self):
        return Session(**self.args)

class Pool(object):
    """
    Connection pool
    """
    def __init__(self, size=1, uri=None, user_name=None, user_private_key=None, cluster_public_key=None):
        self.size    = size
        self.factory = Factory(pool=self,
                               uri=uri,
                               user_name=user_name,
                               user_private_key=user_private_key,
                               cluster_public_key=cluster_public_key)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def connect(self):
        """
        Acquire a new connection from the pool.
        """
        return self.factory.create()

    def release(self, conn):
        """
        """
        print("releasing conn: {}".format(conn))

def _inject_pool_arg(arg, args, kwargs):
    """
    Decorator utility function. Takes the argument provided to the decorator
    that configures how we should inject the pool into the args to the callback
    function, and injects it in the correct place.
    """
    pool = 'Test'

    if isinstance(arg, int):
        # Invoked positional such as `@with_pool(arg=1)`, put the pool into the
        # correct position.
        #
        # Because positional args are always a tuple, and tuples don't have an
        # easy 'insert into position' function, we just cast to and from a list.
        args = list(args)
        args.insert(arg, pool)
        args = tuple(args)
    else:
        assert isinstance(arg, str) == True
        # If not a number, we assume it's a kwarg, which makes things easier
        kwargs[arg] = pool

    return (args, kwargs)


def with_pool(_fn=None, *, arg=0):
    def inner(fn):
        def wrapper(*args, **kwargs):
            print("before decorator, kwargs = {}".format(kwargs))
            (args, kwargs) = _inject_pool_arg(arg, args, kwargs)
            result = fn(*args, **kwargs)
            print("after decorator")
            return result

        return wrapper

    if _fn is None:
        return inner
    else:
        return inner(_fn)
