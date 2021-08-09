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
