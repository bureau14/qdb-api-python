class QdbDaskIntegrationRequired(ImportError):
    """
    Exception raised when trying to use QuasarDB dask integration, but
    qdb-dask-connector is not installed.
    """

    pass


try:
    from qdb_dask_connector import *
except ImportError:
    raise QdbDaskIntegrationRequired(
        "QuasarDB dask integration is not installed. Please qdb-dask-connector."
    )
