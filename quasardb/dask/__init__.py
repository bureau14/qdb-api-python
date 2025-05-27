class QdbDaskIntegrationRequired(ImportError):
    """
    Exception raised when trying to use QuasarDB dask integration, but
    qdb_dask_integration has not been installed.
    """

    pass

try:
    from qdb_dask_connector import *
except ImportError:
    raise QdbDaskIntegrationRequired("QuasarDB dask integration is not installed. Please qdb-dask-connector.")
