###
#
# XXX: If this import fails, ensure that you built the quasardb python API with
#      export QDB_TESTS_ENABLED=ON. E.g.
#
#      ```
#      export QDB_TESTS_ENABLED=ON
#      python3 setup.py test --addopts "-s tests/test_convert.py"
#      ```

from quasardb import test_writer_retry_options as m

def test_default_no_retry():
    m.test_default_no_retry()

def test_permutate_once():
    m.test_permutate_once()
