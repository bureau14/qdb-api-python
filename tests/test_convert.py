###
#
# XXX: If this import fails, ensure that you built the quasardb python API with
#      export QDB_TESTS_ENABLED=ON. E.g.
#
#      ```
#      export QDB_TESTS_ENABLED=ON
#      python3 setup.py test --addopts "-s tests/test_convert.py"
#      ```

from quasardb import test_convert as m

#
###

from utils import assert_indexed_arrays_equal
import conftest

def test_unicode_u32_decode_traits():
    m.test_unicode_u32_decode_traits()

def test_unicode_u8_encode_traits():
    m.test_unicode_u8_encode_traits()

def test_unicode_u8_decode_traits():
    m.test_unicode_u8_decode_traits()

def test_unicode_u8_recode():
    m.test_unicode_u8_recode()

def test_unicode_decode_algo():
    m.test_unicode_decode_algo()

def _test_array_recode(array_with_index_and_table):
    (ctype, dtype, xs1, idx1, table) = array_with_index_and_table

    if dtype.char == 'S':
        # XXX(leon): we don't yet support native qdb -> np.ndarray with dtype `null-terminated binary` (S). I don't think we
        #            should ever do this, but we do need it for input. That's why we can't test this right now, because we
        #            can't do it full circle.
        return True

    (idx2, xs2) = m.test_array_recode(ctype, dtype, (idx1, xs1))
    assert_indexed_arrays_equal((idx1, xs1), (idx2, xs2))


@conftest.override_sparsify('partial')
def test_array_recode_sparsify_partial(array_with_index_and_table):
    return _test_array_recode(array_with_index_and_table)

@conftest.override_sparsify('none')
def test_array_recode_sparsify_none(array_with_index_and_table):
    return _test_array_recode(array_with_index_and_table)
