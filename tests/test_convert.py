from quasardb import test_convert as m

# Re-export all `test_convert` functions for pytest

for x in dir(m):
    if not x.endswith('_test'):
        continue

    # A bit of a hack, but qpparently Python doesn't recognize our function to be an actual
    # function, because it is a class member function and/or a native function.
    #
    # pytest uses inspect.isfunction() to collect tests, so we'll just wrap it into a lambda
    # function to satisfy that requirement
    #
    # see also: https://github.com/pybind/pybind11/issues/2262#issuecomment-655208202

    fn = getattr(m, x)
    fn_ = lambda: fn()

    globals()['test_{}'.format(x)] = lambda: fn()
