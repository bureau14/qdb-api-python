###
# Verifies properties of our exceptions.
###

import inspect
import pytest
import quasardb
import pprint

classes = inspect.getmembers(quasardb, inspect.isclass)
exception_classes = (v for (k, v) in classes if k.endswith('Error'))
exception_tree = inspect.getclasstree(exception_classes)

def test_base_is_runtime_error():
    assert isinstance(exception_tree, list)

    # The dept should be 4; this test may fail if we increase the nesting
    # level, but I don't anticipate that happening any time soon.
    assert len(exception_tree) == 4

    # Define what we expect the hierarchy to look like
    hierarchy = [Exception, RuntimeError, quasardb.Error]

    cur = exception_tree

    def walk_and_check(xs, depth = 0):
        assert depth <= len(hierarchy)

        # Parent/child class
        if isinstance(xs, tuple):
            # We only expect one base class
            assert len(xs[1]) == 1
            derived_from = xs[1][0]
            if depth == len(hierarchy):
                assert derived_from is quasardb.Error

        # Classes derived
        elif isinstance(xs, list):
            if depth == len(hierarchy) - 1:
                return [walk_and_check(x, depth + 1) for x in xs]
            else:
                walk_and_check(xs[0], depth + 1)
                walk_and_check(xs[1], depth + 1)

    walk_and_check(cur)
