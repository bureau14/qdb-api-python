# pylint: disable=C0103,C0111,C0302,W0212
import datetime
import pytest
import quasardb
import sys
import os

sys.path.append(os.path.join(os.path.split(__file__)[0], '..', 'examples/'))


def test_tutorial_python():
    import tutorial.python


def test_tutorial_pandas_tutorial():
    import tutorial.pandas_tutorial
