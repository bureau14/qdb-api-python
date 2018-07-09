#include "Python.h"

int _the_variable = 0;

int the_function()
{
    return _the_variable;
}

PyMODINIT_FUNC initnothing(void)
{
    _the_variable = 1;
}
