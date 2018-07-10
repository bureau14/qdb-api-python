#include "Python.h"

int _the_variable = 0;

int the_function()
{
    return _the_variable;
}

#if PY_MAJOR_VERSION == 2
PyMODINIT_FUNC initnothing(void)
{
    _the_variable = 1;
}
#endif

#if PY_MAJOR_VERSION == 3
PyMODINIT_FUNC PyInit_nothing(void)
{
    _the_variable = 1;
    return NULL;
}
#endif
