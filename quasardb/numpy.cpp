#include "numpy.hpp"
#include "convert/value.hpp"

qdb::numpy::datetime64::datetime64(qdb_timespec_t const & ts)
    : datetime64(convert::value<qdb_timespec_t, std::int64_t>(ts))
{}
