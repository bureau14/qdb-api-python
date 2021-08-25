#pragma once

#include <qdb/ts.h>

template <typename Ostream>
Ostream & operator<<(Ostream & os, qdb_timespec_t timestamp)
{
    os << "(" << timestamp.tv_sec << "," << timestamp.tv_nsec << ")";
    return os;
}

template <typename Ostream>
Ostream & operator<<(Ostream & os, qdb_ts_range_t range)
{
    os << "[" << range.begin << "," << range.end << "]";
    return os;
}