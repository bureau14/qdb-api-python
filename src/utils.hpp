#pragma once

#include "handle.hpp"
#include <qdb/client.h>
#include <qdb/ts.h>
#include <pybind11/numpy.h>
#include <algorithm>
#include <chrono>
#include <string>
#include <vector>

namespace qdb
{
static inline std::vector<std::string> convert_strings_and_release(qdb::handle_ptr h, const char ** ss, size_t c)
{
    std::vector<std::string> res(c);

    std::transform(ss, ss + c, res.begin(), [](const char * s) { return std::string{s}; });

    qdb_release(*h, ss);

    return res;
}

static inline size_t max_length(const qdb_ts_blob_point * points, size_t count)
{
    if (!count) return 0;

    return std::max_element(points, points + count,
        [](const qdb_ts_blob_point & left, const qdb_ts_blob_point & right) { return left.content_length < right.content_length; })
        ->content_length;
}

static inline qdb_time_t to_localtime(qdb_time_t t)
{
    struct tm * local = localtime(&t);
    return mktime(local);
}

} // namespace qdb
