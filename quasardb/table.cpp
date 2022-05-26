#include "table.hpp"
#include "dispatch.hpp"
#include "object_tracker.hpp"
#include "table_reader.hpp"
#include "traits.hpp"
#include "convert/point.hpp"

namespace qdb
{

namespace py = pybind11;

namespace detail
{

template <typename Type>
using point_type = typename traits::qdb_value<Type>::point_type;

template <qdb_ts_column_type_t, concepts::dtype DType>
struct column_inserter;

#define COLUMN_INSERTER_DECL(CTYPE, DTYPE, VALUE_TYPE, FN)                                  \
    template <>                                                                             \
    struct column_inserter<CTYPE, DTYPE>                                                    \
    {                                                                                       \
        inline void operator()(handle_ptr handle,                                           \
            std::string const & table,                                                      \
            std::string const & column,                                                     \
            pybind11::array const & timestamps,                                             \
            qdb::masked_array const & values)                                               \
        {                                                                                   \
            qdb::object_tracker::scoped_repository ctx{};                                   \
            qdb::object_tracker::scoped_capture capture{ctx};                               \
            numpy::array::ensure<traits::datetime64_ns_dtype>(timestamps);                  \
            auto xs = convert::point_array<DTYPE, VALUE_TYPE>(timestamps, values);          \
                                                                                            \
            qdb::qdb_throw_if_error(                                                        \
                *handle, FN(*handle, table.c_str(), column.c_str(), xs.data(), xs.size())); \
        };                                                                                  \
    };

COLUMN_INSERTER_DECL(qdb_ts_column_int64, traits::int64_dtype, qdb_int_t, qdb_ts_int64_insert);
COLUMN_INSERTER_DECL(qdb_ts_column_int64, traits::int32_dtype, qdb_int_t, qdb_ts_int64_insert);
COLUMN_INSERTER_DECL(qdb_ts_column_int64, traits::int16_dtype, qdb_int_t, qdb_ts_int64_insert);
COLUMN_INSERTER_DECL(qdb_ts_column_double, traits::float64_dtype, double, qdb_ts_double_insert);
COLUMN_INSERTER_DECL(qdb_ts_column_double, traits::float32_dtype, double, qdb_ts_double_insert);

COLUMN_INSERTER_DECL(
    qdb_ts_column_timestamp, traits::datetime64_ns_dtype, qdb_timespec_t, qdb_ts_timestamp_insert);
COLUMN_INSERTER_DECL(qdb_ts_column_string, traits::unicode_dtype, qdb_string_t, qdb_ts_string_insert);
COLUMN_INSERTER_DECL(qdb_ts_column_blob, traits::pyobject_dtype, qdb_blob_t, qdb_ts_blob_insert);
COLUMN_INSERTER_DECL(qdb_ts_column_blob, traits::bytestring_dtype, qdb_blob_t, qdb_ts_blob_insert);

#undef COLUMN_INSERTER_DECL

template <qdb_ts_column_type_t ColumnType>
inline void insert_column_dispatch(handle_ptr handle,
    std::string const & table,
    std::string const & column,
    pybind11::array const & timestamps,
    qdb::masked_array const & values)
{
    dispatch::by_dtype<column_inserter, ColumnType>(
        values.dtype(), handle, table, column, timestamps, values);
};

}; // namespace detail

py::object table::reader(
    const std::vector<std::string> & columns, py::object ranges, bool dict_mode) const
{
    auto ranges_ = qdb::convert_ranges(ranges);

    std::vector<detail::column_info> c_columns;

    if (columns.empty())
    {
        // This is a kludge, because technically a table can have no columns, and we're
        // abusing it as "no argument provided". It's a highly exceptional use case, and
        // doesn't really have any implication in practice (we just look up twice), so it
        // should be ok.
        c_columns = list_columns();
    }
    else
    {
        c_columns.reserve(columns.size());
        // This transformation can probably be optimized, but it's only invoked when constructing
        // the reader so it's unlikely to be a performance bottleneck.
        std::transform(std::cbegin(columns), std::cend(columns), std::back_inserter(c_columns),
            [this](const auto & col) {
                const auto & info = column_info_by_id(col);
                return detail::column_info{info.type, col, info.symtable};
            });
    }

    return (dict_mode == true
                ? py::cast(qdb::table_reader<reader::ts_dict_row>(_handle, _alias, c_columns, ranges_),
                    py::return_value_policy::move)
                : py::cast(qdb::table_reader<reader::ts_fast_row>(_handle, _alias, c_columns, ranges_),
                    py::return_value_policy::move));
}

qdb_uint_t table::erase_ranges(const std::string & column, py::object ranges)
{
    auto ranges_ = qdb::convert_ranges(ranges);

    qdb_uint_t erased_count = 0;

    qdb::qdb_throw_if_error(*_handle, qdb_ts_erase_ranges(*_handle, _alias.c_str(), column.c_str(),
                                          ranges_.data(), ranges_.size(), &erased_count));

    return erased_count;
}

// insert_ranges
void table::blob_insert(
    const std::string & column, const pybind11::array & timestamps, const qdb::masked_array & values)
{
    detail::insert_column_dispatch<qdb_ts_column_blob>(_handle, _alias, column, timestamps, values);
}

void table::string_insert(
    const std::string & column, const pybind11::array & timestamps, qdb::masked_array const & values)
{
    detail::insert_column_dispatch<qdb_ts_column_string>(_handle, _alias, column, timestamps, values);
}

void table::double_insert(
    const std::string & column, const pybind11::array & timestamps, qdb::masked_array const & values)
{
    detail::insert_column_dispatch<qdb_ts_column_double>(_handle, _alias, column, timestamps, values);
}

void table::int64_insert(
    const std::string & column, const pybind11::array & timestamps, qdb::masked_array const & values)
{
    detail::insert_column_dispatch<qdb_ts_column_int64>(_handle, _alias, column, timestamps, values);
}

void table::timestamp_insert(
    const std::string & column, const pybind11::array & timestamps, const qdb::masked_array & values)
{
    detail::insert_column_dispatch<qdb_ts_column_timestamp>(
        _handle, _alias, column, timestamps, values);
}

// get_ranges

std::pair<pybind11::array, masked_array> table::blob_get_ranges(
    const std::string & column, py::object ranges)
{
    qdb_ts_blob_point * points = nullptr;
    qdb_size_t count           = 0;

    auto ranges_ = qdb::convert_ranges(ranges);

    qdb::qdb_throw_if_error(*_handle, qdb_ts_blob_get_ranges(*_handle, _alias.c_str(), column.c_str(),
                                          ranges_.data(), ranges_.size(), &points, &count));

    auto ret = convert::point_array<qdb_blob_t, traits::pyobject_dtype>(points, count);

    qdb_release(*_handle, points);

    return ret;
}

std::pair<pybind11::array, masked_array> table::string_get_ranges(
    const std::string & column, py::object ranges)
{
    qdb_ts_string_point * points = nullptr;
    qdb_size_t count             = 0;

    auto ranges_ = qdb::convert_ranges(ranges);

    qdb::qdb_throw_if_error(*_handle, qdb_ts_string_get_ranges(*_handle, _alias.c_str(), column.c_str(),
                                          ranges_.data(), ranges_.size(), &points, &count));

    auto ret = convert::point_array<qdb_string_t, traits::unicode_dtype>(points, count);

    qdb_release(*_handle, points);

    return ret;
}

std::pair<pybind11::array, masked_array> table::double_get_ranges(
    const std::string & column, py::object ranges)
{
    qdb_ts_double_point * points = nullptr;
    qdb_size_t count             = 0;

    auto ranges_ = qdb::convert_ranges(ranges);

    qdb::qdb_throw_if_error(*_handle, qdb_ts_double_get_ranges(*_handle, _alias.c_str(), column.c_str(),
                                          ranges_.data(), ranges_.size(), &points, &count));

    auto ret = convert::point_array<double, traits::float64_dtype>(points, count);

    qdb_release(*_handle, points);

    return ret;
}

std::pair<pybind11::array, masked_array> table::int64_get_ranges(
    const std::string & column, py::object ranges)
{
    qdb_ts_int64_point * points = nullptr;
    qdb_size_t count            = 0;

    auto ranges_ = qdb::convert_ranges(ranges);

    qdb::qdb_throw_if_error(*_handle, qdb_ts_int64_get_ranges(*_handle, _alias.c_str(), column.c_str(),
                                          ranges_.data(), ranges_.size(), &points, &count));

    auto ret = convert::point_array<qdb_int_t, traits::int64_dtype>(points, count);

    qdb_release(*_handle, points);

    return ret;
}

std::pair<pybind11::array, masked_array> table::timestamp_get_ranges(
    const std::string & column, py::object ranges)
{
    qdb_ts_timestamp_point * points = nullptr;
    qdb_size_t count                = 0;

    auto ranges_ = qdb::convert_ranges(ranges);

    qdb::qdb_throw_if_error(
        *_handle, qdb_ts_timestamp_get_ranges(*_handle, _alias.c_str(), column.c_str(), ranges_.data(),
                      ranges_.size(), &points, &count));

    auto ret = convert::point_array<qdb_timespec_t, traits::datetime64_ns_dtype>(points, count);

    qdb_release(*_handle, points);

    return ret;
}
}; // namespace qdb
