#include "table.hpp"
#include "dispatch.hpp"
#include "metrics.hpp"
#include "object_tracker.hpp"
#include "reader.hpp"
#include "traits.hpp"
#include "convert/point.hpp"
#include <memory> // for make_unique

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
            handle->check_open();                                                           \
                                                                                            \
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

void table::_cache_metadata() const
{
    _handle->check_open();

    detail::qdb_resource<qdb_ts_metadata_t> metadata{*_handle};

    qdb_error_t err;

    {
        metrics::scoped_capture("qdb_ts_get_metadata");
        err = qdb_ts_get_metadata(*_handle, _alias.c_str(), &metadata);
    }

    if (err == qdb_e_alias_not_found) [[unlikely]]
    {
        // Can happen if table does not yet exist, do nothing.
        return;
    }

    qdb::qdb_throw_if_error(*_handle, err);

    _columns = detail::convert_columns(metadata->columns, metadata->column_count);

    if (metadata->ttl == qdb_ttl_disabled)
    {
        _ttl = std::chrono::milliseconds{0};
    }
    else
    {
        _ttl = std::chrono::milliseconds{metadata->ttl};
    }

    _shard_size = std::chrono::milliseconds{metadata->shard_size};
}

qdb_uint_t table::erase_ranges(const std::string & column, py::object ranges)
{
    _handle->check_open();

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
    _handle->check_open();

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
    _handle->check_open();

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
    _handle->check_open();

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
    _handle->check_open();

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
    _handle->check_open();

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

qdb::reader_ptr table::reader(                     //
    std::vector<std::string> const & column_names, //
    std::size_t batch_size,                        //
    std::vector<py::tuple> const & ranges) const
{
    std::vector<std::string> table_names{get_name()};
    return std::make_unique<qdb::reader>(_handle, table_names, column_names, batch_size, ranges);
};

}; // namespace qdb
