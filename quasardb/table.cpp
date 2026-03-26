#include "table.hpp"
#include "dispatch.hpp"
#include "metrics.hpp"
#include "object_tracker.hpp"
#include "reader.hpp"
#include "traits.hpp"
#include "writer.hpp"
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

qdb::reader_ptr table::reader(                     //
    std::vector<std::string> const & column_names, //
    std::size_t batch_size,                        //
    std::vector<py::tuple> const & ranges) const
{
    std::vector<std::string> table_names{get_name()};
    return std::make_unique<qdb::reader>(_handle, table_names, column_names, batch_size, ranges);
};

std::unique_ptr<qdb::writer> table::writer() const
{
    _handle->check_open();
    return std::make_unique<qdb::writer>(_handle);
}

}; // namespace qdb
