#include "table.hpp"
#include "metrics.hpp"
#include "reader.hpp"
#include <memory> // for make_unique

namespace qdb
{

namespace py = pybind11;

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

}; // namespace qdb
