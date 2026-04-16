/*
 *
 * Official Python API
 *
 * Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *    * Neither the name of quasardb nor the names of its contributors may
 *      be used to endorse or promote products derived from this software
 *      without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY QUASARDB AND CONTRIBUTORS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#pragma once

#include "entry.hpp"
#include "masked_array.hpp"
#include "reader_fwd.hpp"
#include "table_fwd.hpp"
#include "detail/ts_column.hpp"

namespace qdb
{

class table : public entry
{
public:
    table(handle_ptr h, std::string a)
        : entry{h, a}
        , _has_indexed_columns(false)
    {
        _cache_metadata();
    }

public:
    std::string repr() const
    {
        return "<quasardb.Table name='" + get_name() + "'>";
    }

    /**
     * Retrieves (and caches) table metadata. This mainly involves column information.
     */
    void retrieve_metadata()
    {
        _cache_metadata();
    }

    void create(const std::vector<detail::column_info> & columns,
        std::chrono::milliseconds shard_size = std::chrono::hours{24},
        std::chrono::milliseconds ttl        = std::chrono::milliseconds::zero())
    {
        _handle->check_open();

        qdb_duration_t ttl_ = qdb_ttl_disabled;
        if (ttl != std::chrono::milliseconds::zero())
        {
            ttl_ = ttl.count();
        }

        const auto c_columns = detail::convert_create_columns_ex(columns);
        qdb::qdb_throw_if_error(*_handle, qdb_ts_create_ex(*_handle, _alias.c_str(), shard_size.count(),
                                              c_columns.data(), c_columns.size(), ttl_));
    }

    void insert_columns(const std::vector<detail::column_info> & columns)
    {
        _handle->check_open();

        const auto c_columns = detail::convert_columns_ex(columns);
        qdb::qdb_throw_if_error(*_handle,
            qdb_ts_insert_columns_ex(*_handle, _alias.c_str(), c_columns.data(), c_columns.size()));
    }

    std::vector<detail::column_info> const & list_columns() const
    {
        if (_columns.has_value()) [[likely]]
        {
            return _columns.value();
        }

        _cache_metadata();

        if (_columns.has_value()) [[likely]]
        {
            return _columns.value();
        }

        throw qdb::alias_not_found_exception{};
    }

    detail::indexed_column_info column_info_by_id(const std::string & alias) const
    {
        if (_has_indexed_columns == false)
        {
            // It's important to note that if additional columns are added during
            // the lifetime of this object, we will not pick up on this in our cache.
            _indexed_columns     = detail::index_columns(list_columns());
            _has_indexed_columns = true;
        }

        detail::indexed_columns_t::const_iterator i = _indexed_columns.find(alias);
        if (i == _indexed_columns.end())
            throw qdb::exception{qdb_e_out_of_bounds, std::string("Column not found: ") + alias};

        return i->second;
    }

    detail::column_info column_info_by_index(qdb_size_t idx) const
    {
        auto cols = list_columns();

        // Not strictly necessary, but avoids an "ugly" exception being thrown when
        // using .at() below.
        if (cols.size() <= idx) [[unlikely]]
        {
            throw qdb::exception{
                qdb_e_out_of_bounds, std::string("Column index out of bounds: ") + std::to_string(idx)};
        }

        return cols.at(idx);
    }

    std::string column_id_by_index(qdb_size_t idx) const
    {
        return column_info_by_index(idx).name;
    }

    qdb_ts_column_type_t column_type_by_index(qdb_size_t idx) const
    {
        return column_info_by_index(idx).type;
    }

    qdb_size_t column_index_by_id(const std::string & alias) const
    {
        return column_info_by_id(alias).index;
    }

    qdb_ts_column_type_t column_type_by_id(const std::string & alias) const
    {
        return column_info_by_id(alias).type;
    }

    qdb::reader_ptr reader(                            //
        std::vector<std::string> const & column_names, //
        std::size_t batch_size,                        //
        std::vector<py::tuple> const & ranges) const;

    /**
     * Returns true if this table has a TTL assigned.
     */
    inline bool has_ttl() const
    {
        if (_ttl.has_value()) [[likely]]
        {
            return _ttl.value() != std::chrono::milliseconds::zero();
        }

        _cache_metadata();

        if (_ttl.has_value()) [[likely]]
        {
            return _ttl.value() != std::chrono::milliseconds::zero();
        }

        throw qdb::alias_not_found_exception{};
    }

    inline std::chrono::milliseconds get_ttl() const
    {
        if (_ttl.has_value()) [[likely]]
        {
            return _ttl.value();
        }

        _cache_metadata();

        if (_ttl.has_value()) [[likely]]
        {
            return _ttl.value();
        }

        throw qdb::alias_not_found_exception{};
    }

    inline std::chrono::milliseconds get_shard_size() const
    {
        if (_shard_size.has_value()) [[likely]]
        {
            return _shard_size.value();
        }

        _cache_metadata();

        if (_shard_size.has_value()) [[likely]]
        {
            return _shard_size.value();
        }

        throw qdb::alias_not_found_exception{};
    }

public:
    /**
     * Ensures that all provided tables have an identical schema.
     */
    void ensure_identical_schema();

    /**
     * Given a connection and a Python object, attempts to coerce into
     * a table object.
     *
     * If the Python object is a string, it interprets it as the table name,
     * otherwise it assumed the argument is an actual table object.
     */
    static qdb::table coerce_table(py::object);

private:
    /**
     * Loads column info / metadata from server and caches it locally.
     */
    void _cache_metadata() const;

    /**
     * Loads column info / metadata from server if not yet cached locally.
     */
    void _maybe_cache_metadata() const
    {
        if (_columns.has_value() == false) [[unlikely]]
        {
            // We expect _ttl and _columns and _shard_size (i.e. all metadata) to have the same state
            assert(_ttl.has_value() == false);
            assert(_shard_size.has_value() == false);
            _cache_metadata();
        }
    }

public:
    py::object subscribe(py::object conn)
    {
        auto firehose = py::module::import("quasardb.firehose");
        auto xs       = firehose.attr("subscribe")(conn, get_name());
        return xs;
    }

private:
    mutable bool _has_indexed_columns;
    mutable detail::indexed_columns_t _indexed_columns;

    mutable std::optional<std::vector<detail::column_info>> _columns;
    mutable std::optional<std::chrono::milliseconds> _ttl;
    mutable std::optional<std::chrono::milliseconds> _shard_size;
};

static inline table_ptr make_table_ptr(handle_ptr handle, std::string table_name)
{
    return std::make_unique<table>(handle, table_name);
}

template <typename Module>
static inline void register_table(Module & m)
{
    namespace py = pybind11;

    py::enum_<qdb_ts_column_type_t>{m, "ColumnType", py::arithmetic(), "Column type"}
        .value("Uninitialized", qdb_ts_column_uninitialized)
        .value("Double", qdb_ts_column_double)
        .value("Blob", qdb_ts_column_blob)
        .value("String", qdb_ts_column_string)
        .value("Symbol", qdb_ts_column_symbol)
        .value("Int64", qdb_ts_column_int64)
        .value("Timestamp", qdb_ts_column_timestamp);

    py::class_<qdb::table, qdb::entry>{m, "Table", "Table representation"}
        .def(py::init([](py::args, py::kwargs) {
            throw qdb::direct_instantiation_exception{"conn.table(...)"};
            return nullptr;
        }))
        .def("__repr__", &qdb::table::repr)
        .def("create", &qdb::table::create, //
            py::arg("columns"),             //
            py::arg("shard_size") = std::chrono::hours{24},
            py::arg("ttl")        = std::chrono::milliseconds::zero())
        .def("retrieve_metadata", &qdb::table::retrieve_metadata)
        .def("column_index_by_id", &qdb::table::column_index_by_id)
        .def("column_type_by_id", &qdb::table::column_type_by_id)
        .def("column_info_by_index", &qdb::table::column_info_by_index)
        .def("column_type_by_index", &qdb::table::column_type_by_index)
        .def("column_id_by_index", &qdb::table::column_id_by_index)
        .def("insert_columns", &qdb::table::insert_columns)
        .def("list_columns", &qdb::table::list_columns)
        .def("has_ttl", &qdb::table::has_ttl)
        .def("get_ttl", &qdb::table::get_ttl)
        .def("get_shard_size", &qdb::table::get_shard_size)

        .def("reader", &qdb::table::reader,                       //
            py::kw_only(),                                        //
            py::arg("column_names") = std::vector<std::string>{}, //
            py::arg("batch_size")   = std::size_t{0},             //
            py::arg("ranges")       = std::vector<py::tuple>{})

        .def("subscribe", &qdb::table::subscribe);
}

} // namespace qdb
