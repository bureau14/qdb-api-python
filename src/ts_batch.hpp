#pragma once

#include "entry.hpp"
#include "ts.hpp"

namespace qdb
{

struct batch_column_info
{
    batch_column_info() = default;
    batch_column_info(const std::string & ts_name, const std::string & col_name, qdb_size_t size_hint = 0)
        : timeseries{ts_name}
        , column{col_name}
        , elements_count_hint{size_hint}
    {}

    operator qdb_ts_batch_column_info_t() const noexcept
    {
        qdb_ts_batch_column_info_t res;

        res.timeseries          = timeseries.c_str();
        res.column              = column.c_str();
        res.elements_count_hint = elements_count_hint;
        return res;
    }

    std::string timeseries;
    std::string column;
    qdb_size_t elements_count_hint{0};
};

class ts_batch
{

public:
    ts_batch(qdb::handle_ptr h, const std::vector<batch_column_info> & ci)
        : _handle{h}
    {
        std::vector<qdb_ts_batch_column_info_t> converted(ci.size());

        std::transform(
            ci.cbegin(), ci.cend(), converted.begin(), [](const batch_column_info & ci) -> qdb_ts_batch_column_info_t { return ci; });

        QDB_THROW_IF_ERROR(qdb_ts_batch_table_init(*_handle, converted.data(), converted.size(), &_batch_table));
    }

    // prevent copy because of the table object, use a unique_ptr of the batch in cluster
    // to return the object
    ts_batch(const ts_batch &) = delete;

    ~ts_batch()
    {
        if (_handle && _batch_table)
        {
            qdb_release(*_handle, _batch_table);
            _batch_table = nullptr;
        }
    }

public:
    void set_blob_column(
        const std::string & alias, const std::string & column, const pybind11::array & timestamps, const pybind11::array & values)
    {
        const auto points = convert_values<qdb_ts_blob_point, const char *>{}(timestamps, values);
        QDB_THROW_IF_ERROR(qdb_ts_batch_add_blob(_batch_table, alias.c_str(), column.c_str(), points.data(), points.size()));
    }

    void set_double_column(
        const std::string & alias, const std::string & column, const pybind11::array & timestamps, const pybind11::array_t<double> & values)
    {
        const auto points = convert_values<qdb_ts_double_point, double>{}(timestamps, values);
        QDB_THROW_IF_ERROR(qdb_ts_batch_add_double(_batch_table, alias.c_str(), column.c_str(), points.data(), points.size()));
    }

    void set_int64_column(const std::string & alias,
        const std::string & column,
        const pybind11::array & timestamps,
        const pybind11::array_t<std::int64_t> & values)
    {
        const auto points = convert_values<qdb_ts_int64_point, std::int64_t>{}(timestamps, values);
        QDB_THROW_IF_ERROR(qdb_ts_batch_add_int64(_batch_table, alias.c_str(), column.c_str(), points.data(), points.size()));
    }

    void set_timestamp_column(const std::string & alias,
        const std::string & column,
        const pybind11::array & timestamps,
        const pybind11::array_t<std::int64_t> & values)
    {
        const auto points = convert_values<qdb_ts_timestamp_point, std::int64_t>{}(timestamps, values);
        QDB_THROW_IF_ERROR(qdb_ts_batch_add_timestamp(_batch_table, alias.c_str(), column.c_str(), points.data(), points.size()));
    }

public:
    void append_blob(const std::string & blob)
    {
        QDB_THROW_IF_ERROR(qdb_ts_batch_row_set_blob(_batch_table, blob.data(), blob.size()));
    }

    void append_double(double v)
    {
        QDB_THROW_IF_ERROR(qdb_ts_batch_row_set_double(_batch_table, v));
    }

    void append_int64(std::int64_t v)
    {
        QDB_THROW_IF_ERROR(qdb_ts_batch_row_set_int64(_batch_table, v));
    }

    void append_timestamp(std::int64_t v)
    {
        const qdb_timespec_t converted = convert_timestamp(v);
        QDB_THROW_IF_ERROR(qdb_ts_batch_row_set_timestamp(_batch_table, &converted));
    }

    void finalize_row(std::int64_t ts)
    {
        const qdb_timespec_t converted = convert_timestamp(ts);
        QDB_THROW_IF_ERROR(qdb_ts_batch_row_finalize(_batch_table, &converted));
    }

    void push()
    {
        QDB_THROW_IF_ERROR(qdb_ts_batch_push(_batch_table));
    }

private:
    qdb::handle_ptr _handle;
    qdb_batch_table_t _batch_table{nullptr};
};

// don't use shared_ptr, let Python do the reference counting, otherwise you will have an undefined behavior
using ts_batch_ptr = std::unique_ptr<ts_batch>;

template <typename Module>
static inline void register_ts_batch(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::batch_column_info>{m, "BatchColumnInfo"}                                 //
        .def(py::init<const std::string &, const std::string &, qdb_size_t>(),               //
            py::arg("ts_name"),                                                              //
            py::arg("col_name"),                                                             //
            py::arg("size_hint") = 0)                                                        //
        .def_readwrite("timeseries", &qdb::batch_column_info::timeseries)                    //
        .def_readwrite("column", &qdb::batch_column_info::column)                            //
        .def_readwrite("elements_count_hint", &qdb::batch_column_info::elements_count_hint); //

    py::class_<qdb::ts_batch>{m, "TimeSeriesBatch"}                               //
        .def(py::init<qdb::handle_ptr, const std::vector<batch_column_info> &>()) //
        .def("set_blob_column", &qdb::ts_batch::set_blob_column)                  //
        .def("set_double_column", &qdb::ts_batch::set_double_column)              //
        .def("set_int64_column", &qdb::ts_batch::set_int64_column)                //
        .def("set_timestamp_column", &qdb::ts_batch::set_timestamp_column)        //
        .def("append_blob", &qdb::ts_batch::append_blob)                          //
        .def("append_double", &qdb::ts_batch::append_double)                      //
        .def("append_int64", &qdb::ts_batch::append_int64)                        //
        .def("append_timestamp", &qdb::ts_batch::append_timestamp)                //
        .def("finalize_row", &qdb::ts_batch::finalize_row)                        //
        .def("push", &qdb::ts_batch::push);                                       //
}

} // namespace qdb
