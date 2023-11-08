#include "writer.hpp"
#include "concepts.hpp"
#include "dispatch.hpp"
#include "metrics.hpp"
#include "numpy.hpp"
#include "traits.hpp"
#include "convert/array.hpp"

namespace qdb::detail
{

////////////////////////////////////////////////////////////////////////////////
//
// COLUMN SETTERS
//
// Effectively we delegate these to the appropriate convert:: function. Add an
// additional converter here if you're adding additional column types, *or* wish
// to support additional dtypes for existing column types (e.g. to also support
// py::str objects for string columns).
//
///////////////////

template <qdb_ts_column_type_t ColumnType, concepts::dtype Dtype>
struct column_setter;

#define COLUMN_SETTER_DECL(CTYPE, DTYPE, VALUE_TYPE)                                        \
    template <>                                                                             \
    struct column_setter<CTYPE, DTYPE>                                                      \
    {                                                                                       \
        inline void operator()(qdb::masked_array const & xs, std::vector<VALUE_TYPE> & dst) \
        {                                                                                   \
            convert::masked_array<DTYPE, VALUE_TYPE>(xs, dst);                              \
        }                                                                                   \
    };

// np.dtype('int64') -> qdb_int_t column
COLUMN_SETTER_DECL(qdb_ts_column_int64, traits::int64_dtype, qdb_int_t);

// np.dtype('int32') -> qdb_int_t column
COLUMN_SETTER_DECL(qdb_ts_column_int64, traits::int32_dtype, qdb_int_t);
// np.dtype('int16') -> qdb_int_t column
COLUMN_SETTER_DECL(qdb_ts_column_int64, traits::int16_dtype, qdb_int_t);

// np.dtype('float64') -> double column
COLUMN_SETTER_DECL(qdb_ts_column_double, traits::float64_dtype, double);
// np.dtype('float32') -> double column
COLUMN_SETTER_DECL(qdb_ts_column_double, traits::float32_dtype, double);

// np.dtype('datetime64[ns]') -> qdb_timespec_t column
COLUMN_SETTER_DECL(qdb_ts_column_timestamp, traits::datetime64_ns_dtype, qdb_timespec_t);

// np.dtype('unicode') -> qdb_string_T column
COLUMN_SETTER_DECL(qdb_ts_column_string, traits::unicode_dtype, qdb_string_t);

// np.dtype('object') -> qdb_blob_t column
COLUMN_SETTER_DECL(qdb_ts_column_blob, traits::pyobject_dtype, qdb_blob_t);

// np.dtype('S') -> qdb_blob_t column
COLUMN_SETTER_DECL(qdb_ts_column_blob, traits::bytestring_dtype, qdb_blob_t);

#undef COLUMN_SETTER_DECL

template <qdb_ts_column_type_t ColumnType>
inline void set_column_dispatch(
    std::size_t index, qdb::masked_array const & xs, std::vector<any_column> & columns)
{
    dispatch::by_dtype<detail::column_setter, ColumnType>(
        xs.dtype(), xs, detail::access_column<ColumnType>(columns, index));
};

template <qdb_ts_column_type_t T, typename AnyColumnType>
inline void prepare_column_of_type(AnyColumnType const & in, qdb_exp_batch_push_column_t & out);

template <>
inline void prepare_column_of_type<qdb_ts_column_int64>(
    int64_column const & in, qdb_exp_batch_push_column_t & out)
{
    out.data.ints = in.data();
};

template <>
inline void prepare_column_of_type<qdb_ts_column_double>(
    double_column const & in, qdb_exp_batch_push_column_t & out)
{
    out.data.doubles = in.data();
};

template <>
inline void prepare_column_of_type<qdb_ts_column_timestamp>(
    timestamp_column const & in, qdb_exp_batch_push_column_t & out)
{
    out.data.timestamps = in.data();
};

template <>
inline void prepare_column_of_type<qdb_ts_column_blob>(
    blob_column const & in, qdb_exp_batch_push_column_t & out)
{
    out.data.blobs = in.data();
};

template <>
inline void prepare_column_of_type<qdb_ts_column_string>(
    string_column const & in, qdb_exp_batch_push_column_t & out)
{
    out.data.strings = in.data();
};

template <>
inline void prepare_column_of_type<qdb_ts_column_symbol>(
    string_column const & in, qdb_exp_batch_push_column_t & out)
{
    prepare_column_of_type<qdb_ts_column_string>(in, out);
};

template <qdb_ts_column_type_t ctype>
struct fill_column_dispatch
{
    using value_type  = typename traits::qdb_column<ctype>::value_type;
    using column_type = typename column_of_type<ctype>::value_type;

    inline qdb_exp_batch_push_column_t operator()(any_column const & input)
    {
        qdb_exp_batch_push_column_t ret{};

        ret.name      = traits::null_value<qdb_string_t>();
        ret.data_type = ctype;

        // We swap the pointer inside the `qdb_exp_batch_push_column__t` with the pointer
        // to the data as it is inside the `any_column`.
        //
        // This works, because all these `any_column`'s lifecycle is scoped to the
        // writer class, and as such will survive for a long time.
        prepare_column_of_type<ctype>(std::get<column_type>(input), ret);

        return ret;
    };
};

// Handle symbols as if they are strings. Data should never be provided using a "symbol" type
// anyway, but it's good to handle
template <>
struct fill_column_dispatch<qdb_ts_column_symbol> : fill_column_dispatch<qdb_ts_column_string>
{};

void staged_table::set_index(py::array const & xs)
{
    convert::array<traits::datetime64_ns_dtype, qdb_timespec_t>(
        numpy::array::ensure<traits::datetime64_ns_dtype>(xs), _index);
}

void staged_table::set_blob_column(std::size_t index, const masked_array & xs)
{
    detail::set_column_dispatch<qdb_ts_column_blob>(index, xs, _columns);
}

void staged_table::set_string_column(std::size_t index, const masked_array & xs)
{
    detail::set_column_dispatch<qdb_ts_column_string>(index, xs, _columns);
}

void staged_table::set_int64_column(std::size_t index, const masked_array_t<traits::int64_dtype> & xs)
{
    detail::set_column_dispatch<qdb_ts_column_int64>(index, xs, _columns);
}

void staged_table::set_double_column(
    std::size_t index, const masked_array_t<traits::float64_dtype> & xs)
{
    detail::set_column_dispatch<qdb_ts_column_double>(index, xs, _columns);
}

void staged_table::set_timestamp_column(
    std::size_t index, const masked_array_t<traits::datetime64_ns_dtype> & xs)
{
    detail::set_column_dispatch<qdb_ts_column_timestamp>(index, xs, _columns);
}

std::vector<qdb_exp_batch_push_column_t> const & staged_table::prepare_columns()
{
    _columns_data.clear();
    _columns_data.reserve(_columns.size());

    for (size_t index = 0; index < _columns.size(); ++index)
    {
        qdb_exp_batch_push_column_t column = dispatch::by_column_type<detail::fill_column_dispatch>(
            _column_infos[index].type, _columns.at(index));

        assert(traits::is_null(column.name));

        // XXX(leon): reuses lifecycle of _column_infos[index], which *should* be fine,
        //            but ensuring we take a reference is super important here!
        std::string const & column_name = _column_infos[index].name;
        column.name                     = qdb_string_t{column_name.data(), column_name.size()};

#ifndef NDEBUG
        // Symbol column is actually string data
        qdb_ts_column_type_t expected_data_type =
            (_column_infos[index].type == qdb_ts_column_symbol ? qdb_ts_column_string
                                                               : _column_infos[index].type);
        assert(column.data_type == expected_data_type);
#endif

        _columns_data.push_back(column);
    }

    return _columns_data;
}

void staged_table::prepare_table_data(qdb_exp_batch_push_table_data_t & table_data)
{
    table_data.row_count  = _index.size();
    table_data.timestamps = _index.data();

    const auto & columns    = prepare_columns();
    table_data.columns      = columns.data();
    table_data.column_count = columns.size();
}

void staged_table::prepare_batch(qdb_exp_batch_push_mode_t mode,
    detail::deduplicate_options const & deduplicate_options,
    qdb_ts_range_t * ranges,
    qdb_exp_batch_push_table_t & batch)
{
    batch.name = qdb_string_t{_table_name.data(), _table_name.size()};

    prepare_table_data(batch.data);
    if (mode == qdb_exp_batch_push_truncate)
    {
        batch.truncate_ranges      = ranges;
        batch.truncate_range_count = ranges == nullptr ? 0u : 1u;
    }

    // Zero-initialize these
    batch.where_duplicate       = nullptr;
    batch.where_duplicate_count = 0;
    batch.options               = qdb_exp_batch_option_standard;

    enum detail::deduplication_mode_t mode_ = deduplicate_options.mode_;

    std::visit([&mode_, &batch](auto const & columns) { _set_push_options(mode_, columns, batch); },
        deduplicate_options.columns_);
}

}; // namespace qdb::detail

namespace qdb
{

void writer::data::append(
    qdb::table const & table, py::handle const & index, py::list const & column_data)
{
    py::array index_ = numpy::array::ensure<traits::datetime64_ns_dtype>(index);

    /**
     * Additional check that all the data is actually of the same length, and data has been
     * provided for each and every column.
     */
    if (column_data.size() != table.list_columns().size())
    {
        throw qdb::invalid_argument_exception{"data must be provided for every table column"};
    }

    for (py::handle const & data : column_data)
    {
        qdb::masked_array data_ = data.cast<qdb::masked_array>();
        if (data_.size() != static_cast<std::size_t>(index_.size()))
        {
            throw qdb::invalid_argument_exception{
                "every data array should be exactly the same length as the index array"};
        }
    }

    xs_.push_back(value_type{table, index_, column_data});
}

void writer::push(writer::data const & data, py::kwargs args)
{
    qdb::object_tracker::scoped_capture capture{_object_tracker};
    staged_tables_t staged_tables = _stage_tables(data);

    _push_impl(staged_tables, qdb_exp_batch_push_transactional, _deduplicate_from_args(args));
}

void writer::push_async(writer::data const & data, py::kwargs args)
{
    qdb::object_tracker::scoped_capture capture{_object_tracker};
    staged_tables_t staged_tables = _stage_tables(data);

    _push_impl(staged_tables, qdb_exp_batch_push_async, _deduplicate_from_args(args));
}

void writer::push_fast(writer::data const & data, py::kwargs args)
{
    qdb::object_tracker::scoped_capture capture{_object_tracker};
    staged_tables_t staged_tables = _stage_tables(data);

    _push_impl(staged_tables, qdb_exp_batch_push_fast, _deduplicate_from_args(args));
}

void writer::push_truncate(writer::data const & data, py::kwargs args)
{
    qdb::object_tracker::scoped_capture capture{_object_tracker};
    staged_tables_t staged_tables = _stage_tables(data);

    auto deduplicate = _deduplicate_from_args(args);

    // Sanity check, this should be checked for in the python-side of things as well,
    // but people can invoke this manually if they want.
    if (!std::holds_alternative<bool>(deduplicate.columns_)
        || std::get<bool>(deduplicate.columns_) != false) [[unlikely]]
    {
        throw qdb::invalid_argument_exception{"Cannot set `deduplicate` for push_truncate."};
    };

    // As we are actively removing data, let's add an additional check to ensure the user
    // doesn't accidentally truncate his whole database without inserting anything.
    if (data.empty()) [[unlikely]]
    {
        throw qdb::invalid_argument_exception{"Writer is empty: you did not provide any rows to push."};
    }

    qdb_ts_range_t tr;

    if (args.contains("range"))
    {
        tr = convert::value<py::tuple, qdb_ts_range_t>(py::cast<py::tuple>(args["range"]));
    }
    else
    {
        // TODO(leon): support multiple tables for push truncate
        if (staged_tables.size() != 1) [[unlikely]]
        {
            throw qdb::invalid_argument_exception{
                "Writer push truncate only supports a single "
                "table unless an explicit range is provided: you provided more than one table without"
                " an explicit range."};
        }

        detail::staged_table const & staged_table = staged_tables.cbegin()->second;
        tr                                        = staged_table.time_range();
    }

    _push_impl(staged_tables, qdb_exp_batch_push_truncate, deduplicate, &tr);
}

detail::deduplicate_options writer::_deduplicate_from_args(py::kwargs args)
{
    if (!args.contains("deduplicate") || !args.contains("deduplication_mode"))
    {
        return {};
    }

    std::string deduplication_mode = args["deduplication_mode"].cast<std::string>();

    enum detail::deduplication_mode_t deduplication_mode_;
    if (deduplication_mode == "drop")
    {
        deduplication_mode_ = detail::deduplication_mode_drop;
    }
    else if (deduplication_mode == "upsert")
    {
        deduplication_mode_ = detail::deduplication_mode_upsert;
    }
    else
    {
        std::string error_msg = "Invalid argument provided for `deduplication_mode`: expected "
                                "'drop' or 'upsert', got: ";
        error_msg += deduplication_mode;

        throw qdb::invalid_argument_exception{error_msg};
    }

    auto deduplicate = args["deduplicate"];

    if (py::isinstance<py::list>(deduplicate))
    {
        return detail::deduplicate_options{
            deduplication_mode_, py::cast<std::vector<std::string>>(deduplicate)};
    }
    else if (py::isinstance<py::bool_>(deduplicate))
    {
        return detail::deduplicate_options{deduplication_mode_, py::cast<bool>(deduplicate)};
    }

    std::string error_msg = "Invalid argument provided for `deduplicate`: expected bool, list or "
                            "str('$timestamp'), got: ";
    error_msg += deduplicate.cast<py::str>();

    throw qdb::invalid_argument_exception{error_msg};
};

/* static */ writer::staged_tables_t writer::_stage_tables(writer::data const & data)
{
    staged_tables_t staged_tables;

    for (writer::data::value_type const & table_data : data.xs())
    {
        qdb::table table     = table_data.table;
        py::array index      = table_data.index;
        py::list column_data = table_data.column_data;

        auto column_infos = table.list_columns();

        if (column_infos.size() != column_data.size()) [[unlikely]]
        {
            throw qdb::invalid_argument_exception{
                "data must be provided for every column of the table."};
        }

        detail::staged_table & staged_table = writer::_get_staged_table(table, staged_tables);

        staged_table.set_index(index);

        for (std::size_t i = 0; i < column_data.size(); ++i)
        {
            py::object x = column_data[i];

            if (!x.is_none()) [[likely]]
            {
                switch (column_infos.at(i).type)
                {
                case qdb_ts_column_double:
                    staged_table.set_double_column(
                        i, x.cast<qdb::masked_array_t<traits::float64_dtype>>());
                    break;
                case qdb_ts_column_blob:
                    staged_table.set_blob_column(i, x.cast<qdb::masked_array>());
                    break;
                case qdb_ts_column_int64:
                    staged_table.set_int64_column(
                        i, x.cast<qdb::masked_array_t<traits::int64_dtype>>());
                    break;
                case qdb_ts_column_timestamp:
                    staged_table.set_timestamp_column(
                        i, x.cast<qdb::masked_array_t<traits::datetime64_ns_dtype>>());
                    break;
                case qdb_ts_column_string:
                    /* FALLTHROUGH */
                case qdb_ts_column_symbol:
                    staged_table.set_string_column(i, x.cast<qdb::masked_array>());
                    break;
                case qdb_ts_column_uninitialized:
                    // Likely a corruption
                    throw qdb::invalid_argument_exception{"Uninitialized column."};

                    break;
                    // Likely a corruption
                default:
                    throw qdb::invalid_argument_exception{"Unrecognized column type."};
                }
            }
        }
    }

    return staged_tables;
}

void writer::_push_impl(writer::staged_tables_t & staged_tables,
    qdb_exp_batch_push_mode_t mode,
    detail::deduplicate_options deduplicate_options,
    qdb_ts_range_t * ranges)
{
    _handle->check_open();

    if (staged_tables.empty())
    {
        throw qdb::invalid_argument_exception{"No data written to batch writer."};
    }

    std::vector<qdb_exp_batch_push_table_t> batch;
    batch.assign(staged_tables.size(), qdb_exp_batch_push_table_t());

    int cur = 0;
    _logger.debug("writer::_push_impl");

    for (auto pos = staged_tables.begin(); pos != staged_tables.end(); ++pos)
    {
        std::string const & table_name      = pos->first;
        detail::staged_table & staged_table = pos->second;
        auto & batch_table                  = batch.at(cur++);

        staged_table.prepare_batch(mode, deduplicate_options, ranges, batch_table);

        if (batch_table.data.column_count == 0) [[unlikely]]
        {
            throw qdb::invalid_argument_exception{
                "Writer is empty: you did not provide any columns to push."};
        }

        _logger.debug("Pushing %d rows with %d columns in %s", batch_table.data.row_count,
            batch_table.data.column_count, table_name);
    }

    // Make sure to measure the time it takes to do the actual push
    qdb::metrics::scoped_capture capture{"qdb_batch_push"};

    qdb::qdb_throw_if_error(
        *_handle, qdb_exp_batch_push(*_handle, mode, batch.data(), nullptr, batch.size()));
}

void register_writer(py::module_ & m)
{
    namespace py = pybind11;

    // Writer data
    auto writer_data_c = py::class_<qdb::writer::data>{m, "WriterData"};
    writer_data_c.def(py::init())
        .def("append", &qdb::writer::data::append, py::arg("table"), py::arg("index"),
            py::arg("column_data"), "Append new data")
        .def("empty", &qdb::writer::data::empty, "Returns true if underlying data is empty");

    // And the actual pinned writer
    auto writer_c = py::class_<qdb::writer>{m, "Writer"};

    // basic interface
    writer_c.def(py::init<qdb::handle_ptr>()); //

    writer_c.def_readwrite("_legacy_state", &qdb::writer::legacy_state_);

    // push functions
    writer_c
        .def("push", &qdb::writer::push, "Regular batch push") //
        .def("push_async", &qdb::writer::push_async,
            "Asynchronous batch push that buffers data inside the QuasarDB daemon") //
        .def("push_fast", &qdb::writer::push_fast,
            "Fast, in-place batch push that is efficient when doing lots of small, incremental "
            "pushes.") //
        .def("push_truncate", &qdb::writer::push_truncate,
            "Before inserting data, truncates any existing data. This is useful when you want your "
            "insertions to be idempotent, e.g. in "
            "case of a retry.");
}
}; // namespace qdb
