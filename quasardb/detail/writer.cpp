#include "writer.hpp"
#include "../convert/array.hpp"
#include "concepts.hpp"
#include "dispatch.hpp"
#include "numpy.hpp"
#include "retry.hpp"
#include "traits.hpp"

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

        ret.name      = nullptr;
        ret.data_type = ctype;

        // We swap the pointer inside the `qdb_exp_batch_push_column_t` with the pointer
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
        column.name                     = column_name.c_str();

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
    batch.name = _table_name.c_str();

    prepare_table_data(batch.data);
    if (mode == qdb_exp_batch_push_truncate)
    {
        batch.truncate_ranges      = ranges;
        batch.truncate_range_count = ranges == nullptr ? 0u : 1u;
    }

    // Zero-initialize these
    batch.where_duplicate       = nullptr;
    batch.where_duplicate_count = 0;
    batch.deduplication_mode    = qdb_exp_batch_deduplication_mode_disabled;
    batch.creation              = qdb_exp_batch_dont_create;

    enum detail::deduplication_mode_t mode_ = deduplicate_options.mode_;

    std::visit(
        [&mode_, &batch](auto const & columns) { _set_deduplication_mode(mode_, columns, batch); },
        deduplicate_options.columns_);
}

/* static */ py::kwargs batch_push_mode::ensure(py::kwargs kwargs)
{
    if (kwargs.contains(kw_push_mode) == false)
        [[unlikely]] // Our pandas/numpy adapters always set this, so unlikely
    {

        kwargs[batch_push_mode::kw_push_mode] = batch_push_mode::default_push_mode;
    }

    return kwargs;
}

/* static */ py::kwargs batch_push_mode::set(py::kwargs kwargs, qdb_exp_batch_push_mode_t push_mode)
{
    assert(kwargs.contains(kw_push_mode) == false);

    kwargs[batch_push_mode::kw_push_mode] = push_mode;

    return kwargs;
}

/* static */ qdb_exp_batch_push_mode_t batch_push_mode::from_kwargs(py::kwargs const & kwargs)
{
    assert(kwargs.contains(batch_push_mode::kw_push_mode));

    return py::cast<qdb_exp_batch_push_mode_t>(kwargs[batch_push_mode::kw_push_mode]);
}

/* static */ py::kwargs detail::batch_push_flags::ensure(py::kwargs kwargs)
{
    if (kwargs.contains(batch_push_flags::kw_write_through) == false)
    {
        kwargs[batch_push_flags::kw_write_through] = batch_push_flags::default_write_through;
    }

    return kwargs;
}

/* static */ qdb_uint_t detail::batch_push_flags::from_kwargs(py::kwargs const & kwargs)
{
    assert(kwargs.contains(batch_push_flags::kw_write_through));

    // By default no flags are set
    qdb_uint_t ret = qdb_exp_batch_push_flag_none;

    try
    {
        bool write_through = py::cast<bool>(kwargs["write_through"]);

        // Likely as it's the default, if not, this static assert should fail:
        static_assert(batch_push_flags::default_write_through == true);
        if (write_through == true) [[likely]]
        {
            ret |= static_cast<qdb_uint_t>(qdb_exp_batch_push_flag_write_through);
        }
    }
    catch (py::cast_error const & /*e*/)
    {
        std::string error_msg = "Invalid argument provided for `write_through`: expected bool, got: ";
        error_msg += py::str(py::type::of(kwargs["write_through"])).cast<std::string>();

        throw qdb::invalid_argument_exception{error_msg};
    }

    return ret;
}

/* static */ qdb_exp_batch_options_t detail::batch_options::from_kwargs(py::kwargs const & kwargs)
{
    auto kwargs_ = detail::batch_push_mode::ensure(kwargs);

    return {
        .mode       = detail::batch_push_mode::from_kwargs(kwargs_), //
        .push_flags = detail::batch_push_flags::from_kwargs(kwargs_) //
    };
}

/* static */ detail::deduplicate_options detail::deduplicate_options::from_kwargs(py::kwargs args)
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

/* static */ staged_tables staged_tables::index(detail::writer_data const & data)
{
    // XXX(leon): this function could potentially be moved to e.g. a free
    // function as it doesn't really depend upon anything in writer, but
    // then we'll have to globally declare staged_tables_t and it gets a
    // bit messy.

    detail::staged_tables ret;

    for (detail::writer_data::value_type const & table_data : data.xs())
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

        detail::staged_table & staged_table = ret.get_or_create(table);

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

    return ret;
}
}; // namespace qdb::detail
