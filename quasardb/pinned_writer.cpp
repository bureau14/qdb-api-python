#include "pinned_writer.hpp"
#include "concepts.hpp"
#include "dispatch.hpp"
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
        // pinned_writer class, and as such will survive for a long time.
        prepare_column_of_type<ctype>(std::get<column_type>(input), ret);

        return ret;
    };
};

// Handle symbols as if they are strings. Data should never be provided using a "symbol" type
// anyway, but it's good to handle
template <>
struct fill_column_dispatch<qdb_ts_column_symbol> : fill_column_dispatch<qdb_ts_column_string>
{};

}; // namespace qdb::detail

namespace qdb
{

void pinned_writer::set_index(py::handle const & xs)
{
    qdb::object_tracker::scoped_capture capture{_object_tracker};
    convert::array<traits::datetime64_ns_dtype, qdb_timespec_t>(
        numpy::array::ensure<traits::datetime64_ns_dtype>(xs), _index);
}

void pinned_writer::set_blob_column(std::size_t index, const masked_array & xs)
{
    qdb::object_tracker::scoped_capture capture{_object_tracker};
    detail::set_column_dispatch<qdb_ts_column_blob>(index, xs, _columns);
}

void pinned_writer::set_string_column(std::size_t index, const masked_array & xs)
{
    qdb::object_tracker::scoped_capture capture{_object_tracker};
    detail::set_column_dispatch<qdb_ts_column_string>(index, xs, _columns);
}

void pinned_writer::set_int64_column(std::size_t index, const masked_array_t<traits::int64_dtype> & xs)
{
    qdb::object_tracker::scoped_capture capture{_object_tracker};
    detail::set_column_dispatch<qdb_ts_column_int64>(index, xs, _columns);
}

void pinned_writer::set_double_column(
    std::size_t index, const masked_array_t<traits::float64_dtype> & xs)
{
    qdb::object_tracker::scoped_capture capture{_object_tracker};
    detail::set_column_dispatch<qdb_ts_column_double>(index, xs, _columns);
}

void pinned_writer::set_timestamp_column(
    std::size_t index, const masked_array_t<traits::datetime64_ns_dtype> & xs)
{
    qdb::object_tracker::scoped_capture capture{_object_tracker};
    detail::set_column_dispatch<qdb_ts_column_timestamp>(index, xs, _columns);
}

std::vector<qdb_exp_batch_push_column_t> const & pinned_writer::prepare_columns()
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

void pinned_writer::push(py::kwargs args)
{
    _push_impl(qdb_exp_batch_push_transactional, _deduplicate_from_args(args));
}

void pinned_writer::push_async(py::kwargs args)
{
    _push_impl(qdb_exp_batch_push_async, _deduplicate_from_args(args));
}

void pinned_writer::push_fast(py::kwargs args)
{
    _push_impl(qdb_exp_batch_push_fast, _deduplicate_from_args(args));
}

void pinned_writer::push_truncate(py::kwargs args)
{
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
    if (empty()) [[unlikely]]
    {
        throw qdb::invalid_argument_exception{
            "Pinned writer is empty: you did not provide any rows to push."};
    }

    qdb_ts_range_t tr{_index.front(), _index.back()};
    if (args.contains("range"))
    {
        tr = convert::value<py::tuple, qdb_ts_range_t>(py::cast<py::tuple>(args["range"]));
    }
    else
    {
        // our range is end-exclusive, so let's move the pointer one nanosecond
        // *after* the last element in this batch.
        tr.end.tv_nsec++;
    }
    _push_impl(qdb_exp_batch_push_truncate, deduplicate, &tr);
}

void register_pinned_writer(py::module_ & m)
{
    namespace py = pybind11;

    auto c = py::class_<qdb::pinned_writer>{m, "PinnedWriter"};

    // basic interface
    c.def(py::init<qdb::handle_ptr, const table &>())           //
        .def("column_infos", &qdb::pinned_writer::column_infos) //
        .def("empty", &qdb::pinned_writer::empty,               //
            "Returns true when the writer has no data");

    c.def_readwrite("_legacy_state", &qdb::pinned_writer::legacy_state_);

    // numpy-based / "pinned" api
    c.def("set_index", &qdb::pinned_writer::set_index)                    //
        .def("set_blob_column", &qdb::pinned_writer::set_blob_column)     //
        .def("set_string_column", &qdb::pinned_writer::set_string_column) //
        .def("set_double_column", &qdb::pinned_writer::set_double_column) //
        .def("set_int64_column", &qdb::pinned_writer::set_int64_column)   //
        .def("set_timestamp_column", &qdb::pinned_writer::set_timestamp_column);

    // push functions
    c.def("push", &qdb::pinned_writer::push, "Regular batch push") //
        .def("push_async", &qdb::pinned_writer::push_async,
            "Asynchronous batch push that buffers data inside the QuasarDB daemon") //
        .def("push_fast", &qdb::pinned_writer::push_fast,
            "Fast, in-place batch push that is efficient when doing lots of small, incremental "
            "pushes.") //
        .def("push_truncate", &qdb::pinned_writer::push_truncate,
            "Before inserting data, truncates any existing data. This is useful when you want your "
            "insertions to be idempotent, e.g. in "
            "case of a retry.");
}

}; // namespace qdb
