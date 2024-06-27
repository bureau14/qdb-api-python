#include "reader.hpp"
#include "error.hpp"
#include "table.hpp"
#include "traits.hpp"
#include "convert/array.hpp"
#include "convert/value.hpp"
#include "detail/qdb_resource.hpp"
#include <range/v3/view/counted.hpp>

namespace qdb
{

namespace detail
{

/* static */ py::dict reader_data::convert(qdb_bulk_reader_table_data_t const & data)
{
    py::dict ret{};

    // typedef struct
    // {
    //     qdb_size_t row_count;
    //     qdb_size_t column_count;
    //     const qdb_timespec_t * timestamps;
    //     const qdb_exp_batch_push_column_t * columns;
    // } qdb_exp_batch_push_table_data_t;

    // Convert the timestamp index, which should never contain null values
    // and thus is *not* a masked array.
    auto timestamps = ranges::views::counted(data.timestamps, data.row_count);
    auto columns    = ranges::views::counted(data.columns, data.column_count);

    py::array idx          = convert::array<qdb_timespec_t, traits::datetime64_ns_dtype>(timestamps);
    qdb::masked_array idx_ = qdb::masked_array::masked_none(idx);

    ret[py::str("$timestamp")] = idx;

    for (qdb_exp_batch_push_column_t const & column : columns)
    {
        // typedef struct // NOLINT(modernize-use-using)
        // {
        //     char const * name;
        //     qdb_ts_column_type_t data_type;
        //     union
        //     {
        //         const qdb_timespec_t * timestamps;
        //         const qdb_string_t * strings;
        //         const qdb_blob_t * blobs;
        //         const qdb_int_t * ints;
        //         const double * doubles;
        //     } data;
        // } qdb_exp_batch_push_column_t;

        py::str column_name{column.name};

        qdb::masked_array xs;
        switch (column.data_type)
        {
        case qdb_ts_column_int64:
            xs = convert::masked_array<qdb_int_t, traits::int64_dtype>(
                ranges::views::counted(column.data.ints, data.row_count));
            break;
        case qdb_ts_column_double:
            xs = convert::masked_array<double, traits::float64_dtype>(
                ranges::views::counted(column.data.doubles, data.row_count));
            break;
        case qdb_ts_column_string:
            xs = convert::masked_array<qdb_string_t, traits::unicode_dtype>(
                ranges::views::counted(column.data.strings, data.row_count));
            break;
        case qdb_ts_column_blob:
            xs = convert::masked_array<qdb_blob_t, traits::pyobject_dtype>(
                ranges::views::counted(column.data.blobs, data.row_count));
            break;
        case qdb_ts_column_timestamp:
            xs = convert::masked_array<qdb_timespec_t, traits::datetime64_ns_dtype>(
                ranges::views::counted(column.data.timestamps, data.row_count));
            break;

        case qdb_ts_column_symbol:
            // This should not happen, as "symbol" is just an internal representation, and symbols
            // are exposed to the user as strings. If this actually happens, it indicates either
            // a bug in the bulk reader *or* a memory corruption.
            throw qdb::not_implemented_exception(
                "Internal error: invalid data type: symbol column type returned from bulk reader");

        case qdb_ts_column_uninitialized:
            throw qdb::not_implemented_exception(
                "Internal error: invalid data type: unintialized column "
                "type returned from bulk reader");
        };

        ret[std::move(column_name)] = std::move(xs.cast(py::return_value_policy::move));
    }

    return ret;
}

reader_iterator & reader_iterator::operator++()
{
    if (ptr_ == nullptr)
    {
        // This means this is either the first invocation, or we have
        // previously exhausted all tables in the current "fetch" and
        // should fetch next.
        qdb_error_t err = qdb_bulk_reader_get_data(reader_, &ptr_, batch_size_);

        if (err == qdb_e_iterator_end) [[unlikely]]
        {
            // We have reached the end -- reset all our internal state, and make us look
            // like the "end" iterator.
            handle_      = nullptr;
            reader_      = nullptr;
            batch_size_  = 0;
            table_count_ = 0;
            ptr_         = nullptr;
            n_           = 0;
        }
        else
        {
            qdb::qdb_throw_if_error(*handle_, err);

            // I like assertions
            assert(handle_ != nullptr);
            assert(reader_ != nullptr);
            assert(table_count_ != 0);
            assert(ptr_ != nullptr);

            n_ = 0;
        }
    }
    else
    {
        assert(ptr_ != nullptr);

        if (++n_ == table_count_)
        {
            // We have exhausted our tables. What we will do is just "reset" our internal state
            // to how it would be after the initial constructor, and recurse into this function,
            // which should then just follow the regular flow above
            qdb_release(*handle_, ptr_);

            ptr_ = nullptr;
            n_   = 0;

            return this->operator++();
        }

        // At this point, we *must* have a valid state
        assert(ptr_ != nullptr);
        assert(n_ < table_count_);

    } // if (ptr_ == nullptr)
    return *this;
};

}; // namespace detail

qdb::reader const & reader::enter()
{
    // Very small scope, because we don't need any of the allocated memory after this function is
    // finished, so we will also release memory early.
    qdb::object_tracker::scoped_repository object_tracker{};
    qdb::object_tracker::scoped_capture capture{object_tracker};

    std::vector<qdb_bulk_reader_table_t> tables{};
    tables.reserve(table_names_.size());

    //
    // Convert columns if applicable
    //
    char const ** columns{nullptr};

    // If column names were provided, set them. Otherwise, it defaults to "all columns".
    if (column_names_.empty() == false)
    {

        // Note that this particular converter copies the string and it's tracked
        // using the object tracker.
        //
        // Pre-allocate the data for the columns, make sure that the memory is tracked,
        // so we don't have to worry about memory loss.
        columns = object_tracker::alloc<char const *>(column_names_.size() * sizeof(char const *));

        for (std::size_t i = 0; i < column_names_.size(); ++i)
        {
            // Because the scope of `column_names_` outlives this function / scope, we don't have
            // to copy the string, but can just directly use the .c_str() and things will work out.
            columns[i] = column_names_.at(i).c_str();
        }
    }

    qdb_ts_range_t * ranges{nullptr};

    if (ranges_.empty() == false)
    {
        // Pre-allocate the data for the columns, make sure that the memory is tracked,
        // so we don't have to worry about memory loss.
        ranges = object_tracker::alloc<qdb_ts_range_t>(ranges_.size() * sizeof(qdb_ts_range_t));

        for (std::size_t i = 0; i < ranges_.size(); ++i)
        {
            // This convert::value does not allocate anything on the heap
            ranges[i] = convert::value<py::tuple, qdb_ts_range_t>(ranges_.at(i));
        }
    }

    // We either have columns and have the actual array set, *or* we do not have any customized
    // columns at all.
    // Same applies for ranges
    assert((columns == nullptr) == (column_names_.empty() == true));
    assert((ranges == nullptr) == (ranges_.empty() == true));

    for (std::string const & table_name : table_names_)
    {
        tables.emplace_back(qdb_bulk_reader_table_t{
            // because the scope of `table_name` outlives this function, we can just directly
            // use .c_str() without any copies.
            table_name.c_str(),   //
            columns,              //
            column_names_.size(), //
            ranges,               //
            ranges_.size()        //
        });
    }

    qdb::qdb_throw_if_error(
        *handle_, qdb_bulk_reader_fetch(*handle_, tables.data(), tables.size(), &reader_));

    return *this;
}

void reader::close()
{
    // Even though that from the API it looks like value, qdb_reader_handle_t is actually a pointer
    // itself that needs to be released. This static assert checks for that.
    static_assert(std::is_pointer<decltype(reader_)>());

    if (reader_ != nullptr)
    {
        logger_.debug("closing reader");
        qdb_release(*handle_, reader_);
        reader_ = nullptr;
    }

    assert(reader_ == nullptr);
}

void register_reader(py::module_ & m)
{
    namespace py = pybind11;

    auto reader_c = py::class_<qdb::reader>{m, "Reader"};

    // basic interface
    reader_c
        .def(py::init<                                                                         //
                 qdb::handle_ptr,                                                              //
                 std::vector<std::string> const &,                                             //
                 std::vector<std::string> const &,                                             //
                 std::size_t,                                                                  //
                 std::vector<py::tuple> const &>(),                                            //
            py::arg("conn"),                                                                   //
            py::arg("table_names"),                                                            //
            py::kw_only(),                                                                     //
            py::arg("column_names") = std::vector<std::string>{},                              //
            py::arg("batch_size")   = std::size_t{0},                                          //
            py::arg("ranges")       = std::vector<py::tuple>{}                                 //
            )                                                                                  //
                                                                                               //
        .def("get_batch_size", &qdb::reader::get_batch_size)                                   //
                                                                                               //
        .def("__enter__", &qdb::reader::enter)                                                 //
        .def("__exit__", &qdb::reader::exit)                                                   //
        .def(                                                                                  //
            "__iter__", [](qdb::reader & r) { return py::make_iterator(r.begin(), r.end()); }, //
            py::keep_alive<0, 1>());

    //
}

} // namespace qdb
