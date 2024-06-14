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

py::str reader_data::repr()
{
    // Inefficient but easy, and it lets the user "see" that this object is usable as
    // a regular dict (because we're pretending to be that anyway);
    py::dict tmp;

    for (auto const & tuple : data_)
    {
        tmp[py::str(tuple.first)] = tuple.second.cast(py::return_value_policy::automatic);
    }

    return py::repr(tmp);
};

py::object reader_data::get(std::string const & column_name) const
{

    py::object ret;
    auto iter = data_.find(column_name);
    if (iter != data_.end()) [[likely]]
    {
        ret = iter->second.cast(py::return_value_policy::reference);
    }
    else
    {
        // Not found
        throw new py::key_error("column not found: " + column_name);
    }

    return ret;
}

void reader_data::_assign_data(qdb_bulk_reader_table_data_t const & data)
{
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

    // Reserve two additional, one for timestamp, and one for table
    data_.reserve(ranges::size(columns) + 2);

    py::array idx          = convert::array<qdb_timespec_t, traits::datetime64_ns_dtype>(timestamps);
    qdb::masked_array idx_ = qdb::masked_array::masked_none(idx);

    auto ret = data_.insert(std::make_tuple(py::str("$timestamp"), std::move(idx_)));

    for (qdb_exp_batch_push_column_t const & column : columns)
    {
        // typedef struct // NOLINT(modernize-use-using)
        // {
        //     qdb_string_t name;
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

        std::string column_name = convert::value<qdb_string_t, std::string>(column.name);

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

        auto ret = data_.insert(std::make_tuple(std::move(column_name), std::move(xs)));

        // Extra validation that we didn't overwrite data -- if this fails, it means that either
        // we're not cleaning ourselves up properly, or the bulk reader is returning data for the
        // same column and table twice.
        assert(ret.second == true);
    }
}

reader_iterator & reader_iterator::operator++()
{
    if (ptr_ == nullptr)
    {
        assert(n_ == 0);

        // This means this is either the first invocation, or we have
        // previously exhausted all tables in the current "fetch" and
        // should fetch next.
        qdb_error_t err = qdb_bulk_reader_get_data(reader_, &ptr_, 0);

        if (err == qdb_e_iterator_end) [[unlikely]]
        {
            assert(ptr_ == nullptr);

            // We have reached the end -- reset all our internal state, and make us look
            // like the "end" iterator.
            handle_      = nullptr;
            reader_      = nullptr;
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
    // Scoped capture, because we're converting to qdb_string_t which needs to be released.
    qdb::object_tracker::scoped_capture capture{object_tracker_};

    std::vector<qdb_bulk_reader_table_t> tables{};
    tables.reserve(tables_.size());

    for (std::size_t i = 0; i < tables_.size(); ++i)
    {

        qdb::table const & table_ = tables_.at(i);
        auto const & columns_     = table_.list_columns();

        // Note that this particular converter copies the string and it's tracked
        // using the object tracker.
        //
        // Pre-allocate the data for the columns, make sure that the memory is tracked,
        // so we don't have to worry about memory loss.

        qdb_string_t * columns =
            object_tracker::alloc<qdb_string_t>(columns_.size() * sizeof(qdb_string_t));

        for (std::size_t j = 0; j < columns_.size(); ++j)
        {
            // This convert::value allocates memory on the heap and is tracked using
            // the object_tracker_ / scoped_capture
            columns[j] = convert::value<std::string, qdb_string_t>(columns_.at(j).name);
        }

        // :TODO(leon): support ranges
        tables.emplace_back(
            qdb_bulk_reader_table_t{convert::value<std::string, qdb_string_t>(table_.get_name()),
                columns, columns_.size(), nullptr, 0});
    }

    qdb::qdb_throw_if_error(
        *handle_, qdb_bulk_reader_fetch(*handle_, tables.data(), tables.size(), &reader_));

    return *this;
}

void reader::close()
{
    // Even though that from the API it looks like value, qdb_reader_handle_t is actually a pointer
    // itself that needs to be released. This static assert checks for that.
    static_assert(std::is_pointer<typeof(reader_)>());

    if (reader_ != nullptr)
    {
        logger_.warn("closing reader");
        qdb_release(*handle_, reader_);
        reader_ = nullptr;
    }
}

void register_reader(py::module_ & m)
{
    namespace py = pybind11;

    auto reader_c      = py::class_<qdb::reader>{m, "Reader"};
    auto reader_data_c = py::class_<qdb::detail::reader_data>{m, "ReaderData"};

    // basic interface
    reader_c
        .def(py::init<qdb::handle_ptr, std::vector<qdb::table> const &>(),

            py::arg("conn"),
            py::arg("tables"))                 //
        .def("__enter__", &qdb::reader::enter) //
        .def("__exit__", &qdb::reader::exit)   //
        .def(
            "__iter__", [](qdb::reader & r) { return py::make_iterator(r.begin(), r.end()); },
            py::keep_alive<0, 1>());

    reader_data_c
        .def("__repr__", &detail::reader_data::repr)                                       //
        .def("__getitem__", &detail::reader_data::get, py::return_value_policy::reference) //
        .def("get", &detail::reader_data::get, py::return_value_policy::reference)         //

        ;

    //
}

} // namespace qdb
