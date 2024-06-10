
#include "reader.hpp"
#include "error.hpp"
#include "table.hpp"
#include "convert/value.hpp"

namespace qdb
{

qdb::reader const & reader::enter()
{
    // Scoped capture, because we're converting to qdb_string_t which needs to be released.
    qdb::object_tracker::scoped_capture capture{_object_tracker};

    logger_.warn("opening reader for tables: %s", tables_);

    std::vector<qdb_bulk_reader_table_t> tables{};
    tables.reserve(tables_.size());

    logger_.warn("tables_size.() = %d", tables_.size());

    for (std::size_t i = 0; i < tables_.size(); ++i)
    {

        logger_.warn("filling table i=%d", i);

        qdb::table const & table_ = tables_.at(i);
        auto const & columns_     = table_.list_columns();

        // Note that this particular converter copies the string and it's tracked
        // using the object tracker.

        // Pre-allocate the data for the columns, make sure that the memory is tracked,
        // so we don't have to worry about memory loss.
        // table.column_count = columns_.size();

        // table.columns = object_tracker::alloc<qdb_string_t>(columns_.size() * sizeof(qdb_string_t));

        // for (std::size_t j = 0; j < columns_.size(); ++j)
        // {
        //     // And now we can use the same string conversion again. We're doing a const-cast
        //     // here because we just want to fill the array.
        //     //
        //     // Not the prettiest code, could be improved.

        //     logger_.warn("copying column %d with name: %s", j, columns_.at(j).name);

        //     qdb_string_t & column = const_cast<qdb_string_t &>(table.columns[j]);
        //     column                = convert::value<std::string, qdb_string_t>(columns_.at(j).name);
        // }

        // logger_.warn("converted %d columns", table.column_count);

        tables.emplace_back(qdb_bulk_reader_table_t{
            convert::value<std::string, qdb_string_t>(table_.get_name()), nullptr, 0, nullptr, 0});
    }

    for (auto const & table : tables)
    {
        logger_.warn("table with name '%s' has %d columns",
            std::string{table.name.data, table.name.length}, table.column_count);

        for (auto i = 0; i < table.column_count; ++i)
        {
            logger_.warn("table '%s' has column with name: '%s'",
                std::string{table.name.data, table.name.length},
                std::string{table.columns[i].data, table.columns[i].length});
        }
    }

    qdb::qdb_throw_if_error(
        *handle_, qdb_bulk_reader_fetch(*handle_, tables.data(), tables.size(), &reader_));

    return *this;
}

void reader::close()
{
    logger_.warn("closing reader");

    // Even though that from the API it looks like value, qdb_reader_handle_t is actually a pointer
    // itself that needs to be released. This static assert checks for that.
    static_assert(std::is_pointer<typeof(reader_)>());

    if (reader_ != nullptr)
    {
        qdb_release(*handle_, reader_);
        reader_ = nullptr;
    }
}

void register_reader(py::module_ & m)
{
    namespace py = pybind11;

    auto reader_c = py::class_<qdb::reader>{m, "Reader"};

    // basic interface
    reader_c
        .def(py::init<qdb::handle_ptr, std::vector<qdb::table> const &>(),

            py::arg("conn"),
            py::arg("tables"))                 //
        .def("__enter__", &qdb::reader::enter) //
        .def("__exit__", &qdb::reader::exit);
}

} // namespace qdb
