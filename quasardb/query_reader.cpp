#include "query_reader.hpp"
#include "error.hpp"
#include "masked_array.hpp"
#include "metrics.hpp"
#include "query.hpp"
#include <algorithm>

namespace qdb
{

namespace detail
{

qdb_size_t query_reader_iterator::effective_batch_size() const noexcept
{
    assert(parent_ != nullptr);

    // A batch size of 0 means "everything in a single batch".
    return parent_->batch_size_ == 0 ? parent_->result_->row_count
                                     : static_cast<qdb_size_t>(parent_->batch_size_);
}

qdb_size_t query_reader_iterator::this_batch_row_count() const noexcept
{
    assert(parent_ != nullptr);
    assert(offset_ < parent_->result_->row_count);

    return std::min(effective_batch_size(), parent_->result_->row_count - offset_);
}

query_reader_iterator & query_reader_iterator::operator++()
{
    assert(parent_ != nullptr);

    offset_ += this_batch_row_count();

    if (offset_ >= parent_->result_->row_count)
    {
        // Reached the end: become the end sentinel.
        parent_ = nullptr;
        offset_ = 0;
    }

    return *this;
}

py::dict query_reader_iterator::operator*() const
{
    assert(parent_ != nullptr);

    qdb_query_result_t const & r = *(parent_->result_);
    qdb_size_t n                 = this_batch_row_count();

    py::dict ret{};

    // Duplicate column names collapse to one dict key; the last one wins.
    for (qdb_size_t j = 0; j < r.column_count; ++j)
    {
        // Convert with the tag probed at enter(), so an all-null batch keeps
        // the stream-wide dtype.
        qdb::masked_array xs = numpy_query_array(r.rows + offset_, n, j, parent_->column_types_[j]);

        // cast() returns an owned reference; steal it, or the array leaks.
        ret[py::str(parent_->column_names_[j])] =
            py::reinterpret_steal<py::object>(xs.cast(py::return_value_policy::move));
    }

    return ret;
}

}; // namespace detail

qdb::query_reader const & query_reader::enter()
{
    qdb_error_t err;
    {
        metrics::scoped_capture capture{"qdb_query"};
        err = qdb_query(*handle_, query_.c_str(), &result_);
    }

    qdb::qdb_throw_if_query_error(*handle_, err, result_.get());

    // DML statements return no result set: qdb_query leaves the out-param
    // NULL. Only probe when a result set exists.
    if (result_.get() != nullptr)
    {
        // Fix the stream's column names and value-type tags before the first
        // yield; all batches convert with these tags (see numpy_query_array).
        column_names_ = coerce_column_names(*result_);

        column_types_.reserve(result_->column_count);
        for (qdb_size_t j = 0; j < result_->column_count; ++j)
        {
            column_types_.push_back(probe_column_type(*result_, j));
        }
    }

    entered_ = true;

    return *this;
}

void query_reader::close()
{
    // Idempotent: called from __exit__ and again from the destructor.
    // Yielded batches are numpy-owned copies and stay valid.
    if (result_.get() != nullptr)
    {
        logger_.debug("closing query stream");
        result_.reset();
    }

    column_names_.clear();
    column_types_.clear();
    entered_ = false;

    assert(result_.get() == nullptr);
}

void register_query_reader(py::module_ & m)
{
    namespace py = pybind11;

    auto query_reader_c = py::class_<qdb::query_reader>{m, "QueryReader"};

    // basic interface
    query_reader_c
        .def(py::init([](py::args, py::kwargs) {
            throw qdb::direct_instantiation_exception{"conn.stream_query(...)"};
            return nullptr;
        }))
        .def("get_batch_size", &qdb::query_reader::get_batch_size)
        .def("__enter__", &qdb::query_reader::enter)
        .def("__exit__", &qdb::query_reader::exit)
        .def(
            "__iter__", [](qdb::query_reader & r) { return py::make_iterator(r.begin(), r.end()); },
            py::keep_alive<0, 1>());
}

} // namespace qdb
