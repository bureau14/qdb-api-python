#include "conftest.hpp"
#include <module.hpp>
#include <writer.hpp>

namespace qdb
{

QDB_REGISTER_MODULE(test_writer_retry_options, m)
{
    auto m_ = m.def_submodule("test_writer_retry_options");

    m_.def("test_default_no_retry", []() -> void {
        qdb::detail::retry_options retry_options{};
        TEST_CHECK_EQUAL(retry_options.retries_left_, 0);
        TEST_CHECK_EQUAL(retry_options.has_next(), false);
    });

    m_.def("test_permutate_once", []() -> void {
        qdb::detail::retry_options retry_options{1};
        TEST_CHECK_EQUAL(retry_options.retries_left_, 1);
        TEST_CHECK_EQUAL(retry_options.has_next(), true);

        auto next_ = retry_options.next();
        TEST_CHECK_EQUAL(next_.retries_left_, 0);
        TEST_CHECK_EQUAL(next_.has_next(), false);
    });
}

}; // namespace qdb
