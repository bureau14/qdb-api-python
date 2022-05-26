#include "utils.hpp"
#include "traits.hpp"
#include "convert/value.hpp"

namespace qdb
{

std::vector<qdb_ts_range_t> convert_ranges(py::object xs)
{
    if (xs.is_none() == true)
    {
        return {traits::qdb_value<qdb_ts_range_t>::forever()};
    };

    std::vector<qdb_ts_range_t> ret;

    // Must be a list
    auto xs_ = xs.cast<py::list>();

    ret.resize(xs_.size());
    std::transform(xs_.begin(), xs_.end(), ret.begin(), [](py::handle x) -> qdb_ts_range_t {
        return convert::value<py::tuple, qdb_ts_range_t>(py::cast<py::tuple>(x));
    });

    return ret;
};

}; // namespace qdb
