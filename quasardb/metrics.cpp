#include "metrics.hpp"
#include <mutex>

namespace qdb
{

static metrics_container_t metrics_totals_ = metrics_container_t{};
static std::mutex metrics_lock_            = std::mutex{};

metrics::scoped_capture::~scoped_capture()
{
    using std::chrono::nanoseconds;

    time_point_t stop = clock_t::now();

    auto duration = std::chrono::duration_cast<nanoseconds>(stop - start_);

    metrics::record(test_id_, duration.count());
}

metrics::measure::measure()
    : start_{metrics::totals()}
{}

metrics_container_t metrics::measure::get() const
{
    metrics_container_t cur = metrics::totals();

    metrics_container_t ret{};

    for (auto i : cur)
    {
        assert(ret.find(i.first) == ret.end());

        metrics_container_t::const_iterator prev = start_.find(i.first);

        if (prev == start_.end())
        {
            // Previously, metric didn't exist yet, as such it's entirely new
            // and all accumulated time was within the scope.
            ret.emplace(i.first, i.second);
        }
        else if (i.second > prev->second)
        {
            // Accumulated time actually increased, record difference
            ret.emplace(i.first, i.second - prev->second);
        }
        else
        {
            // Integrity check: we can only increase totals over time, never decrease
            assert(i.second == prev->second);
        }
    };

    return ret;
};

/* static */ void metrics::record(std::string const & test_id, std::uint64_t nsec)
{
    std::lock_guard<std::mutex> guard(metrics_lock_);

    metrics_container_t::iterator pos = metrics_totals_.lower_bound(test_id);

    if (pos == metrics_totals_.end() || pos->first != test_id) [[unlikely]]
    {
        pos = metrics_totals_.emplace_hint(pos, test_id, 0);

        assert(pos->second == 0);
    }

    assert(pos->first == test_id);
    pos->second += nsec;
}

/* static */ metrics_container_t metrics::totals()
{
    std::lock_guard<std::mutex> guard(metrics_lock_);
    return metrics_totals_;
}

/* static */ void metrics::clear()
{
    std::lock_guard<std::mutex> guard(metrics_lock_);
    metrics_totals_.clear();
}

void register_metrics(py::module_ & m)
{

    py::module_ metrics_module =
        m.def_submodule("metrics", "Keep track of low-level performance metrics")
            .def("totals", &qdb::metrics::totals)
            .def("clear", &qdb::metrics::clear);

    auto metrics_measure = py::class_<qdb::metrics::measure>(
        metrics_module, "Measure", "Track all metrics within a block of code")
                               .def(py::init())
                               .def("__enter__", &qdb::metrics::measure::enter)
                               .def("__exit__", &qdb::metrics::measure::exit)
                               .def("get", &qdb::metrics::measure::get);
};

}; // namespace qdb
