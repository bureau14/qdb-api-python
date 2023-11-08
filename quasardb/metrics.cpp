#include "metrics.hpp"
#include <mutex>

using container_t = std::map<std::string, std::uint64_t>;

static container_t metrics_totals_ = container_t{};
static std::mutex metrics_lock_;

namespace qdb
{

metrics::fixture::~fixture()
{
    using std::chrono::nanoseconds;

    time_point_t stop = clock_t::now();

    auto duration = std::chrono::duration_cast<nanoseconds>(stop - start_);

    metrics::record(test_id_, duration.count());
}

/* static */ void metrics::record(std::string const & test_id, std::uint64_t nsec)
{
    std::lock_guard<std::mutex> guard(metrics_lock_);

    container_t::iterator pos = metrics_totals_.lower_bound(test_id);

    if (pos == metrics_totals_.end() || pos->first != test_id) [[unlikely]]
    {
        pos = metrics_totals_.emplace_hint(pos, test_id, 0);

        assert(pos->second == 0);
    }

    assert(pos->first == test_id);
    pos->second += nsec;
}

void register_metrics(py::module_ & m){};

}; // namespace qdb
