#include "continuous.hpp"

namespace qdb
{

query_continuous::query_continuous(qdb::handle_ptr h,
    qdb_query_continuous_mode_type_t mode,
    std::chrono::milliseconds pace,
    const std::string & query_string,
    const py::object & bools)
    : _handle{h}
    , _callback{&query_continuous::continuous_callback}
    , _cont_handle{nullptr}
    , _parse_bools{bools}
    , _previous_watermark{0}
    , _watermark{0}
    , _last_error{qdb_e_uninitialized}
{
    qdb::qdb_throw_if_error(
        *_handle, qdb_query_continuous(*_handle, query_string.c_str(), mode,
                      static_cast<unsigned>(pace.count()), _callback, this, &_cont_handle));
}

query_continuous::~query_continuous()
{
    stop();
    release_results();
}

void query_continuous::release_results()
{
    if (_results)
    {
        qdb_release(*_handle, _results);
        _results = nullptr;
    }
}

qdb_error_t query_continuous::copy_results(const qdb_query_result_t * res)
{
    // release whatever results we may be holding before, if we don't have any it does nothing
    release_results();
    if (!res) return qdb_e_ok;
    return qdb_query_copy_results(*_handle, res, &_results);
}

int query_continuous::continuous_callback(void * p, qdb_error_t err, const qdb_query_result_t * res)
{
    auto pthis = static_cast<query_continuous *>(p);

    {
        std::unique_lock<std::mutex> lock{pthis->_results_mutex};

        ++pthis->_watermark;

        pthis->_last_error = err;
        if (QDB_FAILURE(pthis->_last_error))
        {
            // signal the error, if processing end, we will get a qdb_e_interrupted which is handled in
            // the results function
            lock.unlock();
            pthis->_results_cond.notify_all();
            return 0;
        }

        // copy the results using the API convenience function
        // there are two traps to avoid
        // 1. we are within the context of a thread owned by the quasardb C API, calling Python
        // functions could results in deadlocks
        // 2. the results are valid only in the context of the callback, if we want to work on them
        // outside we need to copy them
        pthis->_last_error = pthis->copy_results(res);
        if (QDB_FAILURE(pthis->_last_error))
        {
            pthis->release_results();
        }
    }

    pthis->_results_cond.notify_all();

    return 0;
}

dict_query_result_t query_continuous::unsafe_results()
{
    // when we return from the condition variable we own the mutex
    _previous_watermark.store(_watermark.load());

    if (_last_error == qdb_e_interrupted) throw py::stop_iteration{};

    // throw an error, user may decide to resume iteration
    qdb::qdb_throw_if_error(*_handle, _last_error);

    // safe and quick to call if _results is nullptr
    auto res = convert_query_results(_results, _parse_bools);

    release_results();

    return res;
}

dict_query_result_t query_continuous::results()
{
    std::unique_lock<std::mutex> lock{_results_mutex};

    // you need an additional mechanism to check if you need to do something with the results
    // because condition variables can have spurious calls
    while (_watermark == _previous_watermark)
    {
        // entering the condition variables releases the mutex
        // the callback can update the values when needed
        // every second we are going to check if the user didn't do CTRL-C
        if (_results_cond.wait_for(lock, std::chrono::seconds{1}) == std::cv_status::timeout)
        {
            // if we don't do this, it will be impossible to interrupt the Python program while we wait
            // for results
            if (PyErr_CheckSignals() != 0) throw py::error_already_set();
        }
    }

    return unsafe_results();
}

// the difference with the call above is that we're returning immediately if there's no change
dict_query_result_t query_continuous::probe_results()
{
    std::unique_lock<std::mutex> lock{_results_mutex};

    // check if there's a new value
    if (_watermark == _previous_watermark)
    {
        // nope return empty, don't wait, don't acquire the condition variable
        return dict_query_result_t{};
    }

    // yes, return the value
    return unsafe_results();
}

void query_continuous::stop()
{
    if (_handle && _cont_handle)
    {
        qdb_release(*_handle, _cont_handle);
        _cont_handle = nullptr;
    }
}

} // namespace qdb
