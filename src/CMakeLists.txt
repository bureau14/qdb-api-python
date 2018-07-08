pybind11_add_module(quasardb
    cluster.hpp
    error.hpp
    entry.hpp
    blob.hpp
    handle.hpp
    memcpy_wrap.cpp
    options.hpp
    qdb_client.cpp
    query.hpp
    query.cpp
    ts.hpp
    ts_batch.hpp
    ts_convert.hpp
    tag.hpp
    utils.hpp
    )

target_link_libraries(quasardb PUBLIC ${QDB_API_LIB})