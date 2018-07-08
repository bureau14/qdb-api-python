#pragma once

#include "handle.hpp"
#include <qdb/option.h>
#include <chrono>

namespace qdb
{

class options
{
public:
    explicit options(qdb::handle_ptr h)
        : _handle{h}
    {}

public:
    void set_timeout(std::chrono::milliseconds ms)
    {
        QDB_THROW_IF_ERROR(qdb_option_set_timeout(*_handle, static_cast<int>(ms.count())));
    }

    std::chrono::milliseconds get_timeout()
    {
        int ms = 0;

        QDB_THROW_IF_ERROR(qdb_option_get_timeout(*_handle, &ms));

        return std::chrono::milliseconds{ms};
    }

    void set_stabilization_max_wait(std::chrono::milliseconds ms)
    {
        QDB_THROW_IF_ERROR(qdb_option_set_stabilization_max_wait(*_handle, static_cast<int>(ms.count())));
    }

    std::chrono::milliseconds get_stabilization_max_wait()
    {
        int ms = 0;

        QDB_THROW_IF_ERROR(qdb_option_get_stabilization_max_wait(*_handle, &ms));

        return std::chrono::milliseconds{ms};
    }

    void set_max_cardinality(qdb_uint_t cardinality)
    {
        QDB_THROW_IF_ERROR(qdb_option_set_max_cardinality(*_handle, cardinality));
    }

    void set_compression(qdb_compression_t level)
    {
        QDB_THROW_IF_ERROR(qdb_option_set_compression(*_handle, level));
    }

    void set_encryption(qdb_encryption_t algo)
    {
        QDB_THROW_IF_ERROR(qdb_option_set_encryption(*_handle, algo));
    }

    void set_cluster_public_key(const std::string & key)
    {
        QDB_THROW_IF_ERROR(qdb_option_set_cluster_public_key(*_handle, key.c_str()));
    }

    void set_user_credentials(const std::string & user, const std::string & private_key)
    {
        QDB_THROW_IF_ERROR(qdb_option_set_user_credentials(*_handle, user.c_str(), private_key.c_str()));
    }

private:
    qdb::handle_ptr _handle;
};

template <typename Module>
static inline void register_options(Module & m)
{
    namespace py = pybind11;

    py::class_<qdb::options> o(m, "Options"); //

    // None is reserved keyword in Python
    py::enum_<qdb_compression_t>{o, "Compression", py::arithmetic(), "Compression type"} //
        .value("Disabled", qdb_comp_none)                                                //
        .value("Fast", qdb_comp_fast)                                                    //
        .value("Best", qdb_comp_best);                                                   //

    py::enum_<qdb_encryption_t>{o, "Encryption", py::arithmetic(), "Encryption type"} //
        .value("Disabled", qdb_crypt_none)                                            //
        .value("AES256GCM", qdb_crypt_aes_gcm_256);                                   //

    o.def(py::init<qdb::handle_ptr>())                                                //
        .def("set_timeout", &qdb::options::set_timeout)                               //
        .def("get_timeout", &qdb::options::get_timeout)                               //
        .def("set_stabilization_max_wait", &qdb::options::set_stabilization_max_wait) //
        .def("get_stabilization_max_wait", &qdb::options::get_stabilization_max_wait) //
        .def("set_max_cardinality", &qdb::options::set_max_cardinality)               //
        .def("set_compression", &qdb::options::set_compression)                       //
        .def("set_encryption", &qdb::options::set_encryption)                         //
        .def("set_cluster_public_key", &qdb::options::set_cluster_public_key)         //
        .def("set_user_credentials", &qdb::options::set_user_credentials);            //
}

} // namespace qdb
