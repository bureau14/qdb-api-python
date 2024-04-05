#include "properties.hpp"
#include <qdb/properties.h>
#include "detail/qdb_resource.hpp"

namespace qdb
{

std::optional<std::string> properties::get(std::string const & key)
{
    detail::qdb_resource<char const> ret{*handle_};

    auto err = qdb_user_properties_get(*handle_, key.c_str(), &ret);

    if (err == qdb_e_alias_not_found) [[unlikely]]
    {
        return {};
    }

    qdb::qdb_throw_if_error(*handle_, err);

    assert(ret != nullptr);

    return std::string{ret};
}

void properties::put(std::string const & key, std::string const & value)
{
    qdb::qdb_throw_if_error(*handle_, qdb_user_properties_put(*handle_, key.c_str(), value.c_str()));
}

}; // namespace qdb
