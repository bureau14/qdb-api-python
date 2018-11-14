
#include "version.hpp"
#include <regex>
#include <sstream>
#include <stdexcept>

static std::pair<int, int> get_version_pair(const char * version)
{
    std::regex re(u8"([0-9]+)\\.([0-9]+)\\..*");
    std::cmatch m;
    if (!std::regex_match(version, m, re))
    {
        std::ostringstream sstr;
        sstr << __FUNCTION__ << ": `version` is " << version << ", expected Major.Minor.Patch format.";
        throw std::invalid_argument(sstr.str());
    }
    const auto major = std::stoi(m[1].str());
    const auto minor = std::stoi(m[2].str());
    return std::make_pair(major, minor);
}

namespace version
{

const char * qdb_version = QDB_PY_VERSION;

void check_version(const char * candidate)
{
    auto ver_c   = get_version_pair(candidate);
    auto ver_ref = get_version_pair(qdb_version);
    if (ver_c != ver_ref)
    {
        std::ostringstream sstr;
        sstr << "Expected C API version " << ver_ref.first << '.' << ver_ref.second << " but got " << ver_c.first << '.' << ver_c.second
             << " instead.";
        throw std::runtime_error(sstr.str());
    }
}

} // namespace version
