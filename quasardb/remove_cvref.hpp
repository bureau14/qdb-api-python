#pragma once

#include <type_traits>

namespace qdb
{

// TODO(C++20): Use std::remove_cvref after upgrading GCC to version 9+ (ARM builds use GCC 8.3).
// #if (__cplusplus > 201703L) && defined(_LIBCPP_STD_VER) && (_LIBCPP_STD_VER > 17)
// #define QDB_HAS_REMOVE_CVREF
// #endif

#ifdef QDB_HAS_REMOVE_CVREF

using std::remove_cvref;
using std::remove_cvref_t;

#else

template <class T>
struct remove_cvref
{
    typedef std::remove_cv_t<std::remove_reference_t<T>> type;
};

template <class T>
using remove_cvref_t = typename remove_cvref<T>::type;

#endif

} // namespace qdb
