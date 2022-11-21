#pragma once

#include "concepts.hpp"
#include "error.hpp"
#include "numpy.hpp"
#include "traits.hpp"
#include <string>

/**
 * Define various way to do runtime -> compile time template dispatch for the
 * types that we have.
 */
namespace qdb::dispatch
{

////////////////////////////////////////////////////////////////////////////////
//
// DTYPE DISPATCHER
//
// Most complex dispatcher: dispatch based on dtype size, kind, and could even
// be extended to datetime precision and whatnot.
//
////////////////////////////////////////////////////////////////////////////////

template <template <qdb_ts_column_type_t, typename> class Callback,
    qdb_ts_column_type_t ColumnType,
    typename DType,
    typename... Args>
concept valid_callback = requires(Args... args) {
                             {
                                 Callback<ColumnType, DType>{}
                             };
                             {
                                 Callback<ColumnType, DType>{}(std::forward<Args>(args)...)
                             };
                         };

template <template <qdb_ts_column_type_t, typename> class Callback,
    qdb_ts_column_type_t ColumnType,
    concepts::dtype DType,
    typename... Args>
    requires(not valid_callback<Callback, ColumnType, DType, Args...>)
static inline constexpr void by_dtype_(Args &&... args)
{
    throw qdb::incompatible_type_exception{"Dtype dispatcher not implemented for ColumnType '"
                                           + std::to_string(ColumnType)
                                           + "': " + numpy::detail::to_string(DType::dtype())};
};

// Template is only expanded when it actually has a dispatch function with the
// correct signature.
template <template <qdb_ts_column_type_t, typename> class Callback,
    qdb_ts_column_type_t ColumnType,
    concepts::dtype DType,
    typename... Args>

    requires(valid_callback<Callback, ColumnType, DType, Args...>)
static inline constexpr decltype(auto) by_dtype_(Args &&... args)
{
    return Callback<ColumnType, DType>{}(std::forward<Args>(args)...);
};

/**
 * Converts runtime kind/size into compile-time template function dispatch.
 *
 * TODO(leon): possibly allow choice whgether or not to raise a compile-time error
 *             on unimplemented template functions, right now it assumes these are
 *             just side effects on template expansion overload: the fact that a
 *             C++ compiler decides to generate functions for all possible
 *             combinations of dtype properties.
 */
template <template <qdb_ts_column_type_t, typename> class Callback,
    qdb_ts_column_type_t ColumnType,
    typename... Args>
static inline constexpr decltype(auto) by_dtype(
    traits::dtype_kind kind, py::ssize_t size, Args &&... args)
{
#define CASE(K, DT) \
    case K:         \
        return by_dtype_<Callback, ColumnType, DT, Args...>(std::forward<Args>(args)...);

    switch (kind)
    {
        CASE(traits::unicode_kind, traits::unicode_dtype);
        CASE(traits::bytestring_kind, traits::bytestring_dtype);
        CASE(traits::datetime_kind, traits::datetime64_ns_dtype);
        CASE(traits::object_kind, traits::pyobject_dtype);

    case traits::int_kind:
        switch (size)
        {
            CASE(2, traits::int16_dtype);
            CASE(4, traits::int32_dtype);
            CASE(8, traits::int64_dtype);
        default:
            throw qdb::not_implemented_exception{
                "Integer dtype with size " + std::to_string(size) + " is not supported"};
        };

    case traits::float_kind:
        switch (size)
        {
            CASE(4, traits::float32_dtype);
            CASE(8, traits::float64_dtype);
        default:
            throw qdb::not_implemented_exception{
                "Integer dtype with size " + std::to_string(size) + " is not supported"};
        };

    default:
        throw qdb::not_implemented_exception{
            "Unable to dispatch: dtype with kind '" + std::string{(char)kind, 1} + "' not recognized"};
    };

#undef SIZED_CASE3
#undef SIZED_CASE2
#undef CASE
}

template <template <qdb_ts_column_type_t, typename> class Callback,
    qdb_ts_column_type_t ColumnType,
    typename... Args>
static inline constexpr decltype(auto) by_dtype(py::dtype const & dt, Args &&... args)
{
    return by_dtype<Callback, ColumnType, Args...>(
        static_cast<traits::dtype_kind>(dt.kind()), dt.itemsize(), std::forward<Args>(args)...);
}

////////////////////////////////////////////////////////////////////////////////
//
// COLUMN TYPE DISPATCHER
//
////////////////////////////////////////////////////////////////////////////////

template <template <qdb_ts_column_type_t> class Callback, typename... Args>
static inline constexpr decltype(auto) by_column_type(qdb_ts_column_type_t ct, Args &&... args)
{
    switch (ct)
    {
#define CASE(CT) \
    case CT:     \
        return Callback<CT>{}(std::forward<Args>(args)...);

        CASE(qdb_ts_column_int64);
        CASE(qdb_ts_column_double);
        CASE(qdb_ts_column_timestamp);
        CASE(qdb_ts_column_blob);
        CASE(qdb_ts_column_string);
        CASE(qdb_ts_column_symbol);

    default:
        throw qdb::not_implemented_exception{"Column type dispatch not handled: " + std::to_string(ct)};
    };
#undef CASE
}

}; // namespace qdb::dispatch
