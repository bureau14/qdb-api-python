#pragma once

#include "traits.hpp" // We include traits.hpp only for static_asserts below
#include <pybind11/numpy.h>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/traits.hpp>
#include <iterator>
#include <type_traits>

namespace qdb::concepts
{

template <typename R, typename T>
concept range_t =
    // Ensure we are a range
    ranges::range<R> &&

    std::is_same_v<ranges::range_value_t<R>, T>;

template <typename R, typename T>
concept input_range_t =
    // Ensure we are an input range
    ranges::input_range<R>

    // Ensure the range carries the correct type.
    && range_t<R, T>;

template <typename R, typename T>
concept forward_range_t =
    // Ensure we are a forward range
    ranges::forward_range<R>

    // And delegate the rest of the checks
    && input_range_t<R, T>;

template <typename R, typename T>
concept sized_range_t =
    // Ensure we are a sized range
    ranges::sized_range<R>

    // And delegate the rest of the checks
    && range_t<R, T>;

namespace py = pybind11;

////////////////////////////////////////////////////////////////////////////////
//
// QuasarDB value types
//
// Verifies a "type" of QuasarDB value we're dealing with.
//
///////////////////

template <typename T>
concept qdb_point =
    // Check for the boolean flag
    traits::qdb_value<T>::is_qdb_point &&

    // Every point should have a "primitive" counterpart, for which we do a
    // basic 'is_qdb_primitive' assessment.
    traits::qdb_value<typename traits::qdb_value<T>::primitive_type>::is_qdb_primitive

    ;

template <typename T>
concept qdb_primitive =

    // Check for the boolean flag
    traits::qdb_value<T>::is_qdb_primitive &&

    // Every primitive has a null representation
    std::is_same_v<decltype(traits::null_value<T>()), T> &&

    // Every primitive should be able to check for null validity
    std::is_same_v<decltype(traits::is_null<T>(T{})), bool> &&

    // And every primitive should have its "point" counterpart, which must
    // be a valid qdb_point.
    qdb_point<typename traits::qdb_value<T>::point_type>;

// XXX(leon): could be enabled for debug-only, but we don't build Debug in CI,
//            and these checks are cheap.

static_assert(qdb_primitive<qdb_int_t>);
static_assert(qdb_primitive<double>);
static_assert(qdb_primitive<qdb_timespec_t>);
static_assert(qdb_primitive<qdb_string_t>);
static_assert(qdb_primitive<qdb_blob_t>);

static_assert(not qdb_point<qdb_int_t>);
static_assert(not qdb_point<double>);
static_assert(not qdb_point<qdb_timespec_t>);
static_assert(not qdb_point<qdb_string_t>);
static_assert(not qdb_point<qdb_blob_t>);

static_assert(qdb_point<qdb_ts_double_point>);
static_assert(qdb_point<qdb_ts_int64_point>);
static_assert(qdb_point<qdb_ts_timestamp_point>);
static_assert(qdb_point<qdb_ts_blob_point>);
static_assert(qdb_point<qdb_ts_string_point>);

static_assert(not qdb_primitive<qdb_ts_double_point>);
static_assert(not qdb_primitive<qdb_ts_int64_point>);
static_assert(not qdb_primitive<qdb_ts_timestamp_point>);
static_assert(not qdb_primitive<qdb_ts_blob_point>);
static_assert(not qdb_primitive<qdb_ts_string_point>);

////////////////////////////////////////////////////////////////////////////////
//
// Numpy / DType concepts
//
///////////////////

template <typename Dtype>
concept dtype = std::is_base_of_v<traits::dtype<Dtype::kind>, Dtype>

    && std::is_enum_v<decltype(Dtype::kind)>

    ;

template <typename Dtype>
concept fixed_width_dtype = dtype<Dtype>

    // Check base class
    && std::is_base_of_v<traits::fixed_width_dtype<Dtype::kind, Dtype::size>, Dtype>;

// Test dtypes against their size
template <typename Dtype, py::ssize_t Size>
concept dtype_of_width = fixed_width_dtype<Dtype> && Dtype::size == Size;

// 64bit dtype
template <typename Dtype>
concept dtype64 = dtype_of_width<Dtype, 8>;

// 32bit dtype
template <typename Dtype>
concept dtype32 = dtype_of_width<Dtype, 4>;

// 16bit dtype
template <typename Dtype>
concept dtype16 = dtype_of_width<Dtype, 2>;

// 8bit dtype
template <typename Dtype>
concept dtype8 = dtype_of_width<Dtype, 1>;

template <typename Dtype>
concept datetime64_dtype = fixed_width_dtype<Dtype>

    // Verify base class is datetime64_dtype
    && std::is_base_of_v<traits::datetime64_dtype<Dtype::precision>, Dtype>

    // datetime64 is always a 64bit object
    && dtype64<Dtype>

    // It needs to have a precision
    && std::is_enum_v<decltype(Dtype::precision)>;

template <typename Dtype>
concept variable_width_dtype =
    dtype<Dtype>

    // Verify base class
    && std::is_base_of_v<
        traits::variable_width_dtype<Dtype::kind, typename Dtype::stride_type, Dtype::code_point_size>,
        Dtype>

    // Verify we have a "stride_type", which is the type used to represent a single stride.
    && Dtype::code_point_size == sizeof(typename Dtype::stride_type::value_type)

    ;

template <typename Dtype>
concept object_dtype = dtype<Dtype>

    // Objects are always fixed width (64-bit pointers, effectively)
    && fixed_width_dtype<Dtype>

    // Verify base class
    && std::is_base_of_v<traits::object_dtype<typename Dtype::value_type>, Dtype>;

// Trivial dtypes are useful for deciding whether you can use fast memcpy-based
// conversions, e.g. when numpy's int64 has the exact same representation as
// qdb_int_t.
template <typename Dtype>
concept trivial_dtype =
    // Trivial dtypes are *always* fixed width
    fixed_width_dtype<Dtype>

    // The low-level numpy representation needs to be trivial; e.g. numpy represents
    // something as int32.
    && std::is_trivial<typename Dtype::value_type>::value

    // Needs to have a quasardb representation of this type as well (e.g.
    // dtype('int64') mapping to qdb_int_t
    && std::is_trivial<typename Dtype::qdb_value>::value

    // Sanity check: the trivial qdb_value needs to be a qdb_primitive -- if this fails,
    // it hints at a misconfigured dtype specification.
    && qdb_primitive<typename Dtype::qdb_value>

    // Last but not least (and most important one): both the dtype's low level
    // representation and quasardb's representation need to be equivalent.
    && std::is_same_v<typename Dtype::value_type, typename Dtype::qdb_value>

    ;

template <typename Dtype>
using delegate_from_type_t = typename Dtype::value_type;

template <typename Dtype>
using delegate_to_type_t = typename Dtype::delegate_type::value_type;

template <typename Dtype>
concept delegate_dtype =
    // Delegates are always dtypes
    dtype<Dtype>

    // And delegates have a delegate that is also a proper dtype
    && dtype<typename Dtype::delegate_type>

    // And conversion between these types should be possible (nothrow could be
    // left away, but that comes with certain performance disadvantages).
    //
    // Note that this only checks for <From, To>, e.g. if int32_t is a delegate
    // to int64_t, it only checks whether int32_t is convertible to int64_t, not the
    // other way around. This is intentional, because the other way around leads to
    // loss of precision, and as such can be argued is *not* convertible, at the
    // very least nothrow_convertible.
    && std::is_nothrow_convertible_v<delegate_from_type_t<Dtype>, delegate_to_type_t<Dtype>>;

// Assertions
static_assert(dtype<traits::unicode_dtype>);

static_assert(variable_width_dtype<traits::unicode_dtype>);
static_assert(fixed_width_dtype<traits::pyobject_dtype>);
static_assert(datetime64_dtype<traits::datetime64_ns_dtype>);
static_assert(not fixed_width_dtype<traits::unicode_dtype>);
static_assert(not variable_width_dtype<traits::pyobject_dtype>);

static_assert(not trivial_dtype<traits::datetime64_ns_dtype>);
static_assert(not trivial_dtype<traits::pyobject_dtype>);
static_assert(not trivial_dtype<traits::unicode_dtype>);
static_assert(not trivial_dtype<traits::bytestring_dtype>);
static_assert(trivial_dtype<traits::float64_dtype>);

static_assert(dtype_of_width<traits::int64_dtype, 8>);
static_assert(dtype_of_width<traits::int32_dtype, 4>);
static_assert(dtype_of_width<traits::int16_dtype, 2>);

static_assert(dtype64<traits::int64_dtype>);
static_assert(dtype32<traits::int32_dtype>);
static_assert(dtype16<traits::int16_dtype>);

static_assert(fixed_width_dtype<traits::int64_dtype>);
static_assert(fixed_width_dtype<traits::int32_dtype>);
static_assert(fixed_width_dtype<traits::int16_dtype>);

static_assert(trivial_dtype<traits::int64_dtype>);
// Not trivial because they don't have a qdb-native representation, so
// they're not trivial in the sense of how we should handle them.
static_assert(not trivial_dtype<traits::int32_dtype>);
static_assert(not trivial_dtype<traits::int16_dtype>);

static_assert(not delegate_dtype<traits::int64_dtype>);
static_assert(delegate_dtype<traits::int32_dtype>);

static_assert(dtype_of_width<traits::float64_dtype, 8>);
static_assert(dtype_of_width<traits::float32_dtype, 4>);

static_assert(trivial_dtype<traits::float64_dtype>);
// Not trivial because they don't have a qdb-native representation, so
// they're not trivial in the sense of how we should handle them.
static_assert(not trivial_dtype<traits::float32_dtype>);

static_assert(fixed_width_dtype<traits::float64_dtype>);
static_assert(fixed_width_dtype<traits::float32_dtype>);
}; // namespace qdb::concepts
