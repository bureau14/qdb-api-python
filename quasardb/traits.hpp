
#pragma once

#include <qdb/client.h>
#include <qdb/ts.h>
#include <pybind11/numpy.h>
#include <cassert>
#include <cmath>

namespace qdb::traits
{

namespace py = pybind11;

////////////////////////////////////////////////////////////////////////////////
//
// BASE CLASSES
//
// The classes below define the "base classes" for all traits defined below.
// Typically only necessary to modify these if you want to add a completely new
// trait type, or change some defaults.
//
///////////////////

template <qdb_ts_column_type_t ColumnType>
struct qdb_column;

template <qdb_ts_column_type_t ColumnType>
struct qdb_column_base
{
    static constexpr qdb_ts_column_type_t column_type = ColumnType;
};

template <typename ValueType>
struct qdb_value;

template <typename ValueType>
struct qdb_value_base
{
    using value_type = ValueType;

    static constexpr bool is_qdb_primitive = false;
    static constexpr bool is_qdb_point     = false;
};

////////////////////////////////////////////////////////////////////////////////
//
// TRAIT DECLARATION MACROS
//
// Convenience macros that make it more concise to declare a new trait, and
// avoid any errors.
//
///////////////////

#define VALUE_TRAIT_DECL(VT) \
    template <>              \
    struct qdb_value<VT> : public qdb_value_base<VT>

#define COLUMN_TRAIT_DECL(CT, VT)                      \
    template <>                                        \
    struct qdb_column<CT> : public qdb_column_base<CT> \
    {                                                  \
        using value_type = VT;                         \
    };

#define DTYPE_TRAIT_DECL(DT) \
    template <>              \
    struct npy_dtype<DT> : public npy_dtype_base<DT>

#define DTYPE_TRAIT_HEADER(DT, VALUE_TYPE)             \
    using value_type = VALUE_TYPE;                     \
                                                       \
    static inline bool is_dtype(py::dtype dt) noexcept \
    {                                                  \
        return DT::is_dtype(dt);                       \
    }

#define DTYPE_DELEGATE_HEADER(DT1, VALUE_TYPE, DT2) \
    DTYPE_TRAIT_HEADER(DT1, VALUE_TYPE);            \
    using delegate_type = DT2;

////////////////////////////////////////////////////////////////////////////////
//
// COLUMN TRAITS
//
///////////////////

COLUMN_TRAIT_DECL(qdb_ts_column_int64, qdb_int_t);
COLUMN_TRAIT_DECL(qdb_ts_column_double, double);
COLUMN_TRAIT_DECL(qdb_ts_column_timestamp, qdb_timespec_t);
COLUMN_TRAIT_DECL(qdb_ts_column_blob, qdb_blob_t);
COLUMN_TRAIT_DECL(qdb_ts_column_string, qdb_string_t);
COLUMN_TRAIT_DECL(qdb_ts_column_symbol, qdb_string_t);

#undef COLUMN_TRAIT_DECL

////////////////////////////////////////////////////////////////////////////////
//
// VALUE TRAITS
//
///////////////////

VALUE_TRAIT_DECL(qdb_int_t)
{
    using point_type = qdb_ts_int64_point;

    static constexpr bool is_qdb_primitive = true;

    static constexpr qdb_int_t null_value() noexcept
    {
        return static_cast<qdb_int_t>(0x8000000000000000ll);
    }

    static constexpr bool is_null(qdb_int_t x) noexcept
    {
        return x == null_value();
    }
};

VALUE_TRAIT_DECL(double)
{
    using point_type = qdb_ts_double_point;

    static constexpr bool is_qdb_primitive = true;

    static constexpr double null_value() noexcept
    {
        return NAN;
    }

    static bool is_null(double x) noexcept
    {
        // std::isnan is not a constexpr on windows
        return std::isnan(x);
    }
};

VALUE_TRAIT_DECL(qdb_timespec_t)
{
    using point_type = qdb_ts_timestamp_point;

    static constexpr bool is_qdb_primitive = true;

    static constexpr qdb_timespec_t null_value() noexcept
    {
        return qdb_timespec_t{qdb_min_time, qdb_min_time};
    }

    static constexpr bool is_null(qdb_timespec_t x) noexcept
    {
        return x.tv_sec == qdb_min_time && x.tv_nsec == qdb_min_time;
    }

    static constexpr qdb_timespec_t min() noexcept
    {
        return qdb_timespec_t{0, 0};
    }

    static constexpr qdb_timespec_t max() noexcept
    {
        return qdb_timespec_t{std::numeric_limits<qdb_time_t>::max(), 0};
    }
};

VALUE_TRAIT_DECL(qdb_ts_range_t)
{
    static constexpr bool is_qdb_primitive = true;

    static constexpr qdb_ts_range_t null_value() noexcept
    {
        return qdb_ts_range_t{
            qdb_value<qdb_timespec_t>::null_value(), qdb_value<qdb_timespec_t>::null_value()};
    }

    static constexpr qdb_ts_range_t forever() noexcept
    {
        return qdb_ts_range_t{qdb_timespec_t{0, 0},

            // Don't hate the player, hate the game of poorly defined constants
            qdb_timespec_t{9223372036, 854775807}};
    }

    static constexpr bool is_null(qdb_timespec_t x) noexcept
    {
        return x.tv_sec == qdb_min_time && x.tv_nsec == qdb_min_time;
    }
};

VALUE_TRAIT_DECL(qdb_string_t)
{
    using point_type                       = qdb_ts_string_point;
    static constexpr bool is_qdb_primitive = true;

    static constexpr qdb_string_t null_value() noexcept
    {
        return qdb_string_t{nullptr, 0};
    }

    static constexpr bool is_null(qdb_string_t x) noexcept
    {
        return x.length == 0;
    }
};

VALUE_TRAIT_DECL(qdb_blob_t)
{
    using point_type                       = qdb_ts_blob_point;
    static constexpr bool is_qdb_primitive = true;

    static constexpr qdb_blob_t null_value() noexcept
    {
        return qdb_blob_t{nullptr, 0};
    }

    static constexpr bool is_null(qdb_blob_t x) noexcept
    {
        return x.content_length == 0;
    }
};

VALUE_TRAIT_DECL(qdb_ts_double_point)
{
    using primitive_type               = double;
    static constexpr bool is_qdb_point = true;
};

VALUE_TRAIT_DECL(qdb_ts_int64_point)
{
    using primitive_type               = qdb_int_t;
    static constexpr bool is_qdb_point = true;
};

VALUE_TRAIT_DECL(qdb_ts_timestamp_point)
{
    using primitive_type               = qdb_timespec_t;
    static constexpr bool is_qdb_point = true;
};

VALUE_TRAIT_DECL(qdb_ts_blob_point)
{
    using primitive_type               = qdb_blob_t;
    static constexpr bool is_qdb_point = true;
};

VALUE_TRAIT_DECL(qdb_ts_string_point)
{
    using primitive_type               = qdb_string_t;
    static constexpr bool is_qdb_point = true;
};

#undef VALUE_TRAIT_DECL

////////////////////////////////////////////////////////////////////////////////
//
// DTYPE DEFINITIONS
//
// Not strictly a "trait" but rather a constant, but we use these to define traits.
// Effectively we declare our dtypes as template-dispatch tags.
//
// You will want to add new kinds / dtypes here, e.g. if we start supporting unsigned
// integers or whatever.
//
///////////////////

/**
 * One of the things we want to be able to dispatch on is the "kind", e.g.
 * "dispatch based on any integer-type".
 *
 * Redefine these character codes to enums
 */
enum dtype_kind
{
    int_kind        = 'i',
    float_kind      = 'f',
    datetime_kind   = 'M',
    object_kind     = 'O',
    unicode_kind    = 'U',
    bytestring_kind = 'S'
};

/**
 * Dtype tag dispatch base struct, only has a "kind", but never directly constructed.
 */
template <dtype_kind Kind>
struct dtype
{
    static constexpr dtype_kind kind = Kind;

    static inline bool is_dtype(py::dtype dt) noexcept
    {
        return dt.kind() == Kind;
    };
};

/**
 * Dtypes with a fixed width, which is most "simple" ones, including py::object.
 */
template <dtype_kind Kind, py::ssize_t Size>
struct fixed_width_dtype : public dtype<Kind>
{
    static constexpr py::ssize_t size = Size;

    static inline bool is_dtype(py::dtype dt) noexcept
    {
        return dt.kind() == Kind && dt.itemsize() == Size;
    }

    // By definition, fixed width dtypes always have a stride size of 1 (i.e.
    // they move forward sizeof(value_type) bytes per item.
    static constexpr py::ssize_t stride_size(py::ssize_t /* itemsize */) noexcept
    {
        return 1;
    };
};

/**
 * Variable width dtypes require special parsing. Numpy variable width encoding
 * encodes the entire array as a single, continuous block of memory, with items
 * of length `CodePointSize`. This length is always the same for a single instance
 * of an array, but differs for different instances.
 *
 * Before encoding the array, Numpy determines the length of the longest item
 * in the array, and uses that as the stride size.
 *
 * There are two variable width dtypes we deal with:
 *
 *  - unicode, that is represented using UTF-32, meaning the size of each
 *    code point is 4 bytes.
 *  - bytestrings, that are represented using "just" bytes, but are null
 *    terminated (!!). Numpy has deprecated it, in favor of just byte[] objects,
 *    but Pandas sometimes generates this type of data.
 *
 *    Since it is very efficient way to encode blob data, we support it.
 *
 * StrideType is the type used to represent a single code point.
 *
 * CodePointSize defines the size (in bytes) of each code point, e.g. for
 * UTF-32 / UCS-4 this would be 4, as each character is represented using 4 bytes.
 */
template <dtype_kind Kind, typename StrideType, py::ssize_t CodePointSize>
    requires(CodePointSize == sizeof(typename StrideType::value_type))
struct variable_width_dtype : public dtype<Kind>
{
    using dtype<Kind>::is_dtype;
    using stride_type = StrideType;

    static constexpr py::ssize_t code_point_size = CodePointSize;

    /**
     * Given the number of bytes of a single item, calculate the number of characters
     * (code points) in a single stride.
     *
     * By definition, n == stride_size(itemsize(n))
     */
    static constexpr py::ssize_t stride_size(py::ssize_t itemsize) noexcept
    {
        assert(itemsize % code_point_size == 0);
        return itemsize / code_point_size;
    };

    /**
     * Based on the number of codepoints, calculates the numpy `itemsize()` value
     * for this stride type (i.e. the number of bytes in a single item).
     *
     * By definition, n == itemsize(stride_size(n))
     */
    static constexpr py::ssize_t itemsize(py::ssize_t codepoints) noexcept
    {
        return codepoints * code_point_size;
    };
};

// Py_intptr_t is best described as a pointer-that-can-be-cast-to-integer and
// back.
//
// As such, it's a good proxy for the size of a py::object as it is represented
// in a np.ndarray.
static constexpr py::ssize_t pyobject_size = sizeof(Py_intptr_t);

template <typename ObjectType>
struct object_dtype : public fixed_width_dtype<object_kind, pyobject_size>
{
    using fixed_width_dtype<object_kind, pyobject_size>::is_dtype;
    using value_type = ObjectType;

    static inline bool is_null(py::object x) noexcept
    {
        return x.is_none();
    };
};

/**
 * We may want to dispatch datetime64 based on the precision (ns, ms, etc), which
 * is what we will allow for here.
 */
enum datetime64_precision
{
    datetime64_ns
};

template <datetime64_precision Precision>
struct datetime64_dtype : public fixed_width_dtype<datetime_kind, 8>
{
    static constexpr datetime64_precision precision = Precision;
};

struct int64_dtype : public fixed_width_dtype<int_kind, 8>
{
    using value_type = std::int64_t;
    using qdb_value  = qdb_int_t;

    static constexpr std::int64_t null_value() noexcept
    {
        return static_cast<std::int64_t>(0x8000000000000000ll);
    };

    static constexpr inline bool is_null(value_type x) noexcept
    {
        return x == null_value();
    };

    static inline py::dtype dtype() noexcept
    {
        return py::dtype("int64");
    }
};

struct int32_dtype : public fixed_width_dtype<int_kind, 4>
{
    using value_type    = std::int32_t;
    using delegate_type = int64_dtype;

    static constexpr value_type null_value() noexcept
    {
        return std::numeric_limits<value_type>::min();
    };

    static constexpr inline bool is_null(value_type x) noexcept
    {
        return x == null_value();
    };

    static inline py::dtype dtype() noexcept
    {
        return py::dtype("int32");
    }
};

struct int16_dtype : public fixed_width_dtype<int_kind, 2>
{
    using value_type    = std::int16_t;
    using delegate_type = int64_dtype;

    static constexpr value_type null_value() noexcept
    {
        return std::numeric_limits<value_type>::min();
    };

    static constexpr inline bool is_null(value_type x) noexcept
    {
        return x == null_value();
    };

    static inline py::dtype dtype() noexcept
    {
        return py::dtype("int16");
    }
};

struct float64_dtype : public fixed_width_dtype<float_kind, 8>
{
    using value_type = double;
    using qdb_value  = double;

    static constexpr value_type null_value() noexcept
    {
        return NAN;
    };

    static inline bool is_null(value_type x) noexcept
    {
        return std::isnan(x);
    };

    static inline py::dtype dtype() noexcept
    {
        return py::dtype("float64");
    }
};

struct float32_dtype : public fixed_width_dtype<float_kind, 4>
{
    using value_type    = float;
    using delegate_type = float64_dtype;

    static constexpr value_type null_value() noexcept
    {
        return NAN;
    };

    static inline py::dtype dtype() noexcept
    {
        return py::dtype("float32");
    };

    static inline bool is_null(value_type x) noexcept
    {
        return std::isnan(x);
    };
};

struct unicode_dtype : public variable_width_dtype<unicode_kind, std::u32string, 4>
{
    using value_type = std::u32string::value_type;

    static constexpr value_type null_value() noexcept
    {
        return '\0';
    };
    static constexpr inline bool is_null(value_type x) noexcept
    {
        return x == null_value();
    };

    static inline py::dtype dtype() noexcept
    {
        return py::dtype("U");
    }

    static inline py::dtype dtype(py::ssize_t codepoints_per_word) noexcept
    {
        return py::dtype(std::string{"U"} + std::to_string(codepoints_per_word));
    }
};

struct bytestring_dtype : public variable_width_dtype<bytestring_kind, std::string, 1>
{
    using value_type = std::string::value_type;

    static constexpr value_type null_value() noexcept
    {
        return '\0';
    };

    static constexpr inline bool is_null(value_type x) noexcept
    {
        return x == null_value();
    };

    static inline py::dtype dtype() noexcept
    {
        return py::dtype("S");
    }
};

struct pyobject_dtype : public object_dtype<py::object>
{
    using value_type = py::object;

    static inline py::object null_value() noexcept
    {
        return py::none{};
    };

    static inline py::dtype dtype() noexcept
    {
        return py::dtype("O");
    }
};

struct datetime64_ns_dtype : public datetime64_dtype<datetime64_ns>
{
    static_assert(sizeof(int64_t) == sizeof(qdb_time_t));
    using value_type = std::int64_t;

    static constexpr std::int64_t null_value() noexcept
    {
        return static_cast<std::int64_t>(qdb_min_time);
    }

    static constexpr inline bool is_null(value_type x) noexcept
    {
        return x == null_value();
    };

    static inline py::dtype dtype() noexcept
    {
        return py::dtype("datetime64[ns]");
    }
};

// Easy accessors for some commonly requested functions
template <typename T>
inline bool is_null(T x) noexcept
{
    return qdb_value<T>::is_null(x);
};

template <typename T>
inline T null_value() noexcept
{
    return qdb_value<T>::null_value();
};

// Easy accessors for some commonly requested functions
template <typename T>
inline bool is_null(typename T::value_type x) noexcept
{
    return T::is_null(x);
};

// Easy accessors for some commonly requested functions
template <typename T>
inline typename T::value_type null_value() noexcept
{
    return T::null_value();
};

}; // namespace qdb::traits
