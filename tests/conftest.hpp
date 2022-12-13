#pragma once

#include <pybind11/pybind11.h>
#include <string>

namespace py = pybind11;

namespace qdb
{

/**
 * Models Python's AssertionError, useful for throwing within pytest for proper reporting
 */
class assertion_error
{
public:
    assertion_error() noexcept
    {}

    explicit assertion_error(std::string msg) noexcept
        : msg_{msg}
    {}

    virtual const char * what() const noexcept
    {
        return msg_.c_str();
    }

private:
    std::string msg_;
};

/**
 * General purpose check failed.
 */
class assertion_error_check : public assertion_error
{
public:
    assertion_error_check() noexcept
        : assertion_error{}
    {}

    explicit assertion_error_check(std::string check) noexcept
        : assertion_error{"Condition failed: " + check}
    {}
};

/**
 * Equality condition failed.
 */
class assertion_error_check_equal : public assertion_error_check
{
public:
    assertion_error_check_equal() noexcept
        : assertion_error_check{}
    {}

    explicit assertion_error_check_equal(std::string lhs, std::string rhs) noexcept
        : assertion_error_check{std::string{"Condition failed: " + lhs + " == " + rhs}}
    {}
};

/**
 * Greater than or equal condition failed.
 */
class assertion_error_check_gte : public assertion_error_check
{
public:
    assertion_error_check_gte() noexcept
        : assertion_error_check{}
    {}

    explicit assertion_error_check_gte(std::string lhs, std::string rhs) noexcept
        : assertion_error_check{std::string{"Condition failed: " + lhs + " >= " + rhs}}
    {}
};

/**
 * Equality condition failed.
 */
class assertion_error_check_not_equal : public assertion_error_check
{
public:
    assertion_error_check_not_equal() noexcept
        : assertion_error_check{}
    {}

    explicit assertion_error_check_not_equal(std::string lhs, std::string rhs) noexcept
        : assertion_error_check{std::string{"Condition failed: " + lhs + " != " + rhs}}
    {}
};

}; // namespace qdb

#define TEST_CHECK(check)                                                                            \
    if (static_cast<bool>(check) == false)                                                           \
    {                                                                                                \
        throw ::qdb::assertion_error(                                                                \
            std::string{"Condition failed: " + std::string{#check} + ": " + std::to_string(check)}); \
    }

#define TEST_CHECK_EQUAL(lhs, rhs)                                                            \
    if (!(lhs == rhs))                                                                        \
    {                                                                                         \
        throw ::qdb::assertion_error(                                                         \
            std::string{"Condition failed: " + std::string{#lhs} + " [" + std::to_string(lhs) \
                        + "] == " + std::string{#rhs} + " [" + std::to_string(lhs) + "]"});   \
    }

#define TEST_CHECK_NOT_EQUAL(lhs, rhs)                                                        \
    if (!(lhs != rhs))                                                                        \
    {                                                                                         \
        throw ::qdb::assertion_error(                                                         \
            std::string{"Condition failed: " + std::string{#lhs} + " [" + std::to_string(lhs) \
                        + "] != " + std::string{#rhs} + " [" + std::to_string(lhs) + "]"});   \
    }

#define TEST_CHECK_GTE(lhs, rhs)                                                              \
    if (!(lhs >= rhs))                                                                        \
    {                                                                                         \
        throw ::qdb::assertion_error(                                                         \
            std::string{"Condition failed: " + std::string{#lhs} + " [" + std::to_string(lhs) \
                        + "] >= " + std::string{#rhs} + " [" + std::to_string(lhs) + "]"});   \
    }
