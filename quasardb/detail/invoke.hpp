#pragma once

#include "concepts.hpp"
#include <qdb/client.h>

namespace qdb::detail
{

/**
 * Default qdb invoke strategy just invokes the function and returns the result
 */
template <typename Fn, typename... Args>
struct default_invoke_strategy
{
    inline qdb_error_t operator()(Args... args) const noexcept
    {
        return Fn(std::forward<Args>(args)...);
    }
};

// Validate using some static assertions
static_assert(qdb::concepts::qdb_invoke_strategy<
    default_invoke_strategy<decltype(qdb_exp_batch_push_with_options)>>);

static_assert(
    qdb::concepts::qdb_invoke_strategy<default_invoke_strategy<decltype(qdb_exp_batch_push)>>);

static_assert(qdb::concepts::qdb_invoke_strategy<default_invoke_strategy<decltype(qdb_open_tcp)>>);

} // namespace qdb::detail
