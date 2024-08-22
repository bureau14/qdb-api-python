#pragma once

#include "concepts.hpp"
#include <qdb/client.h>

namespace qdb::detail
{

/**
 * Mock invoke strategy, which doesn't actually invoke the function but just returns a specific error
 */
template <typename Fn, typename... Args>
struct mock_invoke_strategy
{
    qdb_error_t err_;

    inline constexpr mock_invoke_strategy(qdb_error_t err) noexcept
        : err_{err} {};

    inline qdb_error_t operator()(Args... args) const noexcept
    {
        return err_;
    }
};

// Validate using some static assertions
static_assert(qdb::concepts::qdb_invoke_strategy<
    mock_invoke_strategy<decltype(qdb_exp_batch_push_with_options)>>);

static_assert(qdb::concepts::qdb_invoke_strategy<mock_invoke_strategy<decltype(qdb_exp_batch_push)>>);
static_assert(qdb::concepts::qdb_invoke_strategy<mock_invoke_strategy<decltype(qdb_open_tcp)>>);

} // namespace qdb::detail
