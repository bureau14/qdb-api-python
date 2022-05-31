#pragma once

#include <range/v3/algorithm/max_element.hpp>
#include <range/v3/view/transform.hpp>

namespace qdb::convert::detail
{

template <typename R>
static inline std::size_t largest_word_length(R const & xs) noexcept
{
    // Transform into a range of sizes
    auto xs_ =
        xs | ranges::views::transform([](auto const & x) -> std::size_t { return ranges::size(x); });

    // Return the element with the largest size
    auto iter = ranges::max_element(xs_);

    return std::max(*iter, std::size_t(1));
};

}; // namespace qdb::convert::detail
