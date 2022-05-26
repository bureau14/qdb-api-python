#pragma once

#include <range/v3/range/concepts.hpp>
#include <range/v3/view/transform.hpp>
#include <iterator>
#include <tuple>
#include <type_traits>
#include <utility>

namespace qdb
{

template <std::size_t I, ranges::input_range R>
inline decltype(auto) make_unzip_view(R const & input) noexcept
{
    using tuple_type = ranges::range_value_t<R>;

    auto xform = [](tuple_type const & x) { return std::get<I>(x); };

    return ranges::views::transform(input, xform);
}

template <std::size_t I>
struct view_unzipper
{
    template <ranges::input_range R>
    inline decltype(auto) operator()(R const &) = delete;
};

template <>
struct view_unzipper<1>
{
    template <ranges::input_range R>
    inline decltype(auto) operator()(R const & input)
    {
        return std::make_tuple(make_unzip_view<0>(input));
    }
};

template <>
struct view_unzipper<2>
{
    template <ranges::input_range R>
    inline decltype(auto) operator()(R const & input)
    {
        return std::make_tuple(make_unzip_view<0>(input), make_unzip_view<1>(input));
    }
};

template <>
struct view_unzipper<3>
{
    template <ranges::input_range R>
    inline decltype(auto) operator()(R const & input)
    {
        return std::make_tuple(
            make_unzip_view<0>(input), make_unzip_view<1>(input), make_unzip_view<2>(input));
    }
};

template <>
struct view_unzipper<4>
{
    template <ranges::input_range R>
    inline decltype(auto) operator()(R const & input)
    {
        return std::make_tuple(make_unzip_view<0>(input), make_unzip_view<1>(input),
            make_unzip_view<2>(input), make_unzip_view<3>(input));
    }
};

/**
 * Transforms a single range of tuples to a tuple of ranges, such that
 * std::get<I>(std::next(in, N)) becomes std::next(std::get<I>(out), N)
 *
 * TODO(leon): It isn't perfect, as we iterate over the input array multiple
 *             times, once for each type. A better approach would be to iterate
 *             once and build all sub-ranges out of it, but that approach
 *             doesn't map well to the existing ranges views / abstractions.
 */
template <ranges::input_range R>
inline constexpr decltype(auto) make_unzip_views(R const & input) noexcept
{
    using tuple_type = typename ranges::range_value_t<R>;
    using tuple_size = std::tuple_size<tuple_type>;
    return view_unzipper<tuple_size::value>{}(input);
}

}; // namespace qdb
