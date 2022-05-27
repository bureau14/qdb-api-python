#pragma once

#include <range/v3/range_fwd.hpp>
#include <range/v3/view_adaptor.hpp>

namespace qdb::convert::detail
{

/**
 * The 'passenger view' is a utility that doesn't do anything of itself, except
 * that it allows an arbitrary object (the passenger) to be put onto the range
 * pipeline.
 *
 * This is especially useful for managing object lifetimes, i.e. when using
 * a py::array.
 */
template <class Rng, typename P>
class passenger_view_fn : public ranges::view_adaptor<passenger_view_fn<Rng, P>, Rng>
{
    friend ranges::range_access;
    P xs_;

public:
    constexpr auto size() const
    {
        return ranges::size(this->base());
    };

    inline P const & cdata() const noexcept
    {
        return xs_;
    }

    inline P & data() noexcept
    {
        return xs_;
    }

    inline P && steal() noexcept
    {
        return std::move(xs_);
    }

public:
    passenger_view_fn() = default;
    passenger_view_fn(Rng && rng, P && xs)
        : passenger_view_fn::view_adaptor{std::forward<Rng>(rng)}
        , xs_{xs}
    {}
};

template <typename Rng, typename P>
requires(ranges::sized_range<Rng>) inline passenger_view_fn<Rng, P> passenger_view(
    Rng && rng, P && xs) noexcept
{
    return {std::move(rng), std::move(xs)};
};

}; // namespace qdb::convert::detail
