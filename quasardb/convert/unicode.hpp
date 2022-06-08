#pragma once

#include <range/v3/range_fwd.hpp>
#include <range/v3/view/cache1.hpp>
#include <range/v3/view/adaptor.hpp>

namespace qdb::convert::unicode
{

typedef std::uint32_t cp_type;
typedef std::uint32_t u32_type; // UTF32 == CodePoint
typedef std::uint8_t u8_type;

namespace encode
{

template <typename CharT>
struct fn_;

template <typename CharT, std::size_t Width = (sizeof(cp_type) / sizeof(CharT))>
requires(1 <= Width && Width <= 4) struct next_chars
{
public:
    constexpr next_chars() = default;

    constexpr next_chars(CharT _1)
        : xs_{{_1}}
        , n_{1}
    {}

    constexpr next_chars(CharT _1, CharT _2) requires(Width >= 2)
        : xs_{{_1, _2}}
        , n_{2}
    {}

    constexpr next_chars(CharT _1, CharT _2, CharT _3) requires(Width >= 3)
        : xs_{{_1, _2, _3}}
        , n_{3}
    {}

    constexpr next_chars(CharT _1, CharT _2, CharT _3, CharT _4) requires(Width == 4)
        : xs_{{_1, _2, _3, _4}}
        , n_{4}
    {}
    constexpr inline CharT pop() noexcept
    {
        assert(p_ < n_);
        return xs_[p_++];
    };

    constexpr inline bool empty() const noexcept
    {
        return p_ == n_;
    };

private:
    std::array<CharT, Width> xs_{{}};
    std::size_t n_ = 0;
    std::size_t p_ = 0;
};

// codepoint -> utf8
template <>
struct fn_<u8_type>
{
    inline next_chars<u8_type> operator()(cp_type cp) const noexcept
    {
        if (cp >= (1L << 16)) [[unlikely]]
        {
            return {static_cast<u8_type>(0xf0 | (cp >> 18)),
                static_cast<u8_type>(0x80 | ((cp >> 12) & 0x3f)),
                static_cast<u8_type>(0x80 | ((cp >> 6) & 0x3f)),
                static_cast<u8_type>(0x80 | ((cp >> 0) & 0x3f))};
        }
        else if (cp >= (1L << 11))
        {
            return {static_cast<u8_type>(0xe0 | (cp >> 12)),
                static_cast<u8_type>(0x80 | ((cp >> 6) & 0x3f)),
                static_cast<u8_type>(0x80 | ((cp >> 0) & 0x3f))};
        }
        else if (cp >= (1L << 7))
        {
            return {static_cast<u8_type>(0xc0 | (cp >> 6)),
                static_cast<u8_type>(0x80 | ((cp >> 0) & 0x3f))};
        }
        else [[likely]]
        {
            return {static_cast<u8_type>(cp)};
        }
    }
};

/**
 * UTF32->UTF8 encoding view
 *
 * Reads codepoints (i.e. UTF32/UCS4) and emits UTF8. Effectively used for
 * conversion from numpy to qdb.
 */
template <typename OutCharT, typename Rng>
requires(ranges::sized_range<Rng>) class view_
    : public ranges::view_facade<view_<OutCharT, Rng>, ranges::finite>
{
    friend ranges::range_access;
    using iterator_t = ranges::iterator_t<Rng>;
    using value_type = OutCharT;

public:
    view_() = default;
    explicit view_(Rng && rng) noexcept
        : rng_{rng}
        , iter_{ranges::begin(rng)}
    {}

    constexpr inline bool equal(ranges::default_sentinel_t) const noexcept
    {
        return iter_ == ranges::end(rng_) && next_.empty();
    }

public:
    // Actual encoding logic
    inline value_type read() const noexcept
    {
        // Because our iterator may emit 1..4 positions for every read(), we cannot
        // easily separate read() and next() without taking a performance hit.
        //
        // As such, we'll const_cast ourselves to make our internal 'next' chars
        // writable.
        view_ * this_ = const_cast<view_ *>(this);
        return this_->read();
    };

    inline value_type read() noexcept
    {
        if (next_.empty() == true) [[likely]]
        {
            assert(iter_ != ranges::end(rng_));
            next_ = encode_(*(iter_++));
        };

        assert(next_.empty() == false);
        return next_.pop();
    };

    constexpr inline void next() noexcept {};

private:
    Rng rng_;
    iterator_t iter_;
    next_chars<OutCharT> next_;
    fn_<OutCharT> encode_;
};

template <typename OutCharT, class Rng>
view_<OutCharT, Rng> view(Rng && rng)
{
    return view_<OutCharT, Rng>{std::forward<Rng>(rng)};
}

template <class Rng>
view_<u8_type, Rng> utf8_view(Rng && rng)
{
    return view<u8_type, Rng>(std::forward<Rng>(rng));
}

}; // namespace encode

namespace decode
{

template <typename CharT>
struct fn_;

template <>
struct fn_<u8_type>
{
    template <ranges::input_iterator I>
    constexpr inline cp_type operator()(I & it) const noexcept
    {
        cp_type cp;

        if (it[0] < 0x80) [[likely]]
        {
            cp = *it++;
        }
        else if ((it[0] & 0xe0) == 0xc0)
        {
            cp = ((long)(it[0] & 0x1f) << 6) | ((long)(it[1] & 0x3f) << 0);
            it += 2;
        }
        else if ((it[0] & 0xf0) == 0xe0)
        {
            cp = ((long)(it[0] & 0x0f) << 12) | ((long)(it[1] & 0x3f) << 6)
                 | ((long)(it[2] & 0x3f) << 0);
            it += 3;
        }
        else if ((it[0] & 0xf8) == 0xf0 && (it[0] <= 0xf4))
        {
            cp = ((long)(it[0] & 0x07) << 18) | ((long)(it[1] & 0x3f) << 12)
                 | ((long)(it[2] & 0x3f) << 6) | ((long)(it[3] & 0x3f) << 0);
            it += 4;
        }
        else [[unlikely]]
        {
            cp = -1;
            ++it;
        }

        return cp;
    }
};

/**
 * UTF8->UTF32 decoding view
 *
 * Reads UTF8 and decodes it into codepoints (i.e. UTF32/UCS4). Effectively
 * used for conversion from qdb strings to numpy.
 */

template <typename InCharT, typename Rng>
requires(ranges::sized_range<Rng>) class view_
    : public ranges::view_facade<view_<InCharT, Rng>, ranges::finite>
{
    friend ranges::range_access;
    using iterator_t = ranges::iterator_t<Rng>;
    using value_type = cp_type;

    // Lifecycle
public:
    view_() = default;
    explicit view_(Rng && rng) noexcept
        : rng_{rng}
        , iter_{ranges::begin(rng)} {};

    constexpr inline ranges::range_size_t<Rng> size() const noexcept
    {
        return ranges::size(rng_);
    }

    constexpr inline bool equal(ranges::default_sentinel_t) const noexcept
    {
        return iter_ == ranges::end(rng_);
    }

public:
    // Actual iterator / decoding logic
    constexpr inline value_type read() const noexcept
    {
        // Because our iterator advances 1..4 positions for every read(), we cannot
        // easily separate read() and next() without taking a performance hit.
        //
        // As such, we'll const_cast ourselves to make our internal position
        // writable.
        view_ * this_ = const_cast<view_ *>(this);
        return this_->read();
    };

    constexpr inline value_type read() noexcept
    {
        return decode_(iter_);
    };

    constexpr inline void next() noexcept {};

    // General plumbing

private:
    Rng rng_;
    iterator_t iter_;
    fn_<InCharT> decode_{};
};

template <typename InCharT, typename Rng>
inline view_<InCharT, Rng> view(Rng && rng)
{
    return view_<InCharT, Rng>{std::forward<Rng>(rng)};
}

template <class Rng>
inline decltype(auto) utf8_view(Rng && rng)
{
    return view<u8_type>(std::forward<Rng>(rng));
}

}; // namespace decode

}; // namespace qdb::convert::unicode
