#pragma once

#include "../concepts.hpp"
#include <range/v3/range_fwd.hpp>
#include <range/v3/view/adaptor.hpp>
#include <range/v3/view/cache1.hpp>
#include <range/v3/view/transform.hpp>
#include <iostream>
#include <optional>

namespace qdb::convert::unicode
{

typedef std::uint32_t u32_type;
typedef std::uint8_t u8_type;

namespace detail
{

/**
 * General purpose buffer which holds the "next" characters. This is useful for
 * buffering purposes, e.g. when encoding codepoints to UTF-8, where advancing the
 * iterator forward by 1 can actually emit 4 characters.
 */
template <typename CharT, std::size_t Width = (sizeof(u32_type) / sizeof(CharT))>
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

    constexpr inline CharT top() const noexcept
    {
        assert(p_ < n_);
        return xs_[p_];
    };

    constexpr inline bool empty() const noexcept
    {
        return p_ == n_;
    };

    constexpr inline std::size_t size() const noexcept
    {
        return n_ - p_;
    }

private:
    std::array<CharT, Width> xs_{{}};
    std::size_t n_ = 0;
    std::size_t p_ = 0;
};

class code_point
{
private:
    std::uint32_t value_;

public:
    constexpr explicit code_point(std::uint32_t value)
        : value_{value} {};

    constexpr explicit code_point(std::uint32_t && value)
        : value_{std::move(value)} {};

    constexpr inline std::uint32_t get() const noexcept
    {
        return value_;
    }

    friend constexpr bool operator==(code_point const & lhs, code_point const & rhs)
    {
        return lhs.value_ == rhs.value_;
    }
    friend constexpr bool operator!=(code_point const & lhs, code_point const & rhs)
    {
        return !(lhs == rhs);
    }
};

static_assert(sizeof(code_point) == sizeof(std::uint32_t));

/**
 * Functor that accepts codepoints and emits encoded UTF characters.
 */
template <typename CharT>
struct encode_fn_;

/**
 * Functor for encoding codepoints to UTF-8.
 */
template <>
struct encode_fn_<u8_type>
{
    inline next_chars<u8_type> operator()(std::uint32_t cp) const noexcept
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

    inline next_chars<u8_type> operator()(detail::code_point cp) const noexcept
    {
        return operator()(cp.get());
    }
};

/**
 * Functor that accepts UTF encoded characters and yields codepoints.
 */
template <typename CharT>
struct decode_fn_;

/**
 * UTF-8 -> CodePoint functor.
 */
template <>
struct decode_fn_<u8_type>
{
    template <ranges::input_iterator I>
    inline detail::code_point operator()(I it) const noexcept
    {
        std::uint32_t cp;

        if (*it < 0x80) [[likely]]
        {
            cp = *it++;
        }
        else if ((*it & 0xe0) == 0xc0)
        {
            cp = ((long)(*it++ & 0x1f) << 6) | ((long)(*it++ & 0x3f) << 0);
        }
        else if ((*it & 0xf0) == 0xe0)
        {
            cp = ((long)(*it++ & 0x0f) << 12) | ((long)(*it++ & 0x3f) << 6)
                 | ((long)(*it++ & 0x3f) << 0);
        }
        else if ((*it & 0xf8) == 0xf0 && (*it <= 0xf4))
        {
            cp = ((long)(*it++ & 0x07) << 18) | ((long)(*it++ & 0x3f) << 12)
                 | ((long)(*it++ & 0x3f) << 6) | ((long)(*it++ & 0x3f) << 0);
        }
        else [[unlikely]]
        {
            cp = -1;
            ++it;
        }

        return detail::code_point{cp};
    }
};

/**
 * Counts how many UTF characters are represented by a code point.
 */
template <typename CharT>
struct count_fn_;

/**
 * Counts how many UTF-8 characters are represented by a code point.
 */
template <>
struct count_fn_<u8_type>
{
    inline std::size_t operator()(detail::code_point cp) const noexcept
    {
        if (cp.get() >= (1L << 16)) [[unlikely]]
        {
            return 4;
        }
        else if (cp.get() >= (1L << 11))
        {
            return 3;
        }
        else if (cp.get() >= (1L << 7))
        {
            return 2;
        }
        else [[likely]]
        {
            return 1;
        }
    }
};

/**
 * Skips as many UTF characters as necessary to read a single code point.
 */
template <typename CharT>
struct skip_fn_;

/**
 * Skips as many UTF-8 characters as necessary to read a single code point.
 */
template <>
struct skip_fn_<u8_type>
{
    template <ranges::input_iterator I>
    constexpr inline void operator()(I & it) const noexcept
    {
        if (*it < 0x80) [[likely]]
        {
            ranges::advance(it, 1);
        }
        else if ((*it & 0xe0) == 0xc0)
        {
            ranges::advance(it, 2);
        }
        else if ((*it & 0xf0) == 0xe0)
        {
            ranges::advance(it, 3);
        }
        else if ((*it & 0xf8) == 0xf0 && (*it <= 0xf4))
        {
            ranges::advance(it, 4);
        }
        else [[unlikely]]
        {
            ranges::advance(it, 1);
        }
    }
};

namespace utf8
{

/**
 * CodePoint -> UTF8 encoding view
 *
 * Reads codepoints and emits UTF8. Effectively used for conversion from numpy to qdb.
 */
template <typename Rng>
requires(concepts::forward_range_t<Rng, detail::code_point>) class encode_view_
    : public ranges::view_facade<encode_view_<Rng>, ranges::finite>
{
    friend ranges::range_access;
    using iterator_t = ranges::iterator_t<Rng>;
    using value_type = u8_type;

    /**
     * Use a separate cursor struct, so we can implement the range as a forward range (i.e.
     * make the iterators weakly comparable).
     */
    template <typename IterT, typename SentT>
    struct cursor
    {
    private:
        IterT iter_;
        SentT last_;
        next_chars<value_type> next_;
        encode_fn_<value_type> encode_;
        count_fn_<value_type> count_;

    public:
        cursor() = default;
        cursor(IterT iter, SentT last)
            : iter_{iter}
            , last_{last}
        {
            next();
        };

        inline value_type read() const noexcept
        {
            return next_.top();
        }

        constexpr inline void next() noexcept
        {
            if (next_.empty() == false) [[likely]]
            {
                next_.pop();
            }

            if (iter_ != last_ && next_.empty() == true) [[likely]]
            {
                next_ = encode_(*(iter_++));
            }
        };

        constexpr inline bool equal(cursor<IterT, SentT> const & rhs) const noexcept
        {
            return iter_ == rhs.iter_ && next_.empty() == rhs.next_.empty();
        }

        constexpr inline bool equal(ranges::default_sentinel_t) const noexcept
        {
            return iter_ == last_ && next_.empty();
        }

        inline ranges::iter_difference_t<IterT> distance_to(ranges::default_sentinel_t) const
        {
            // This is a hack, but we initialize the return value to `1` if we already read
            // the next token in our constructor.
            ranges::iter_difference_t<IterT> ret(next_.size());

            // And finally, let's parse all the remaining tokens
            for (IterT cur = iter_; cur != last_; ++cur)
            {
                ret += count_(*cur);
            }

            return ret;
        }
    };

public:
    encode_view_() = default;
    explicit encode_view_(Rng && rng) noexcept
        : rng_{rng}
    {}

    inline decltype(auto) begin_cursor() const
    {
        auto beg = ranges::begin(rng_);
        auto end = ranges::end(rng_);
        return cursor<decltype(beg), decltype(end)>{beg, end};
    }

public:
private:
    Rng rng_;
};

/**
 * UTF8->CodePoint decoding view
 *
 * Reads UTF8 and decodes it into codepoints (i.e. UTF32/UCS4). Effectively
 * used for conversion from qdb strings to numpy.
 */

template <typename Rng>
requires(ranges::forward_range<Rng>) class decode_view_
    : public ranges::view_facade<decode_view_<Rng>, ranges::finite>
{
    friend ranges::range_access;
    using iterator_t = ranges::iterator_t<Rng>;
    using value_type = detail::code_point;

    /**
     * Use a separate cursor struct, so we can implement the range as a forward range (i.e.
     * make the iterators weakly comparable).
     */
    template <typename IterT, typename SentT>
    struct cursor
    {
    private:
        IterT iter_;
        SentT last_;
        decode_fn_<u8_type> decode_{};
        skip_fn_<u8_type> skip_{};

    public:
        cursor() = default;
        cursor(IterT iter, SentT last)
            : iter_{iter}
            , last_{last} {};

        inline value_type read() const noexcept
        {
            // XXX(leon)
            // This is somewhat inefficient, as we're peeking ahead trying to find the "next"
            // codepoint, which may actually iterate forward more than one step.
            //
            // Then, when next() is requested, we _actually_ move ahead, and have to do the same
            // (similar) checks again.
            //
            // However, after too much edge cases and debugging, I conclude that in order to be
            // "really" compatible with ranges, you *must* separate these two, there's no way around
            // it.
            return decode_(iter_);
        };

        constexpr inline void next() noexcept
        {
            skip_(iter_);
        };

        constexpr inline bool equal(cursor<IterT, SentT> const & rhs) const noexcept
        {
            return iter_ == rhs.iter_;
        }

        constexpr inline bool equal(ranges::default_sentinel_t) const noexcept
        {
            return iter_ == last_;
        }

        inline ranges::iter_difference_t<iterator_t> distance_to(ranges::default_sentinel_t) const
        {
            return _distance_to(last_);
        }

    private:
        template <typename T>
        inline ranges::iter_difference_t<iterator_t> _distance_to(T const & other) const
        {
            ranges::iter_difference_t<iterator_t> ret{0};

            for (IterT cur = iter_; cur != last_; skip_(cur))
            {
                ++ret;
            }

            return ret;
        }
    };

    // Lifecycle
public:
    decode_view_() = default;
    explicit decode_view_(Rng && rng) noexcept
        : rng_{rng} {};

    inline decltype(auto) begin_cursor() const
    {
        auto beg = ranges::begin(rng_);
        auto end = ranges::end(rng_);

        return cursor<decltype(beg), decltype(end)>{beg, end};
    }

private:
    Rng rng_;
};

}; // namespace utf8

}; // namespace detail

namespace utf8
{

struct encode_view_base_fn
{
    template <typename Rng>
    constexpr detail::utf8::encode_view_<Rng> operator()(Rng && rng) const
    {
        return detail::utf8::encode_view_<Rng>{std::forward<Rng>(rng)};
    }
};

struct encode_view_bind_fn
{
    constexpr auto operator()() const
    {
        return ranges::make_view_closure(encode_view_base_fn{});
    }
};

struct encode_view_fn
    : encode_view_base_fn
    , encode_view_bind_fn
{
    using encode_view_base_fn::operator();
    using encode_view_bind_fn::operator();
};

RANGES_INLINE_VARIABLE(encode_view_fn, encode_view)

struct decode_view_base_fn
{
    /**
     * Default case: our input range already has a 'perfect' u8_type (unsigned char)
     */
    template <typename Rng>
    requires(concepts::range_t<Rng, u8_type>) constexpr detail::utf8::decode_view_<Rng> operator()(
        Rng && rng) const
    {
        return detail::utf8::decode_view_<Rng>{std::forward<Rng>(rng)};
    }

    /**
     * Non-default case, which happens when e.g. using a std::string as a range: the underlying type is
     * not exactly u8_type, but we can easily convert it to the appropriate type.
     *
     * This avoids a case where signed chars were interpreted as unsigned chars and all kinds of
     * shenanigans, which is fixed here.
     */
    template <typename Rng>
    requires(
        !concepts::range_t<Rng,
            u8_type> && std::is_nothrow_convertible_v<ranges::range_value_t<Rng>, u8_type>) constexpr decltype(auto)
    operator()(Rng && rng) const
    {
        auto xform_fn = [](auto x) -> u8_type { return static_cast<u8_type>(x); };

        // Transform and delegate to the 'regular' operator()
        return operator()(ranges::views::transform(rng, xform_fn));
    }
};

struct decode_view_bind_fn
{
    constexpr auto operator()() const
    {
        return ranges::make_view_closure(decode_view_base_fn{});
    }
};

struct decode_view_fn
    : decode_view_base_fn
    , decode_view_bind_fn
{
    using decode_view_base_fn::operator();
    using decode_view_bind_fn::operator();
};

RANGES_INLINE_VARIABLE(decode_view_fn, decode_view)

}; // namespace utf8

namespace utf32
{

/**
 * Returns view that reads codepoints and emits UTF-32 characters.
 */
inline decltype(auto) encode_view()
{
    auto xform_fn = [](detail::code_point x) -> u32_type { return static_cast<u32_type>(x.get()); };
    return ranges::views::transform(xform_fn);
}

/**
 * Returns view that reads codepoints and emits UTF-32 characters.
 */
template <class Rng>
requires(concepts::input_range_t<Rng, detail::code_point>) inline decltype(auto) encode_view(Rng && rng)
{
    return rng | encode_view();
}

/**
 * Returns view that reads UTF-32 characters and emits codepoints.
 */
inline decltype(auto) decode_view()
{
    auto xform_fn = [](u32_type x) -> detail::code_point { return detail::code_point{x}; };
    return ranges::views::transform(xform_fn);
}

/**
 * Returns view that reads UTF-32 characters and emits codepoints.
 */
template <typename Rng>
inline decltype(auto) decode_view(Rng && rng)
{
    return rng | decode_view();
}

}; // namespace utf32

}; // namespace qdb::convert::unicode
