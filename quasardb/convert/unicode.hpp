#pragma once

#include <range/v3/range_fwd.hpp>
#include <range/v3/view/cache1.hpp>
#include <range/v3/view_adaptor.hpp>

namespace qdb::convert::unicode
{

typedef std::uint32_t cp_type;
typedef std::uint32_t u32_type; // UTF32 == CodePoint
typedef std::uint8_t u8_type;

namespace encode
{

template <typename CharT>
struct fn_;

// codepoint -> utf8
template <>
struct fn_<u8_type>
{
    template <typename I>
    requires(ranges::output_iterator<I, u8_type>) inline I operator()(cp_type cp, I out) const noexcept
    {
        if (cp >= (1L << 16))
        {
            *out++ = 0xf0 | (cp >> 18);
            *out++ = 0x80 | ((cp >> 12) & 0x3f);
            *out++ = 0x80 | ((cp >> 6) & 0x3f);
            *out++ = 0x80 | ((cp >> 0) & 0x3f);
        }
        else if (cp >= (1L << 11))
        {
            *out++ = 0xe0 | (cp >> 12);
            *out++ = 0x80 | ((cp >> 6) & 0x3f);
            *out++ = 0x80 | ((cp >> 0) & 0x3f);
        }
        else if (cp >= (1L << 7))
        {
            *out++ = 0xc0 | (cp >> 6);
            *out++ = 0x80 | ((cp >> 0) & 0x3f);
        }
        else [[likely]]
        {
            *out++ = cp;
        }

        return out;
    }
};

/**
 * UTF32->UTF8 encoding view
 *
 * Reads codepoints (i.e. UTF32/UCS4) and emits UTF8. Effectively used for
 * conversion from numpy to qdb.
 */
template <class Rng, typename CharT>
class view_ : public ranges::view_adaptor<view_<Rng, CharT>, Rng, ranges::finite>
{

public:
    view_() = default;
    view_(Rng && rng) noexcept
        : view_::view_adaptor{std::forward<Rng>(rng)}
        , buf_n_{0}
        , buf_idx_{0}
    {}

private:
    friend ranges::range_access;

    static constexpr std::size_t CharsPerCP = sizeof(cp_type) / sizeof(CharT);
    fn_<CharT> encode_{};

    class adaptor : public ranges::adaptor_base
    {
    public:
        adaptor() = default;
        constexpr adaptor(view_ * rng) noexcept
            : rng_{rng} {};

        constexpr inline CharT read(ranges::iterator_t<Rng> it) const noexcept
        {
            return rng_->_satisfy_read(it);
        }

        constexpr inline void next(ranges::iterator_t<Rng> & it) noexcept
        {
            rng_->_satisfy_next(it);
        };

    private:
        view_ * rng_;
    };

    adaptor begin_adaptor() noexcept
    {
        _cache_begin();
        return {this};
    }

    adaptor end_adaptor() const noexcept
    {
        return {};
    }

    adaptor end_adaptor() noexcept
    {
        return {this};
    }

    constexpr inline void _cache_begin() noexcept
    {
        assert(buf_n_ == 0);
        assert(buf_idx_ == 0);

        if (ranges::empty(this->base()) == true) [[unlikely]]
        {
            return;
        };

        auto it = ranges::begin(this->base());
        _satisfy_next(it);

        begin_.emplace(std::move(it));
    };

    constexpr inline CharT _satisfy_read(ranges::iterator_t<Rng> it) const noexcept
    {
        assert(buf_idx_ <= buf_n_);
        return buf_[buf_idx_];
    };

    constexpr inline void _satisfy_next(ranges::iterator_t<Rng> & it) noexcept
    {
        assert(buf_idx_ <= buf_n_);

        if (buf_n_ == buf_idx_)
        {
            // Read next codepoint into 1..4 UTF-8 chars
            CharT * end_ = encode_(*(it++), buf_);

            buf_n_   = (std::distance(buf_, end_) - 1);
            buf_idx_ = 0;
        }
        else
        {
            ++buf_idx_;
        };
    };

private:
    ranges::detail::non_propagating_cache<ranges::iterator_t<Rng>> begin_;

    CharT buf_[CharsPerCP];
    std::size_t buf_n_;
    std::size_t buf_idx_;
};

template <class Rng, typename CharT>
view_<Rng, CharT> view(Rng && rng)
{
    return {std::forward<Rng>(rng)};
}

template <class Rng>
view_<Rng, u8_type> utf8_view(Rng && rng)
{
    return view<Rng, u8_type>(std::forward<Rng>(rng));
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
    constexpr inline void operator()(I & it, cp_type & cp) const noexcept
    {
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
            cp = -1; // itvalid
            ++it;    // itkip thiit byte
        }
    }
};

/**
 * UTF8->UTF32 decoding view
 *
 * Reads UTF8 and decodes it into codepoints (i.e. UTF32/UCS4). Effectively
 * used for conversion from qdb strings to numpy.
 */

template <class Rng, typename CharT>
class view_ : public ranges::view_adaptor<view_<Rng, CharT>, Rng, ranges::finite>
{

public:
    view_() = default;
    view_(Rng && rng) noexcept
        : view_::view_adaptor{std::forward<Rng>(rng)}
    {}

    constexpr inline ranges::range_size_t<Rng> size() const noexcept
    {
        return ranges::size(this->base());
    }

private:
    friend ranges::range_access;
    fn_<CharT> decode_{};

    class adaptor : public ranges::adaptor_base
    {
    public:
        adaptor() = default;
        constexpr adaptor(view_ * rng) noexcept
            : rng_{rng} {};

        constexpr inline cp_type read(ranges::iterator_t<Rng> it) const noexcept
        {
            return rng_->cp_;
        }

        constexpr inline void next(ranges::iterator_t<Rng> & it) noexcept
        {
            rng_->_satisfy_next(it);
        };

    private:
        view_ * rng_;
    };

    adaptor begin_adaptor() noexcept
    {
        _cache_begin();
        return {this};
    }

    adaptor end_adaptor() const noexcept
    {
        return {};
    }

    adaptor end_adaptor() noexcept
    {
        return {this};
    }

    constexpr inline void _cache_begin() noexcept
    {
        if (ranges::empty(this->base()) == true) [[unlikely]]
        {
            return;
        };

        auto it = ranges::begin(this->base());
        _satisfy_next(it);

        begin_.emplace(std::move(it));
    };

    constexpr inline void _satisfy_next(ranges::iterator_t<Rng> & it) noexcept
    {
        decode_(it, cp_);
    };

private:
    ranges::detail::non_propagating_cache<ranges::iterator_t<Rng>> begin_;
    cp_type cp_;
};

template <class Rng, typename CharT>
view_<Rng, CharT> view(Rng && rng)
{
    return {std::forward<Rng>(rng)};
}

template <class Rng>
view_<Rng, u8_type> utf8_view(Rng && rng)
{
    return view<Rng, u8_type>(std::forward<Rng>(rng));
}

}; // namespace decode

}; // namespace qdb::convert::unicode
