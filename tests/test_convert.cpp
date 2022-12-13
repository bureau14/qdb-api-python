
#include "conftest.hpp"
#include <convert/unicode.hpp>
#include <range/v3/all.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/traits.hpp>
#include <array>
#include <iostream>
#include <iterator>
#include <module.hpp>
#include <random>
#include <string>
#include <type_traits>

namespace qdb
{

typedef qdb::convert::unicode::detail::code_point cp_type;
typedef qdb::convert::unicode::u32_type u32_type;
typedef qdb::convert::unicode::u8_type u8_type;

template <typename T>
T gen_char()
{
    std::array<std::pair<T, T>, 13> valid_ranges = {std::make_pair(0x0021, 0x0021),
        std::make_pair(0x0023, 0x0026), std::make_pair(0x0028, 0x007E), std::make_pair(0x00A1, 0x00AC),
        std::make_pair(0x00AE, 0x00FF), std::make_pair(0x0100, 0x017F), std::make_pair(0x0180, 0x024F),
        std::make_pair(0x2C60, 0x2C7F), std::make_pair(0x16A0, 0x16F0), std::make_pair(0x0370, 0x0377),
        std::make_pair(0x037A, 0x037E), std::make_pair(0x0384, 0x038A), std::make_pair(0x038C, 0x038C)};

    auto n = valid_ranges.size();

    std::random_device rd;
    std::mt19937 gen(rd());

    std::uniform_int_distribution<> range_dis(0, n - 1);
    auto [beg, end] = valid_ranges.at(range_dis(gen));

    std::uniform_int_distribution<> char_dis(static_cast<u32_type>(beg), static_cast<u32_type>(end));

    return char_dis(gen);
}

inline std::u32string u32_input(std::size_t n)
{
    std::u32string ret;
    ret.resize(n);

    for (std::size_t i = 0; i < n; ++i)
    {
        ret.at(i) = gen_char<u32_type>();
    }

    return ret;
}

inline std::u32string u32_input()
{
    return u32_input(32);
}

inline std::string u8_input()
{
    return {"Ᵽ΅ģeȵƿĕĮ@n!"};
}

QDB_REGISTER_MODULE(test_convert, m)
{
    auto m_ = m.def_submodule("test_convert");

    m_.def("unicode_u32_decode_traits_test", []() -> void {
        auto utf32 = u32_input();
        auto xs    = utf32 | qdb::convert::unicode::utf32::decode_view();
        static_assert(std::is_same_v<ranges::range_value_t<decltype(xs)>, cp_type>);
    });

    m_.def("unicode_u8_encode_traits_test", []() -> void {
        auto input = u32_input();
        auto xs    = input | qdb::convert::unicode::utf32::decode_view()
                  | qdb::convert::unicode::utf8::encode_view();

        static_assert(std::is_same_v<ranges::range_value_t<decltype(xs)>, u8_type>);
        static_assert(ranges::range<decltype(xs)>);
        static_assert(ranges::input_range<decltype(xs)>);
        static_assert(ranges::forward_range<decltype(xs)>);
        static_assert(ranges::sized_range<decltype(xs)>);
    });

    m_.def("unicode_u8_decode_traits_test", []() -> void {
        auto input = u32_input();
        auto xs    = input | qdb::convert::unicode::utf32::decode_view()
                  | qdb::convert::unicode::utf8::encode_view()
                  | qdb::convert::unicode::utf8::decode_view()
                  | qdb::convert::unicode::utf32::encode_view();

        static_assert(std::is_same_v<ranges::range_value_t<decltype(xs)>, u32_type>);
        static_assert(ranges::range<decltype(xs)>);
        static_assert(ranges::input_range<decltype(xs)>);
        static_assert(ranges::forward_range<decltype(xs)>);
        static_assert(ranges::sized_range<decltype(xs)>);
    });

    m_.def("unicode_u8_recode_test", []() -> void {
        auto utf8        = u8_input();
        auto codepoints  = utf8 | qdb::convert::unicode::utf8::decode_view();
        auto utf32       = codepoints | qdb::convert::unicode::utf32::encode_view();
        auto codepoints_ = utf32 | qdb::convert::unicode::utf32::decode_view();
        auto utf8_       = codepoints_ | qdb::convert::unicode::utf8::encode_view();

        TEST_CHECK_EQUAL(ranges::size(codepoints), ranges::size(utf32));
        TEST_CHECK_EQUAL(ranges::size(utf8), ranges::size(utf8_));
        TEST_CHECK_GTE(ranges::size(utf8), ranges::size(codepoints));
        TEST_CHECK(ranges::equal(codepoints, codepoints_));
    });

    m_.def("unicode_decode_algo_test", []() -> void {
        // Validates some common range algorithms work as expected with u8 decoded ranges.
        // This mostly validates edge cases and increases the surface area of how we use
        // the range.
        auto input = u32_input(32);
        TEST_CHECK_EQUAL(ranges::size(input), 32);

        // Bullshit test, but: verify we can copy the input range
        std::u32string input_ = ranges::to<std::u32string>(input);
        TEST_CHECK_EQUAL(ranges::size(input), ranges::size(input_));
        TEST_CHECK(ranges::equal(input, input_));

        // Now recode
        auto codepoints = input | qdb::convert::unicode::utf32::decode_view()
                          | qdb::convert::unicode::utf8::encode_view()
                          | qdb::convert::unicode::utf8::decode_view()
                          | qdb::convert::unicode::utf32::encode_view();

        TEST_CHECK_EQUAL(ranges::size(input), ranges::size(codepoints));
        TEST_CHECK(ranges::equal(codepoints, input));

        std::u32string codepoints_ = ranges::to<std::u32string>(codepoints);

        TEST_CHECK_EQUAL(ranges::size(codepoints), ranges::size(codepoints_));
        TEST_CHECK(ranges::equal(codepoints, codepoints_));
    });
}

}; // namespace qdb
