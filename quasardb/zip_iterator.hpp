#pragma once

#include "remove_cvref.hpp"
#include <tuple>
#include <type_traits>

template <typename T, std::size_t... I>
void tuple_swap_impl(T & rhs, T & lhs, std::index_sequence<I...> /*unused*/)
{
    (std::swap(std::get<I>(rhs), std::get<I>(lhs)), ...);
}

// we need a std::swap(std::tuple<T&...>, std::tuple<T&...>) in sort
// because we create a zip_iterator for both time_offsets and values
// we need to swap them both at the same time
namespace std
{

template <typename... T>
void swap(tuple<T &...> lhs, tuple<T &...> rhs)
{
    tuple_swap_impl(lhs, rhs, std::index_sequence_for<T...>{});
}

} // namespace std

namespace qdb
{
namespace detail
{

template <size_t N>
struct tuple_util
{

    template <typename TupleType, typename DifferenceType>
    static void increment(TupleType & tuple_it, DifferenceType forward)
    {
        std::apply(
            [forward](auto &... its) {
                auto advance = [forward](auto & it) {
                    auto forward_to = static_cast<typename std::iterator_traits<std::decay_t<decltype(it)>>::difference_type>(forward);
                    it              = std::next(it, forward_to);
                };
                (void)std::initializer_list<int>{(advance(its), 0)...};
            },
            tuple_it);
    }

    template <typename TupleType, typename DifferenceType>
    static bool check_sync(const TupleType & it1, const TupleType & it2, DifferenceType val)
    {
        if (std::get<N - 1>(it1) - std::get<N - 1>(it2) != val) return false;

        return tuple_util<N - 1>::check_sync(it1, it2, val);
    }
};

template <>
struct tuple_util<0>
{
    template <typename TupleType, typename DifferenceType>
    static void increment(TupleType &, DifferenceType)
    {}

    template <typename TupleType, typename DifferenceType>
    static bool check_sync(const TupleType &, const TupleType &, DifferenceType)
    {
        return true;
    }
};

template <typename TupleReturnType>
struct make_references
{
    template <typename TupleType, std::size_t... Is>
    TupleReturnType operator()(const TupleType & t, std::index_sequence<Is...>)
    {

        return std::tie((*std::get<Is>(t))...);
    }
};

} // namespace detail

template <typename... Types>
class zip_iterator
{
    static const std::size_t num_types = sizeof...(Types);

    using iterators = typename std::tuple<Types...>;

public:
    using difference_type   = std::ptrdiff_t;
    using value_type        = std::tuple<typename std::iterator_traits<remove_cvref_t<Types>>::value_type...>;
    using reference         = std::tuple<typename std::iterator_traits<remove_cvref_t<Types>>::reference...>;
    using pointer           = std::tuple<typename std::iterator_traits<remove_cvref_t<Types>>::pointer...>;
    using iterator_category = std::random_access_iterator_tag;

    zip_iterator() = default;

    explicit zip_iterator(Types... args)
        : _its(std::make_tuple(args...))
    {}

    zip_iterator(const zip_iterator & other)
        : _its(other._its)
    {}

    reference operator*() const
    {
        return detail::make_references<reference>()(_its, std::make_index_sequence<num_types>());
    }

    template <size_t I>
    decltype(auto) get()
    {
        return std::get<I>(_its);
    }

    template <size_t I>
    decltype(auto) get() const
    {
        return std::get<I>(_its);
    }

    reference operator[](difference_type i) const
    {
        return *(*this + i);
    }

    difference_type operator-(const zip_iterator & it) const
    {
        return std::get<0>(_its) - std::get<0>(it._its);
    }

    zip_iterator & operator+=(difference_type forward)
    {
        detail::tuple_util<num_types>::increment(_its, forward);

        return *this;
    }

    zip_iterator & operator-=(difference_type backward)
    {
        return *this += -backward;
    }

    zip_iterator & operator++()
    {
        return *this += 1;
    }

    zip_iterator & operator--()
    {
        return *this -= 1;
    }

    zip_iterator operator++(int)
    {
        zip_iterator it(*this);

        ++(*this);

        return it;
    }

    zip_iterator operator--(int)
    {
        zip_iterator it(*this);

        --(*this);

        return it;
    }

    zip_iterator operator-(difference_type backward) const
    {

        zip_iterator it(*this);

        return it -= backward;
    }

    zip_iterator operator+(difference_type forward) const
    {

        zip_iterator it(*this);

        return it += forward;
    }

    friend zip_iterator operator+(difference_type forward, const zip_iterator & it)
    {
        return it + forward;
    }

    bool operator==(const zip_iterator & it) const
    {

        return *this - it == 0;
    }

    bool operator!=(const zip_iterator & it) const
    {
        return !(*this == it);
    }

    bool operator<(const zip_iterator & it) const
    {
        return *this - it < 0;
    }

    bool operator>(const zip_iterator & it) const
    {
        return it < *this;
    }

    bool operator<=(const zip_iterator & it) const
    {
        return !(*this > it);
    }

    bool operator>=(const zip_iterator & it) const
    {
        return !(*this < it);
    }

private:
    iterators _its;
};

template <typename... T>
zip_iterator<T...> zip(T &&... args)
{
    return zip_iterator<T...>(std::forward<T>(args)...);
}

template <typename... T>
auto make_zip_iterator(T &&... args)
{
    return zip(std::forward<T>(args)...);
}

} // namespace qdb

namespace std
{

template <size_t I, typename... T>
inline decltype(auto) get(const qdb::zip_iterator<T...> & zit)
{
    return zit.template get<I>();
}

template <size_t I, typename... T>
inline decltype(auto) get(qdb::zip_iterator<T...> && zit)
{
    return zit.template get<I>();
}

} // namespace std
