#pragma once

#include "remove_cvref.hpp"
#include <iterator>

// we need a std::swap(std::tuple<T&...>, std::tuple<T&...>) in sort
// because we create a zip_iterator for both time_offsets and values
// we need to swap them both at the same time
namespace std
{

template <typename T>
void swap(tuple<qdb_timespec_t &, T &> lhs, tuple<qdb_timespec_t &, T &> rhs)
{
    std::swap(std::get<0>(lhs), std::get<0>(rhs));
    std::swap(std::get<1>(lhs), std::get<1>(rhs));
}

} // namespace std

namespace qdb
{

template <typename T>
class ts_iterator
{
    using iterators = typename std::tuple<std::vector<qdb_timespec_t>::iterator, typename std::vector<T>::iterator>;

public:
    using difference_type   = std::ptrdiff_t;
    using value_type        = std::tuple<qdb_timespec_t, T>;
    using reference         = std::tuple<qdb_timespec_t &, T &>;
    using pointer           = std::tuple<qdb_timespec_t *, T *>;
    using iterator_category = std::random_access_iterator_tag;

    ts_iterator() = default;

    explicit ts_iterator(std::vector<qdb_timespec_t>::iterator it_ts, typename std::vector<T>::iterator it_vs)
        : _its(std::make_tuple(it_ts, it_vs))
    {}

    ts_iterator(const ts_iterator & other)
        : _its(other._its)
    {}

    reference operator*() const
    {
        return std::tie((*std::get<0>(_its)), (*std::get<1>(_its)));
    }

    template <size_t I>
    decltype(auto) get()
    {
        static_assert(I < 2, "index must be less than 2");
        return std::get<I>(_its);
    }

    template <size_t I>
    decltype(auto) get() const
    {
        static_assert(I < 2, "index must be less than 2");
        return std::get<I>(_its);
    }

    reference operator[](difference_type i) const
    {
        return *(*this + i);
    }

    difference_type operator-(const ts_iterator & it) const
    {
        return std::get<0>(_its) - std::get<0>(it._its);
    }

    ts_iterator & operator+=(difference_type forward)
    {
        auto advance = [forward](auto & it) {
            auto forward_to = static_cast<typename std::iterator_traits<std::decay_t<decltype(it)>>::difference_type>(forward);
            it              = std::next(it, forward_to);
        };
        advance(std::get<0>(_its));
        advance(std::get<1>(_its));

        return *this;
    }

    ts_iterator & operator-=(difference_type backward)
    {
        return *this += -backward;
    }

    ts_iterator & operator++()
    {
        return *this += 1;
    }

    ts_iterator & operator--()
    {
        return *this -= 1;
    }

    ts_iterator operator++(int)
    {
        ts_iterator it(*this);

        ++(*this);

        return it;
    }

    ts_iterator operator--(int)
    {
        ts_iterator it(*this);

        --(*this);

        return it;
    }

    ts_iterator operator-(difference_type backward) const
    {

        ts_iterator it(*this);

        return it -= backward;
    }

    ts_iterator operator+(difference_type forward) const
    {

        ts_iterator it(*this);

        return it += forward;
    }

    friend ts_iterator operator+(difference_type forward, const ts_iterator & it)
    {
        return it + forward;
    }

    bool operator==(const ts_iterator & it) const
    {

        return *this - it == 0;
    }

    bool operator!=(const ts_iterator & it) const
    {
        return !(*this == it);
    }

    bool operator<(const ts_iterator & it) const
    {
        return *this - it < 0;
    }

    bool operator>(const ts_iterator & it) const
    {
        return it < *this;
    }

    bool operator<=(const ts_iterator & it) const
    {
        return !(*this > it);
    }

    bool operator>=(const ts_iterator & it) const
    {
        return !(*this < it);
    }

private:
    iterators _its;
};

template <size_t I, typename T>
inline decltype(auto) get(const qdb::ts_iterator<T> & it)
{
    return it.template get<I>();
}

} // namespace qdb

template <typename Iterator>
auto make_ts_iterator(std::vector<qdb_timespec_t>::iterator ts_it, Iterator vs_it)
{
    using value_type = typename std::iterator_traits<Iterator>::value_type;
    return qdb::ts_iterator<value_type>(ts_it, vs_it);
}
