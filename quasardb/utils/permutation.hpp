#pragma once

#include "stable_sort.hpp"
#include <vector>

namespace utils
{

template <typename RandomAccessIterator1, typename RandomAccessIterator2>
void apply_permutation(RandomAccessIterator1 item_begin,
    RandomAccessIterator1 item_end,
    RandomAccessIterator2 ind_begin,
    RandomAccessIterator2 ind_end)
{
    using Diff = typename std::iterator_traits<RandomAccessIterator1>::difference_type;
    using std::swap;
    Diff size = std::distance(item_begin, item_end);
    for (Diff i = 0; i < size; i++)
    {
        auto current = i;
        while (i != ind_begin[current])
        {
            auto next = ind_begin[current];
            swap(item_begin[current], item_begin[next]);
            ind_begin[current] = current;
            current            = next;
        }
        ind_begin[current] = current;
    }
}

template <typename Range1, typename Range2>
void apply_permutation(Range1 & item_range, Range2 & ind_range)
{
    apply_permutation(
        std::begin(item_range), std::end(item_range), std::begin(ind_range), std::end(ind_range));
}

template <typename T, typename Compare>
std::vector<std::int64_t> sort_permutation(const std::vector<T> & vec, Compare && compare)
{
    std::vector<std::int64_t> order;
    order.resize(vec.size());
    std::iota(std::begin(order), std::end(order), 0);
    stable_sort(std::begin(order), std::end(order),
        [&](std::size_t i, std::size_t j) { return compare(vec[i], vec[j]); });
    return order;
}

} // namespace utils
