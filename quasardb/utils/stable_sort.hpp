#pragma once

#include <algorithm>
#include <functional>

namespace utils
{

using std::stable_sort;

template <class BidirContainer, class BinaryOp>
void stable_sort(BidirContainer & c, BinaryOp && comparer)
{
    using std::begin;
    using std::end;
    stable_sort(begin(c), end(c), comparer);
}

template <class BidirContainer>
void stable_sort(BidirContainer & c)
{
    stable_sort(c, std::less<typename BidirContainer::value_type>{});
}

} // namespace utils