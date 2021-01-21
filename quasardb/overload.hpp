#pragma once

template <class... Ts>
struct overload : Ts...
{
    using Ts::operator()...;
};

// Deduction guide.
template <class... Ts>
overload(Ts...) -> overload<Ts...>;

