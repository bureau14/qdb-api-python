#pragma once

#if defined(__cpp_deduction_guides)

template <class... Ts>
struct overload : Ts...
{
    using Ts::operator()...;
};

// Deduction guide.
template <class... Ts>
overload(Ts...) -> overload<Ts...>;

#else

template <typename... Ts>
struct overload_set;

template <typename T, typename... Ts>
struct overload_set<T, Ts...>
    : T
    , overload_set<Ts...>
{
    overload_set(T t, Ts... ts)
        : T(t)
        , overload_set<Ts...>(ts...)
    {}

    using T::operator();
    using overload_set<Ts...>::operator();
};

template <typename T>
struct overload_set<T> : T
{
    overload_set(T t)
        : T(t)
    {}

    using T::operator();
};

template <typename... Ts>
overload_set<Ts...> overload(Ts... ts)
{
    return overload_set<Ts...>(ts...);
}

#endif
