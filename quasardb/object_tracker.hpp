#pragma once

#include <any>
#include <cassert>
#include <iostream>
#include <memory>
#include <stack>
#include <typeindex>
#include <unordered_map>
#include <unordered_set>

namespace qdb::object_tracker
{

////////////////////////////////////////////////////////////////////////////////
//
// Implementation
//
///////////////////

namespace detail
{

////////////////////////////////////////////////////////////////////////////////
//
// Trackable objects
//
///////////////////
//
// Typically we don't really care about the type of the underlying pointers
// we track, and would like to just use `void *`; this makes the code a lot
// simpler.
//
// There's a problem, however: we cannot invoke `delete` on void * because
// the compiler wouldn't know which destructor to call.
//
// The code below deal with this mess: it's able to "wrap" any typed unique_ptr
// in something which can best be considered a std::unique_ptr<void>, but
// uses the original, type-based deleter.
//
// Not sure if evil hack or elaborate genius, but it works.
//
///////////////////

using void_delete_fn = std::function<void(void *)>;

template <typename T, typename DT>
inline auto deleter_of() noexcept -> void_delete_fn
{
    // Magic is here: we make a `void *` deleter function, which under-the-hood
    // invokes the original deleter.
    return [](void * x) -> void { DT{}(static_cast<T *>(x)); };
};

using trackable_object = std::unique_ptr<void, void_delete_fn>;

template <typename T, typename DT>
inline trackable_object make_trackable_object(std::unique_ptr<T, DT> && x)
{
    return trackable_object{x.release(), deleter_of<T, DT>()};
};

template <typename T, typename DT = std::default_delete<T>>
inline trackable_object make_trackable_object(T * x)
{
    return make_trackable_object(std::unique_ptr<T>{x});
};

////////////////////////////////////////////////////////////////////////////////
//
// Repository
//
///////////////////
//
// The repository contains all tracked objects. It maintains a container of tracked
// objects, which are effectively unique_ptr<void>, and as such automatically released
// upon destruction.
//
// Intended use is to immediately pass the pointer to immediately track:
//
//   char * buf = qdb::object_tracker::track<char>(new char[32]);
//
// or, even better:
//
//   char * buf = qdb::object_tracker::alloc<char>(32);
//
///////////////////

class repository
{
private:
    using container_t = std::vector<trackable_object>;

public:
    repository()  = default;
    ~repository() = default;

    repository(repository const &)             = delete;
    repository(repository && other)            = delete;
    repository & operator=(repository const &) = delete;

public:
    [[nodiscard]] static inline repository & instance() noexcept
    {
        static repository instance_;
        // If this assertion fails it implies a component tries to have its objects
        // tracked but there is no collector being able to track these yet.
        //
        // Ensure you have a `collector` with its scoped wrapped around the rest of
        // the code that wants its objects tracked.
        return instance_;
    };

    /**
     * Swaps the tracked objects of `this` repo with the tracked objects of another
     * repository.
     *
     * Used by `scoped_repository` to temporarily "claim" the global scope, and to
     * release it back again later.
     */
    static inline void swap(repository & x) noexcept
    {
        std::swap(instance().xs_, x.xs_);
    };

public:
    // Delegate functions

    [[nodiscard]] inline void * track(trackable_object && x) noexcept
    {
        void * ptr = x.get();
        xs_.push_back(std::move(x));

        return ptr;
    };

    template <typename T>
        requires(!std::is_pointer_v<T>)
    [[nodiscard]] inline T * track(T * x) noexcept
    {
        return static_cast<T *>(track(make_trackable_object(std::unique_ptr<T>{x})));
    };

    template <typename T>
    [[nodiscard]] inline T * alloc(std::size_t n) noexcept
    {
        return track<T>(static_cast<T *>(std::malloc(n)));
    };

public:
    // Inspection / management functions

    [[nodiscard]] inline std::size_t size() const noexcept
    {
        return xs_.size();
    };

    [[nodiscard]] inline bool empty() const noexcept
    {
        return xs_.empty();
    };

private:
    container_t xs_;
};

////////////////////////////////////////////////////////////////////////////////
//
// Scoped Repository
//
///////////////////
//
// Wraps a repository that automatically releases everything when it goes out
// of scope.
//
// Intended use case:
// * Attach a scoped_repository to the object that initiates a query/request, *and*
//   stays alive as long as any pointers are still being referenced.
// * In conjunction with `scoped_capture`, captures all tracked pointers during
//   its scope;
// * only once e.g. the scoped_repository goes completely out of scope, does the cleanup
//   happen.
//
// You would attach this scoped_repository to e.g. a `query_result` structure.
//
///////////////////

class scoped_repository
{
    friend class scoped_capture;

public:
    scoped_repository() = default;

    ~scoped_repository() = default;

    [[nodiscard]] inline std::size_t size() const noexcept
    {
        return repo_.size();
    };

    [[nodiscard]] inline bool empty() const noexcept
    {
        return repo_.empty();
    };

    inline void swap()
    {
        repository::swap(repo_);
    };

private:
    repository repo_;
};

////////////////////////////////////////////////////////////////////////////////
//
// Scoped Capture
//
///////////////////
//
// During its scope, redirects all new tracked objects to the provided repository.
// You would typically put this around the code that invokes conversion or any
// other components that can create tracked objects.
//
// *WARNING*: Multiple scoped_captures being active at the same time is unsupported.
//            Keep these scopes as small as possible, and do *not* use them in async
//            code.
//
///////////////////

class scoped_capture
{
public:
    inline scoped_capture(scoped_repository & ctx)
        : ctx_{ctx}
    {
        // This is where the magic happens: we 'claim' the global scope by swapping
        // our local scope with the global one...
        ctx_.swap();
    };

    inline ~scoped_capture()
    {
        // ... and we release it back by putting the global scope back in place.
        ctx_.swap();
    };

private:
    scoped_repository & ctx_;
};
}; // namespace detail

////////////////////////////////////////////////////////////////////////////////
//
// Public interface
//
///////////////////

template <typename T>
inline T * track(T * x)
{
    return detail::repository::instance().track<T>(x);
};

template <typename T, typename DT>
inline T * track(std::unique_ptr<T, DT> && x)
{
    return detail::repository::instance().track<T>(std::forward<std::unique_ptr<T, DT> &&>(x));
};

template <typename T>
inline T * alloc(std::size_t n)
{
    return detail::repository::instance().alloc<T>(n);
};

using scoped_repository = qdb::object_tracker::detail::scoped_repository;
using scoped_capture    = qdb::object_tracker::detail::scoped_capture;
}; // namespace qdb::object_tracker
