
namespace qdb
{
namespace detail
{

    struct init_reverse {};
    struct init_end {};

}
}

namespace qdb
{
namespace detail
{
    class const_iterator_impl
    {
    public:
        // might not look super efficient, but using raw pointers would result in dangling pointers
        // in too many scenarii
        typedef std::pair<std::string, api_buffer_ptr> value_type;

    public:
        // initialize to begin
        explicit const_iterator_impl(qdb_handle_t h);
        // initializes as reverse begin (final entry)
        const_iterator_impl(qdb_handle_t h, init_reverse);
        // initializes as end iterator
        const_iterator_impl(qdb_handle_t h, init_end);

    public:
        // copy construction and assignment, we need to do a low level copy of the iterator
        // otherwise pointers will be dangling...
        const_iterator_impl(const const_iterator_impl & orig);

    public:
        ~const_iterator_impl(void);

    public:
        void close(void);

    private:
        template <typename BeginFunction, typename NextFunction>
        const_iterator_impl & iterate(BeginFunction begin, NextFunction next);

    public:
        const_iterator_impl & next(void);
        const_iterator_impl & previous(void);

    public:
        bool operator == (const const_iterator_impl & it) const;

    public:
        const value_type & value(void) const;
        value_type * value_ptr(void) const;

    public:
        qdb_error_t last_error(void) const;

    public:
        bool end(void) const;
        bool valid(void) const;

    private:
        qdb_handle_t _handle;
        bool _end;
        qdb_const_iterator_t _iterator;

    private:
        mutable qdb_error_t _last_error;
        // lazily updated
        mutable value_type _value;
    };

    class const_iterator_base 
    {
    public:
        typedef const_iterator_impl::value_type value_type;

    protected:
        explicit const_iterator_base(qdb_handle_t h);
        const_iterator_base(qdb_handle_t h, const_iterator_impl::init_reverse v);
        const_iterator_base(qdb_handle_t h, const_iterator_impl::init_end v);

    protected:
        // we don't want reverse to compare with forward, we therefore make this protected
        // and use a name which is more convenient to explicitely call than an operator
        bool equals(const const_iterator_base & it) const;

    protected:
        const_iterator_base & next(void);
        const_iterator_base & previous(void);

    public:
        // we return the alias/content pair, content will be 0 if we cannot access it
        const value_type & operator * () const;
        value_type * operator -> () const;

    public:
        // needed by the Python API
        void close(void);

    public:
        qdb_error_t last_error(void) const;
        bool valid(void) const;

    private:
        const_iterator_impl _impl;
    };

}

    class const_iterator : public detail::const_iterator_base
    {
        // can only construct from handle class
        friend class handle;

    private:
        explicit const_iterator(qdb_handle_t h);
        // make end iterator
        const_iterator(qdb_handle_t h, detail::const_iterator_impl::init_end v);

    public:
        // if you compare with a reverse iterator, it will not compile, which is what we want
        bool operator == (const const_iterator & other) const;
        bool operator != (const const_iterator & other) const;
    };

    class const_reverse_iterator : public detail::const_iterator_base
    {
        // can only construct from handle class
        friend class handle;

    private:
        explicit const_reverse_iterator(qdb_handle_t h);
        const_reverse_iterator(qdb_handle_t h, detail::const_iterator_impl::init_end v);

    public:
        // if you compare with a forward iterator, it will not compile, which is what we want
        bool operator == (const const_reverse_iterator & other) const;
        bool operator != (const const_reverse_iterator & other) const;
    };


}

%{
namespace qdb
{
namespace detail
{
    typedef const_iterator_impl::init_reverse init_reverse;
    typedef const_iterator_impl::init_end init_end;
}
}
%}