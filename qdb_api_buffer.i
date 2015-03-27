namespace qdb
{

class api_buffer
{

public:
    api_buffer(qdb_handle_t h, const char * data, size_t length);
    ~api_buffer(void);

private:
    // prevent copy
    api_buffer(const api_buffer &);

public:
    bool operator == (const api_buffer & other) const;
    bool operator != (const api_buffer & other) const;

public:
    const char * data(void) const;
    size_t size(void) const;

private:
    const qdb_handle_t _handle;
    const char * const _data;
    const size_t _length;
};

} // namespace