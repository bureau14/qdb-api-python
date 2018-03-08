%inline%{


class wrap_qdb_table_result_t
{
public:
    qdb_error_t check_out_of_bounds(qdb_size_t r, qdb_size_t c) const
    {
        if (r >= 0 && c >= 0 && r < rows_count && c < columns_count) return qdb_e_ok;
        return qdb_e_out_of_bounds;
    }

    std::pair<qdb_error_t, qdb_query_result_value_type_t> get_type(qdb_size_t r, qdb_size_t c) const
    {
        qdb_error_t err = check_out_of_bounds(r, c);
        if (err != qdb_e_ok) return std::make_pair(err, qdb_query_result_none);
        return std::make_pair(err, rows[r][c].type);
    }

    std::pair<qdb_error_t, std::string> get_payload_blob(qdb_size_t r, qdb_size_t c) const
    {
        qdb_error_t err = check_out_of_bounds(r, c);
        if (err != qdb_e_ok) return std::make_pair(err, "");
        if (rows[r][c].type != qdb_query_result_blob) return std::make_pair(qdb_e_incompatible_type, "");
        return std::make_pair(err, std::string(static_cast<const char*>(rows[r][c].payload.blob.content), rows[r][c].payload.blob.content_length));
    }

    std::pair<qdb_error_t, qdb_int_t> get_payload_int64(qdb_size_t r, qdb_size_t c) const
    {
        qdb_error_t err = check_out_of_bounds(r, c);
        if (err != qdb_e_ok) return std::make_pair(err, 0);
        if (rows[r][c].type != qdb_query_result_int64) return std::make_pair(qdb_e_incompatible_type, 0);
        return std::make_pair(err, rows[r][c].payload.int64_.value);
    }

    std::pair<qdb_error_t, double> get_payload_double(qdb_size_t r, qdb_size_t c) const
    {
        qdb_error_t err = check_out_of_bounds(r, c);
        if (err != qdb_e_ok) return std::make_pair(err, 0.0);
        if (rows[r][c].type != qdb_query_result_double) return std::make_pair(qdb_e_incompatible_type, 0.0);
        return std::make_pair(err, rows[r][c].payload.double_.value);
    }

    std::pair<qdb_error_t, qdb_timespec_t> get_payload_timestamp(qdb_size_t r, qdb_size_t c) const
    {
        qdb_error_t err = check_out_of_bounds(r, c);
        qdb_timespec_t zero_ts;
        zero_ts.tv_sec = 0;
        zero_ts.tv_nsec = 0;

        if (err != qdb_e_ok) return std::make_pair(err, zero_ts);
        if (rows[r][c].type != qdb_query_result_timestamp) return std::make_pair(qdb_e_incompatible_type, zero_ts);
        return std::make_pair(err, rows[r][c].payload.timestamp.value);
    }

    std::string table_name;
    std::vector<std::string> columns_names;
    qdb_size_t columns_count;
    qdb_size_t rows_count;
    friend struct copy_table;

private:
    std::vector < std::vector < qdb_point_result_t > > rows;
};

struct convert_qdb_string_to_std_string
{
    std::string operator()(const qdb_string_t &str)
    {
        return std::string(str.data);
    }
};

struct copy_table
{
    wrap_qdb_table_result_t operator()(const qdb_table_result_t &that_table)
    {
        wrap_qdb_table_result_t _tbl;
        _tbl.rows_count = that_table.rows_count;
        _tbl.columns_count = that_table.columns_count;
        _tbl.table_name = that_table.table_name.data;
        _tbl.columns_names.resize(_tbl.columns_count);

        std::transform(that_table.columns_names, that_table.columns_names + that_table.columns_count, _tbl.columns_names.begin(), convert_qdb_string_to_std_string());

        _tbl.rows.resize(_tbl.rows_count);

        for (qdb_size_t i = 0; i < _tbl.rows_count; ++i)
        {
            _tbl.rows[i].resize(_tbl.columns_count);
            std::copy(that_table.rows[i], that_table.rows[i] + that_table.columns_count, _tbl.rows[i].begin());
        }
        return _tbl;
    }
};

struct wrap_qdb_query_result_t
{
    wrap_qdb_query_result_t() : _handle(NULL), _query_result(NULL), tables_count(0), scanned_rows_count(0) {}

    explicit wrap_qdb_query_result_t(qdb_handle_t h, qdb_query_result_t * qr) :
        _handle(h),
        _query_result(qr),
        tables_count(qr->tables_count),
        scanned_rows_count(qr->scanned_rows_count),
        tables(qr->tables_count)
    {
        std::transform(qr->tables, qr->tables + qr->tables_count, tables.begin(), copy_table());
    }

private:
    wrap_qdb_query_result_t(const wrap_qdb_query_result_t &) {}

    void release()
    {
        if (_handle && _query_result)
        {
            qdb_release(_handle, _query_result);
            _handle = NULL;
            _query_result = NULL;
        }
    }

    public:
        ~wrap_qdb_query_result_t()
        {
            release();
        }

    private:
        qdb_handle_t _handle;
        qdb_query_result_t * _query_result;

    public:
        qdb_size_t tables_count;
        qdb_size_t scanned_rows_count;
        std::vector <wrap_qdb_table_result_t> tables;
    };



typedef wrap_qdb_query_result_t * wrap_qdb_query_result_ptr;

%}

typedef wrap_qdb_query_result_t * wrap_qdb_query_result_ptr;
