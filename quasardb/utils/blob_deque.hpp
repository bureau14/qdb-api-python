#pragma once

#include <qdb/client.h>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <forward_list>
#include <vector>

namespace utils
{

/// Stores chunks of bytes. They stay stable in memory until the deque is cleared.
class blob_deque
{
    struct blob_block
    {
        std::unique_ptr<char[]> storage;
        size_t size;
        size_t capacity;

        explicit blob_block(size_t capacity)
            // TODO(C++20): Use std::make_unique_for_overwrite.
            : storage{std::make_unique<char[]>(capacity)}
            , size{}
            , capacity{capacity}
        {}

        bool can_store(size_t bytes_cnt) const noexcept
        {
            return bytes_cnt <= capacity - size;
        }

        qdb_blob_t add(qdb_blob_t blob) noexcept
        {
            assert(can_store(blob.content_length));
            auto dst = qdb_blob_t{static_cast<void *>(storage.get() + size), blob.content_length};
            std::memcpy(const_cast<void *>(dst.content), blob.content, blob.content_length);
            size += blob.content_length;
            return dst;
        }

        void clear() noexcept
        {
            size = 0;
        }
    };

    std::forward_list<blob_block> _blocks;
    decltype(_blocks)::iterator _cur_block;
    size_t _filled_blocks_size = 0;

public:
    explicit blob_deque(size_t initial_capacity = 64)
    {
        _blocks.emplace_front(initial_capacity);
        _cur_block = _blocks.begin();
    }

    qdb_blob_t add(qdb_blob_t blob)
    {

        while (!_cur_block->can_store(blob.content_length))
        {
            _filled_blocks_size += _cur_block->size;
            auto next_it = _cur_block;
            ++next_it;
            if (next_it == _blocks.end())
            {
                const auto cap = std::max(blob.content_length, _cur_block->capacity * 2);
                _cur_block     = _blocks.emplace_after(_cur_block, cap);
                break;
            }
            _cur_block = next_it;
        }
        return _cur_block->add(blob);
    }

    // Keep allocated memory to reuse it in subsequent 'add' calls.
    void clear()
    {
        for (auto & block : _blocks)
        {
            block.clear();
        }
        _cur_block          = _blocks.begin();
        _filled_blocks_size = 0;
    }

    size_t bytes_count() const noexcept
    {
        return _filled_blocks_size + _cur_block->size;
    }
};

} // namespace utils