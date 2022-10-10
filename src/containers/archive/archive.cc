// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "containers/archive/archive.hpp"

#include <string.h>

#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <netinet/in.h>
#endif

#include <algorithm>

#include "containers/archive/versioned.hpp"
#include "containers/uuid.hpp"
#include "rpc/serialize_macros.hpp"

const char *archive_result_as_str(archive_result_t archive_result) {
    switch (archive_result) {
    case archive_result_t::SUCCESS:
        return "archive_result_t::SUCCESS";
        break;
    case archive_result_t::SOCK_ERROR:
        return "archive_result_t::SOCK_ERROR";
        break;
    case archive_result_t::SOCK_EOF:
        return "archive_result_t::SOCK_EOF";
        break;
    case archive_result_t::RANGE_ERROR:
        return "archive_result_t::RANGE_ERROR";
        break;
    default:
        unreachable();
    }
}

int64_t force_read(read_stream_t *s, void *p, int64_t n) {
    rassert(n >= 0);

    char *chp = static_cast<char *>(p);
    int64_t written_so_far = 0;
    while (n > 0) {
        int64_t res = s->read(chp, n);
        if (res == 0) {
            return written_so_far;
        }
        if (res == -1) {
            // We don't communicate what data has been read so far.
            // I'm guessing callers to force_read don't care.
            return -1;
        }
        rassert(res <= n);

        written_so_far += res;
        chp += res;
        n -= res;
    }
    return written_so_far;
}

template <size_t N>
void write_message_template<N>::append(const void *p, int64_t n) {
    while (n > 0) {
        size_t last_buf_size = size_ % write_message_buffer_t<N>::DATA_SIZE;
        if (last_buf_size == 0) {
            buffers_.emplace_back(new write_message_buffer_t<N>);
        }

        write_message_buffer_t<N> *b = buffers_.back().get();
        int64_t k = std::min<int64_t>(n, write_message_buffer_t<N>::DATA_SIZE - last_buf_size);

        memcpy(b->data + last_buf_size, p, k);
        size_ += k;
        p = static_cast<const char *>(p) + k;
        n = n - k;
    }
}

template <size_t N>
std::string write_message_template<N>::send_to_string() const {
    std::string ret;
    ret.reserve(size_);
    size_t remaining = size_;
    for (const auto &p : buffers_) {
        size_t written = std::min(size_t(write_message_buffer_t<N>::DATA_SIZE), remaining);
        ret.append(p->data, written);
        remaining -= written;
    }
    return ret;
}

template <size_t N>
std::vector<char> write_message_template<N>::send_to_vector() const {
    std::vector<char> ret;
    ret.reserve(size_);
    size_t remaining = size_;
    for (const auto &p : buffers_) {
        size_t written = std::min(size_t(write_message_buffer_t<N>::DATA_SIZE), remaining);
        ret.insert(ret.end(), p->data, p->data + written);
        remaining -= written;
    }
    return ret;
}

int send_write_message(write_stream_t *s, const write_message_t *wm) {
    constexpr size_t N = WRITE_MESSAGE_BUF_SIZE;
    const std::vector<scoped<write_message_buffer_t<N>>> *bufs = wm->unsafe_expose_buffers();
    size_t remaining = wm->size();
    for (const auto &p : *bufs) {
        size_t written = std::min(size_t(write_message_buffer_t<N>::DATA_SIZE), remaining);
        int64_t res = s->write(p->data, written);
        if (res == -1) {
            return -1;
        }
        rassert(res == static_cast<int64_t>(written));
        remaining -= written;
    }
    return 0;
}

// You MUST NOT change the behavior of serialize_universal and deserialize_universal
// functions!  (You could find a way to remove their callers and remove them though.)
void serialize_universal(write_message_t *wm, const uuid_u &uuid) {
    rassert(!uuid.is_unset());
    wm->append(uuid.data(), uuid_u::static_size());
}
MUST_USE archive_result_t deserialize_universal(read_stream_t *s, uuid_u *uuid) {
    int64_t sz = uuid_u::static_size();
    int64_t res = force_read(s, uuid->data(), sz);

    if (res == -1) { return archive_result_t::SOCK_ERROR; }
    if (res < sz) { return archive_result_t::SOCK_EOF; }
    rassert(res == sz);
    return archive_result_t::SUCCESS;
}

template <cluster_version_t W>
void serialize(write_message_t *wm, const uuid_u &uuid) {
    serialize_universal(wm, uuid);
}

template <cluster_version_t W>
MUST_USE archive_result_t deserialize(read_stream_t *s, uuid_u *uuid) {
    return deserialize_universal(s, uuid);
}

INSTANTIATE_SERIALIZABLE_SINCE_v1_13(uuid_u);

void serialize_universal(write_message_t *wm, bool b) {
    // We depend on the versioned implementation for booleans not changing.
    serialize<cluster_version_t::LATEST_OVERALL>(wm, b);
}
MUST_USE archive_result_t deserialize_universal(read_stream_t *s, bool *b) {
    return deserialize<cluster_version_t::LATEST_OVERALL>(s, b);
}


template <cluster_version_t W>
void serialize(write_message_t *wm, const in6_addr &addr) {
    wm->append(&addr.s6_addr, sizeof(addr.s6_addr));
}

template <cluster_version_t W>
MUST_USE archive_result_t deserialize(read_stream_t *s, in6_addr *addr) {
    int64_t sz = sizeof(addr->s6_addr);
    int64_t res = force_read(s, &addr->s6_addr, sz);

    if (res == -1) { return archive_result_t::SOCK_ERROR; }
    if (res < sz) { return archive_result_t::SOCK_EOF; }
    rassert(res == sz);
    return archive_result_t::SUCCESS;
}

INSTANTIATE_SERIALIZABLE_SINCE_v1_13(in6_addr);

// Keep the struct keyword here to satisfy VC++
RDB_IMPL_SERIALIZABLE_1_SINCE_v1_13(struct in_addr, s_addr);

template class write_message_template<WRITE_MESSAGE_BUF_SIZE>;

