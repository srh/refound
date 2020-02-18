#ifndef RETHINKDB_CONTAINERS_ARCHIVE_STL_UNORDERED_MAP_HPP_
#define RETHINKDB_CONTAINERS_ARCHIVE_STL_UNORDERED_MAP_HPP_

#include <inttypes.h>
#include <unordered_map>

#include "containers/archive/archive.hpp"
#include "containers/archive/varint.hpp"
#include "containers/archive/stl/pair.hpp"
#include "version.hpp"

template <cluster_version_t W, class K, class V, class C>
size_t serialized_size(const std::unordered_map<K, V, C> &m) {
    size_t ret = varint_uint64_serialized_size(m.size());
    for (const std::pair<K, V> &pair : m) {
        ret += serialized_size<W>(pair);
    }
    return ret;
}

template <cluster_version_t W, class K, class V, class C>
void serialize(write_message_t *wm, const std::unordered_map<K, V, C> &m) {
    serialize_varint_uint64(wm, m.size());
    for (const std::pair<K, V> &pair : m) {
        serialize<W>(wm, pair);
    }
}

template <cluster_version_t W, class K, class V, class C>
MUST_USE archive_result_t deserialize(read_stream_t *s, std::unordered_map<K, V, C> *m) {
    m->clear();

    uint64_t sz;
    archive_result_t res = deserialize_varint_uint64(s, &sz);
    if (bad(res)) { return res; }

    if (sz > std::numeric_limits<size_t>::max()) {
        return archive_result_t::RANGE_ERROR;
    }

    m->reserve(sz);
    for (uint64_t i = 0; i < sz; ++i) {
        std::pair<K, V> p;
        res = deserialize<W>(s, &p);
        if (bad(res)) { return res; }
        bool inserted = m->insert(std::move(p)).second;
        if (!inserted) {
            return archive_result_t::RANGE_ERROR;
        }
    }

    return archive_result_t::SUCCESS;
}

#endif  // RETHINKDB_CONTAINERS_ARCHIVE_STL_UNORDERED_MAP_HPP_

