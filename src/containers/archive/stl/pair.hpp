#ifndef RETHINKDB_CONTAINERS_ARCHIVE_STL_PAIR_HPP_
#define RETHINKDB_CONTAINERS_ARCHIVE_STL_PAIR_HPP_

#include <utility>

#include "containers/archive/archive.hpp"
#include "version.hpp"

template <cluster_version_t W, class T, class U>
size_t serialized_size(const std::pair<T, U> &p) {
    return serialized_size<W>(p.first) + serialized_size<W>(p.second);
}

// Keep in sync with serialized_size.
template <cluster_version_t W, class T, class U>
void serialize(write_message_t *wm, const std::pair<T, U> &p) {
    serialize<W>(wm, p.first);
    serialize<W>(wm, p.second);
}

template <cluster_version_t W, class T, class U>
MUST_USE archive_result_t deserialize(read_stream_t *s, std::pair<T, U> *p) {
    archive_result_t res = deserialize<W>(s, &p->first);
    if (bad(res)) { return res; }
    res = deserialize<W>(s, &p->second);
    return res;
}

#endif  // RETHINKDB_CONTAINERS_ARCHIVE_STL_PAIR_HPP_

