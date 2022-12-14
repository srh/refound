// Copyright 2010-2013 RethinkDB, all rights reserved.
#ifndef CONTAINERS_ARCHIVE_STL_TYPES_HPP_
#define CONTAINERS_ARCHIVE_STL_TYPES_HPP_

#include <inttypes.h>

#include <array>
#include <deque>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "containers/archive/archive.hpp"
#include "containers/archive/stl/pair.hpp"
#include "containers/archive/varint.hpp"
#include "version.hpp"

// Implementations for deque, pair, map, set, string, vector, and array.

template <cluster_version_t W, class K, class V, class C>
size_t serialized_size(const std::map<K, V, C> &m) {
    size_t ret = varint_uint64_serialized_size(m.size());
    for (auto it = m.begin(), e = m.end(); it != e; ++it) {
        ret += serialized_size<W>(*it);
    }
    return ret;
}

template <cluster_version_t W, class K, class V, class C>
void serialize(write_message_t *wm, const std::map<K, V, C> &m) {
    serialize_varint_uint64(wm, m.size());
    for (auto it = m.begin(), e = m.end(); it != e; ++it) {
        serialize<W>(wm, *it);
    }
}

template <cluster_version_t W, class K, class V, class C>
MUST_USE archive_result_t deserialize(read_stream_t *s, std::map<K, V, C> *m) {
    m->clear();

    uint64_t sz;
    archive_result_t res = deserialize_varint_uint64(s, &sz);
    if (bad(res)) { return res; }

    if (sz > std::numeric_limits<size_t>::max()) {
        return archive_result_t::RANGE_ERROR;
    }

    // Using position should make this function take linear time, not
    // sz*log(sz) time.
    typename std::map<K, V, C>::iterator position = m->begin();

    for (uint64_t i = 0; i < sz; ++i) {
        std::pair<K, V> p;
        res = deserialize<W>(s, &p);
        if (bad(res)) { return res; }
        position = m->insert(position, std::move(p));
    }

    return archive_result_t::SUCCESS;
}

template <cluster_version_t W, class T>
void serialize(write_message_t *wm, const std::set<T> &s) {
    serialize_varint_uint64(wm, s.size());
    for (typename std::set<T>::iterator it = s.begin(), e = s.end(); it != e; ++it) {
        serialize<W>(wm, *it);
    }
}

template <cluster_version_t W, class T>
MUST_USE archive_result_t deserialize(read_stream_t *s, std::set<T> *out) {
    out->clear();

    uint64_t sz;
    archive_result_t res = deserialize_varint_uint64(s, &sz);
    if (bad(res)) { return res; }

    if (sz > std::numeric_limits<size_t>::max()) {
        return archive_result_t::RANGE_ERROR;
    }

    typename std::set<T>::iterator position = out->begin();

    for (uint64_t i = 0; i < sz; ++i) {
        T value;
        res = deserialize<W>(s, &value);
        if (bad(res)) { return res; }
        position = out->insert(position, value);
    }

    return archive_result_t::SUCCESS;
}

size_t serialize_universal_size(const std::string &s);
void serialize_universal(write_message_t *wm, const std::string &s);
MUST_USE archive_result_t deserialize_universal(read_stream_t *s, std::string *out);

template <cluster_version_t W>
size_t serialized_size(const std::string &s) {
    return serialize_universal_size(s);
}
template <cluster_version_t W>
void serialize(write_message_t *wm, const std::string &s) {
    serialize_universal(wm, s);
}
template <cluster_version_t W>
MUST_USE archive_result_t deserialize(read_stream_t *s, std::string *out) {
    return deserialize_universal(s, out);
}


// Think twice before using this function on vectors containing a primitive type --
// it'll take O(n) time!
// Keep in sync with serialize.
template <cluster_version_t W, class T>
size_t serialized_size(const std::vector<T> &v) {
    size_t ret = varint_uint64_serialized_size(v.size());
    for (auto it = v.begin(), e = v.end(); it != e; ++it) {
        ret += serialized_size<W>(*it);
    }
    return ret;
}


// Keep in sync with serialized_size.
template <cluster_version_t W, class T>
void serialize(write_message_t *wm, const std::vector<T> &v) {
    serialize_varint_uint64(wm, v.size());
    for (auto it = v.begin(), e = v.end(); it != e; ++it) {
        serialize<W>(wm, *it);
    }
}

template <cluster_version_t W, class T>
MUST_USE archive_result_t deserialize(read_stream_t *s, std::vector<T> *v) {
    v->clear();

    uint64_t sz;
    archive_result_t res = deserialize_varint_uint64(s, &sz);
    if (bad(res)) { return res; }

    if (sz > std::numeric_limits<size_t>::max()) {
        return archive_result_t::RANGE_ERROR;
    }

    v->resize(sz);
    for (uint64_t i = 0; i < sz; ++i) {
        res = deserialize<W>(s, &(*v)[i]);
        if (bad(res)) { return res; }
    }

    return archive_result_t::SUCCESS;
}

template <cluster_version_t W, class T>
void serialize(write_message_t *wm, const std::deque<T> &v) {
    serialize_varint_uint64(wm, v.size());
    for (typename std::deque<T>::const_iterator it = v.begin(); it != v.end(); ++it) {
        serialize<W>(wm, *it);
    }
}

template <cluster_version_t W, class T>
MUST_USE archive_result_t deserialize(read_stream_t *s, std::deque<T> *v) {
    uint64_t sz;
    archive_result_t res = deserialize_varint_uint64(s, &sz);
    if (bad(res)) { return res; }

    if (sz > std::numeric_limits<size_t>::max()) {
        return archive_result_t::RANGE_ERROR;
    }

    for (uint64_t i = 0; i < sz; ++i) {
        // We avoid copying a non-empty value.
        v->push_back(T());
        res = deserialize<W>(s, &v->back());
        if (bad(res)) { return res; }
    }

    return archive_result_t::SUCCESS;
}

template <cluster_version_t W, class T, std::size_t N>
void serialize(write_message_t *write_message, std::array<T, N> const &values) {
    serialize_varint_uint64(write_message, N);
    for (T const & value : values) {
        serialize<W>(write_message, value);
    }
}

template <cluster_version_t W, class T, std::size_t N>
MUST_USE archive_result_t deserialize(read_stream_t *read_stream, std::array<T, N> *values) {
    uint64_t sz = 0;
    archive_result_t archive_result = deserialize_varint_uint64(read_stream, &sz);
    if (bad(archive_result)) {
        return archive_result;
    }
    if (sz > std::numeric_limits<size_t>::max()) {
        return archive_result_t::RANGE_ERROR;
    }
    size_t size = static_cast<size_t>(sz);
    if (size != N) {
        return archive_result_t::RANGE_ERROR;
    }

    for (std::size_t i = 0; i < size; ++i) {
        archive_result = deserialize<W>(read_stream, &((*values)[i]));
        if (bad(archive_result)) {
            return archive_result;
        }
    }

    return archive_result_t::SUCCESS;
}

#endif  // CONTAINERS_ARCHIVE_STL_TYPES_HPP_
