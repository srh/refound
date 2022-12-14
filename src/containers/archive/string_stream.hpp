// Copyright 2010-2013 RethinkDB, all rights reserved.
#ifndef CONTAINERS_ARCHIVE_STRING_STREAM_HPP_
#define CONTAINERS_ARCHIVE_STRING_STREAM_HPP_

#include <string>

#include "containers/archive/archive.hpp"
#include "version.hpp"

class string_stream_t : public write_stream_t {
public:
    string_stream_t();
    virtual ~string_stream_t();

    virtual MUST_USE int64_t write(const void *p, int64_t n);

    std::string &str() { return str_; }

private:
    std::string str_;

    DISABLE_COPYING(string_stream_t);
};

template <cluster_version_t W, class T>
std::string serialize_to_string(const T &value) {
    static_assert(W == cluster_version_t::LATEST_DISK,
                  "Serializing to earlier version.");
    // We still make a second copy when serializing.
    write_message_t wm;
    serialize<W>(&wm, value);
    return wm.send_to_string();
}

template <class T>
std::string serialize_for_cluster_to_string(const T &value) {
    return serialize_to_string<cluster_version_t::CLUSTER>(value);
}


class string_read_stream_t : public read_stream_t {
public:
    explicit string_read_stream_t(std::string &&_source, int64_t _offset);
    virtual ~string_read_stream_t();

    virtual MUST_USE int64_t read(void *p, int64_t n);

    void swap(std::string *other_source, int64_t *other_offset);

private:
    std::string source;
    int64_t offset;

    DISABLE_COPYING(string_read_stream_t);
};

#endif  // CONTAINERS_ARCHIVE_STRING_STREAM_HPP_
