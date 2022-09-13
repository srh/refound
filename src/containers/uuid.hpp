// Copyright 2010-2013 RethinkDB, all rights reserved.
#ifndef CONTAINERS_UUID_HPP_
#define CONTAINERS_UUID_HPP_

#include <string.h>
#include <stdint.h>

#include <functional>  // To see std::hash.
#include <string>

#include "errors.hpp"

class printf_buffer_t;

// uuid_t is defined on Darwin.  I have given up on what to name it.  Please
// don't use guid_t, for it has a Windowsian connotation and we might run into
// the same sort of problem from that.
// UUIDs are kept in network byte order at all times.
class uuid_u {
public:
    uuid_u();

    bool is_unset() const;
    bool is_nil() const;

    static constexpr size_t kStaticSize = 16;
    static constexpr size_t kStringSize = 2 * kStaticSize + 4;  // hexadecimal, 4 hyphens
    static size_t static_size() {
        CT_ASSERT(sizeof(uuid_u) == kStaticSize);
        return kStaticSize;
    }

    uint8_t *data() { return data_; }
    const uint8_t *data() const { return data_; }

    static uuid_u from_hash(const uuid_u &base, const std::string &name);

private:
    uint8_t data_[kStaticSize];
};

struct randbuf128 {
    uint8_t data[uuid_u::kStaticSize];
};

bool operator==(const uuid_u& x, const uuid_u& y);
inline bool operator!=(const uuid_u& x, const uuid_u& y) { return !(x == y); }
bool operator<(const uuid_u& x, const uuid_u& y);

/* This does the same thing as `boost::uuids::random_generator()()`, except that
Valgrind won't complain about it. */
uuid_u generate_uuid();

randbuf128 generate_randbuf128();

// Returns boost::uuids::nil_generator()().
uuid_u nil_uuid();

void debug_print(printf_buffer_t *buf, const uuid_u& id);

std::string uuid_to_str(uuid_u id);
void uuid_onto_str(uuid_u id, std::string *onto);

uuid_u str_to_uuid(const std::string &str);
uuid_u str_to_uuid(const char *buf, size_t len);

MUST_USE bool str_to_uuid(const std::string &str, uuid_u *out);
MUST_USE bool str_to_uuid(const char *str, size_t count, uuid_u *out);

struct namespace_id_t {
    uuid_u value;
};
struct database_id_t {
    uuid_u value;
};

// Internal utility useful elsewhere.
void push_hex(std::string *s, uint8_t byte);

namespace std {
template<> struct hash<uuid_u> {
    size_t operator()(const uuid_u& x) const {
        // TODO: Don't allocate a string.
        return std::hash<std::string>()(::std::string(reinterpret_cast<const char *>(x.data()), x.static_size()));
    }
};
}  // namespace std

#endif  // CONTAINERS_UUID_HPP_
