#ifndef RETHINKDB_RDB_PROTOCOL_ADMIN_IDENTIFIER_FORMAT_HPP_
#define RETHINKDB_RDB_PROTOCOL_ADMIN_IDENTIFIER_FORMAT_HPP_

enum class admin_identifier_format_t {
    /* Some parts of the code rely on the fact that `admin_identifier_format_t` can be
    mapped to `{0, 1}` using `static_cast`. */
    name = 0,
    uuid = 1
};

#endif  // RETHINKDB_RDB_PROTOCOL_ADMIN_IDENTIFIER_FORMAT_HPP_
