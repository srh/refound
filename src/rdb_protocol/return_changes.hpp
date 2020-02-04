#ifndef RETHINKDB_RDB_PROTOCOL_RETURN_CHANGES_HPP_
#define RETHINKDB_RDB_PROTOCOL_RETURN_CHANGES_HPP_

enum class return_changes_t {
    NO = 0,
    YES = 1,
    ALWAYS = 2
};
ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(
        return_changes_t, int8_t,
        return_changes_t::NO, return_changes_t::ALWAYS);

#endif  // RETHINKDB_RDB_PROTOCOL_RETURN_CHANGES_HPP_
