#ifndef RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_HPP_
#define RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_HPP_

#include <limits.h>

#include <unordered_map>

// TODO: Uncomment or remove.
// #include "clustering/administration/auth/username.hpp"
// #include "clustering/administration/auth/user.hpp"
#include "clustering/id_types.hpp"
#include "containers/counted.hpp"
#include "containers/uuid.hpp"
#include "containers/name_string.hpp"
#include "fdb/id_types.hpp"

template <class T> class optional;

// TODO: Handle this exception.
class config_version_exc_t : public std::exception {
public:
    config_version_exc_t() {}

    const char *what() const noexcept override {
        return "Config version out of date";
    }
};

inline void check_cv(const config_version_checker &older, reqlfdb_config_version newer) {
    if (older.value != UINT64_MAX && older.value != newer.value) {
        throw config_version_exc_t();
    }
}

// TODO: Every caller could more gracefully check provenance to see if relevant subset
// of config is actually different, between older and newer version.
inline void check_cv(reqlfdb_config_version older, reqlfdb_config_version newer) {
    rassert(older.value <= newer.value);
    if (older.value != newer.value) {
        throw config_version_exc_t();
    }
}

// Carries config information and its provenance.
template <class T>
class config_info {
public:
    T ci_value;
    reqlfdb_config_version ci_cv;
};

class table_config_t;

struct name_string_hasher {
    size_t operator()(const name_string_t &ns) const {
        return std::hash<std::string>()(ns.str());
    }
};
struct database_id_hasher {
    size_t operator()(const database_id_t &db_id) const {
        return std::hash<uuid_u>()(db_id.value);
    }
};
struct namespace_id_hasher {
    size_t operator()(const namespace_id_t &db_id) const {
        return std::hash<uuid_u>()(db_id.value);
    }
};
struct cc_pair_hasher {
    size_t operator()(const std::pair<database_id_t, name_string_t> &p) const {
        size_t db_hash = database_id_hasher()(p.first);
        size_t name_hash = name_string_hasher()(p.second);

        // This is the same formula used in boost's hash_combine.
        return db_hash ^ (name_hash + 0x9e3779b9 + (db_hash << 6) + (db_hash >> 2));
    }
};

class reqlfdb_config_cache {
public:
    reqlfdb_config_cache();
    ~reqlfdb_config_cache();

    reqlfdb_config_version config_version;

    // These maps do _not_ contain the "rethinkdb" database or its system tables.

    // These two maps are kept in sync.
    std::unordered_map<name_string_t, database_id_t, name_string_hasher> db_name_index;
    std::unordered_map<database_id_t, name_string_t, database_id_hasher> db_id_index;

    // These two maps are kept in sync.
    std::unordered_map<std::pair<database_id_t, name_string_t>, namespace_id_t, cc_pair_hasher> table_name_index;
    std::unordered_map<namespace_id_t, counted_t<const rc_wrapper<table_config_t>>,
        namespace_id_hasher> table_id_index;

    // TODO: Uncomment auth_index or remove.
    //
    // The table and db indexes are useful for implementing r.db() and r.table() terms
    // efficiently.  The user auth checks, ultimately, cannot succeed or fail without a
    // round-trip to the db, and the only purpose in caching is to avoid a key/value
    // request from fdb.  This is something that may be worth implementing later, when
    // we add the ability to recover from check_cv failures when it doesn't affect the
    // config key in question.
    //
    // std::unordered_map<auth::username_t, auth::user_t> auth_index;

    void wipe();

    void note_version(reqlfdb_config_version cv) {
        rassert(cv.value >= config_version.value);
        if (cv.value > config_version.value) {
            wipe();
        }
        config_version = cv;
    }

    void add_db(const database_id_t &db_id, const name_string_t &db_name);
    void add_table(const namespace_id_t &table_id, counted_t<const rc_wrapper<table_config_t>> config);

    MOVABLE_BUT_NOT_COPYABLE(reqlfdb_config_cache);
};

// Returns r_nullopt if the cache doesn't have it.
optional<config_info<database_id_t>>
try_lookup_cached_db(const reqlfdb_config_cache *cache, const name_string_t &db_name);

optional<config_info<namespace_id_t>>
try_lookup_cached_table(const reqlfdb_config_cache *cache,
    const std::pair<database_id_t, name_string_t> &table_name);

// TODO: Uncomment or remove
#if 0
optional<config_info<auth::user_t>>
try_lookup_cached_user(const reqlfdb_config_cache *cache, const auth::username_t &username);
#endif

#endif  // RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_HPP_
