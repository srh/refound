#ifndef RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_HPP_
#define RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_HPP_

#include <map>

#include "clustering/administration/auth/username.hpp"
#include "clustering/administration/auth/user.hpp"
#include "containers/uuid.hpp"
#include "containers/name_string.hpp"
#include "rdb_protocol/context.hpp"

namespace auth {
class username_t;
class user_t;
}

// TODO: Make use of this.
// TODO: Handle this exception.
class config_version_exc_t : public std::exception {
public:
    config_version_exc_t() {}

    const char *what() const noexcept override {
        return "Config version out of date";
    }
};

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

class reqlfdb_config_cache {
public:
    reqlfdb_config_cache();
    ~reqlfdb_config_cache();

    reqlfdb_config_version config_version;

    // These maps do _not_ contain the "rethinkdb" database or its system tables.

    // TODO: unordered maps?
    // These two maps are kept in sync.
    std::map<name_string_t, database_id_t> db_name_index;
    std::map<database_id_t, name_string_t> db_id_index;

    // These two maps are kept in sync.
    std::map<std::pair<database_id_t, name_string_t>, namespace_id_t> table_name_index;
    std::map<namespace_id_t, table_config_t> table_id_index;

    std::map<auth::username_t, auth::user_t> auth_index;

    void wipe();

    void note_version(reqlfdb_config_version cv) {
        rassert(cv.value >= config_version.value);
        if (cv.value > config_version.value) {
            wipe();
        }
        config_version = cv;
    }

    void add_db(const database_id_t &db_id, const name_string_t &db_name);
    void add_table(const namespace_id_t &table_id, const table_config_t &config);

    MOVABLE_BUT_NOT_COPYABLE(reqlfdb_config_cache);
};

// Returns r_nullopt if the cache doesn't have it.
optional<config_info<database_id_t>>
try_lookup_cached_db(const reqlfdb_config_cache *cache, const name_string_t &db_name);

optional<config_info<namespace_id_t>>
try_lookup_cached_table(const reqlfdb_config_cache *cache,
    const std::pair<database_id_t, name_string_t> &table_name);




#endif  // RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_HPP_
