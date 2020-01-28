#ifndef RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_HPP_
#define RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_HPP_

#include <map>

#include "clustering/administration/auth/username.hpp"
#include "clustering/administration/auth/user.hpp"
#include "containers/uuid.hpp"
#include "containers/name_string.hpp"

namespace auth {
class username_t;
class user_t;
}

struct reqlfdb_config_version {
    uint64_t value;
};

RDB_DECLARE_SERIALIZABLE(reqlfdb_config_version);


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

    // TODO: unordered maps?
    std::map<name_string_t, database_id_t> db_name_index;
    std::map<name_string_t, namespace_id_t> table_name_index;

    std::map<database_id_t, name_string_t> db_id_index;
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

    MOVABLE_BUT_NOT_COPYABLE(reqlfdb_config_cache);
};

// Returns r_nullopt if the cache doesn't have it.
optional<config_info<database_id_t>>
try_lookup_cached_db(const reqlfdb_config_cache *cache, const name_string_t &db_name);





#endif  // RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_HPP_
