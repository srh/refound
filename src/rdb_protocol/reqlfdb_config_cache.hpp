#ifndef RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_HPP_
#define RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_HPP_

#include <map>

#include "containers/uuid.hpp"
#include "containers/name_string.hpp"

struct reqlfdb_config_version {
    uint64_t value;
};

RDB_DECLARE_SERIALIZABLE(reqlfdb_config_version);

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

    MOVABLE_BUT_NOT_COPYABLE(reqlfdb_config_cache);
};

#endif  // RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_HPP_
