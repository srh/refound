#ifndef RETHINKDB_FDB_SYSTEM_TABLES_HPP_
#define RETHINKDB_FDB_SYSTEM_TABLES_HPP_

#include "containers/uuid.hpp"
#include "fdb/reql_fdb.hpp"

class table_config_t;

// TODO: Remove, use table_config_by_id::ukey_str.
inline ukey_string table_by_id_key(const namespace_id_t &table_id) {
    return uuid_primary_key(table_id.value);
}

struct table_config_by_id {
    using ukey_type = namespace_id_t;
    using value_type = table_config_t;
    static constexpr const char *prefix = REQLFDB_TABLE_CONFIG_BY_ID;

    static ukey_string ukey_str(const ukey_type &k) {
        return table_by_id_key(k);
    }
};


#endif  // RETHINKDB_FDB_SYSTEM_TABLES_HPP_
