#ifndef RETHINKDB_FDB_SYSTEM_TABLES_HPP_
#define RETHINKDB_FDB_SYSTEM_TABLES_HPP_

#include "clustering/administration/auth/username.hpp"
#include "containers/uuid.hpp"
#include "fdb/index.hpp"
#include "fdb/reql_fdb.hpp"

namespace auth {
class username_t;
}
class table_config_t;

struct table_config_by_id {
    using ukey_type = namespace_id_t;
    using value_type = table_config_t;
    static constexpr const char *prefix = REQLFDB_TABLE_CONFIG_BY_ID;

    static ukey_string ukey_str(const ukey_type &k) {
        return uuid_primary_key(k.value);
    }

    static ukey_type parse_ukey(key_view k) {
        return namespace_id_t{parse_uuid_primary_key(k)};
    }
};

struct db_config_by_id {
    using ukey_type = database_id_t;
    using value_type = name_string_t;
    static constexpr const char *prefix = REQLFDB_DB_CONFIG_BY_ID;

    static ukey_string ukey_str(const ukey_type &k) {
        // We make an aesthetic key.
        return ukey_string{uuid_to_str(k.value)};
    }

    static ukey_type parse_ukey(key_view k) {
        database_id_t ret;
        bool is_uuid = str_to_uuid(as_char(k.data), size_t(k.length), &ret.value);
        guarantee(is_uuid, "db_config_by_id parse_ukey sees bad key");
        return ret;
    }
};

struct db_config_by_name {
    using ukey_type = name_string_t;
    using value_type = database_id_t;
    static constexpr const char *prefix = REQLFDB_DB_CONFIG_BY_NAME;

    static ukey_string ukey_str(const ukey_type &k) {
        return ukey_string{k.str()};
    }

    static ukey_type parse_ukey(key_view k) {
        name_string_t str;
        bool success = str.assign_value(std::string(as_char(k.data), size_t(k.length)));
        guarantee(success, "db_config_by_name::parse_ukey got bad name_string_t");
        return str;
    }
};

struct users_by_username {
    using ukey_type = auth::username_t;
    using value_type = auth::user_t;
    static constexpr const char *prefix = REQLFDB_USERS_BY_USERNAME;

    static ukey_string ukey_str(const ukey_type &k) {
        return ukey_string{k.to_string()};
    }

    static ukey_type parse_ukey(key_view k) {
        return auth::username_t{std::string(as_char(k.data), size_t(k.length))};
    }
};


#endif  // RETHINKDB_FDB_SYSTEM_TABLES_HPP_
