#ifndef RETHINKDB_FDB_SYSTEM_TABLES_HPP_
#define RETHINKDB_FDB_SYSTEM_TABLES_HPP_

#include "clustering/auth/username.hpp"
#include "containers/uuid.hpp"
#include "fdb/index.hpp"
#include "fdb/reql_fdb.hpp"

namespace auth {
class username_t;
class user_t;
}
class table_config_t;

struct node_info_by_id {
    using ukey_type = fdb_node_id;
    using value_type = node_info;
    static constexpr const char *prefix = REQLFDB_NODES_BY_ID;

    static ukey_string ukey_str(const ukey_type &k) {
        // TODO: Use binary uuid?  In general.
        return uuid_primary_key(k.value);
    }

    static ukey_type parse_ukey(key_view k) {
        return fdb_node_id{parse_uuid_primary_key(k)};
    }
};

// TODO: These are implemented in jobs.cc.
skey_string reqlfdb_clock_sindex_key(reqlfdb_clock clock);
std::pair<reqlfdb_clock, key_view> split_clock_key_pair(key_view key);

struct node_info_by_lease_expiration {
    using skey_type = reqlfdb_clock;
    using pkey_type = fdb_node_id;
    static constexpr const char *prefix = REQLFDB_NODES_BY_LEASE_EXPIRATION;

    // NNN: Use same parsing function of key format as jobs table.
    static skey_string skey_str(const skey_type &k) {
        return reqlfdb_clock_sindex_key(k);
    }

    static ukey_string pkey_str(const pkey_type &k) {
        return node_info_by_id::ukey_str(k);
    }

    static std::pair<skey_type, pkey_type> parse_skey(key_view k) {
        std::pair<reqlfdb_clock, key_view> pair = split_clock_key_pair(k);
        std::pair<skey_type, pkey_type> ret;
        ret.first = pair.first;
        ret.second = node_info_by_id::parse_ukey(pair.second);
        return ret;
    }
};

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

constexpr const char *table_by_name_separator = ".";

// The thing to which we append the table name.
inline std::string table_by_name_ukey_prefix(const database_id_t db_id) {
    // We make an aesthetic key.  UUID's are fixed-width so it's OK.
    return uuid_to_str(db_id.value) + table_by_name_separator;
}

// Takes a std::string we don't know is a valid table name.  If the format ever changes
// such that an invalid name wouldn't work as a key, we'd have to remove this function.
inline ukey_string table_by_unverified_name_key(
        const database_id_t &db_id,
        const std::string &table_name) {
    // TODO: Use standard compound index key format, so db_list works well.
    return ukey_string{table_by_name_ukey_prefix(db_id) + table_name};
}

inline ukey_string table_by_name_key(
        const database_id_t &db_id,
        const name_string_t &table_name) {
    return table_by_unverified_name_key(db_id, table_name.str());
}

struct table_config_by_name {
    using ukey_type = std::pair<database_id_t, name_string_t>;
    using value_type = namespace_id_t;
    static constexpr const char *prefix = REQLFDB_TABLE_CONFIG_BY_NAME;

    static ukey_string ukey_str(const ukey_type &k) {
        return table_by_unverified_name_key(k.first, k.second.str());
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

struct users_by_ids {
    // As mentioned, the uuid is a database id or namespace id.
    using skey_type = uuid_u;
    using pkey_type = auth::username_t;
    static constexpr const char *prefix = REQLFDB_USERS_BY_IDS;

    static skey_string skey_str(const skey_type &k) {
        return skey_string{uuid_to_str(k)};
    }

    static ukey_string pkey_str(const pkey_type &k) {
        return ukey_string{k.to_string()};
    }
};


#endif  // RETHINKDB_FDB_SYSTEM_TABLES_HPP_
