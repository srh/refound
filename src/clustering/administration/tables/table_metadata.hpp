// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_TABLES_TABLE_METADATA_HPP_
#define CLUSTERING_ADMINISTRATION_TABLES_TABLE_METADATA_HPP_

#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "containers/name_string.hpp"
#include "containers/uuid.hpp"
#include "fdb/id_types.hpp"
#include "protocol_api.hpp"
#include "rdb_protocol/context.hpp"
#include "rdb_protocol/protocol.hpp"
#include "rpc/semilattice/joins/macros.hpp"
#include "rpc/serialize_macros.hpp"

/* This is the metadata for a single table. */

/* `table_basic_config_t` contains the subset of the table's configuration that the
parser needs to process queries against the table. A copy of this is stored on every
thread of every server for every table. */
class table_basic_config_t {
public:
    name_string_t name;
    database_id_t database;
    std::string primary_key;
};

RDB_DECLARE_SERIALIZABLE(table_basic_config_t);
RDB_DECLARE_EQUALITY_COMPARABLE(table_basic_config_t);

// TODO: Do we ever use write_ack_config_t::SINGLE?
enum class write_ack_config_t {
    SINGLE,
    MAJORITY
};
ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(
    write_ack_config_t,
    int8_t,
    write_ack_config_t::SINGLE,
    write_ack_config_t::MAJORITY);

class user_data_t {
public:
    ql::datum_t datum;
};

user_data_t default_user_data();

/* `table_config_t` describes the complete contents of the `rethinkdb.table_config`
artificial table. */

// This is currently not par of sindex_config_t, because that value gets serialized
// externally in some ways, and we don't want to touch that.  TODO: Maybe just put this
// with sindex_config_t.  Or move func_version out to this.
class sindex_metaconfig_t {
public:
    // The id is used for fdb key prefix.
    sindex_id_t sindex_id;
    // The sindex isn't ready until this is nil.
    fdb_shared_task_id creation_task_or_nil;
};
RDB_MAKE_SERIALIZABLE_2(sindex_metaconfig_t, sindex_id, creation_task_or_nil);

class table_config_t {
public:
    table_basic_config_t basic;
    // Two parallel maps.
    // TODO: Combine these maps after non-FDB code is stripped out.
    std::map<std::string, sindex_config_t> sindexes;
    std::map<std::string, sindex_metaconfig_t> fdb_sindexes;
    optional<write_hook_config_t> write_hook;
    user_data_t user_data;  // has user-exposed name "data"
};

RDB_DECLARE_SERIALIZABLE(table_config_t);
RDB_DECLARE_EQUALITY_COMPARABLE(table_config_t);

// Represents a store_key_t.
struct virtual_key_ptr {
    bool is_decremented = false;
    const store_key_t *key;

    virtual_key_ptr(bool _is_decremented, const store_key_t *_key)
        : is_decremented(_is_decremented), key(_key) {}
    explicit virtual_key_ptr(const store_key_t *k) : is_decremented(false), key(k) {}
    static virtual_key_ptr decremented(const store_key_t *k) {
        return virtual_key_ptr(true, k);
    }

    static virtual_key_ptr guarantee_decremented(const store_key_t *k) {
        guarantee(k->size() != 0, "guarantee_decremented sees empty key");
        return virtual_key_ptr(true, k);
    }

    bool grequal_to(const store_key_t &rhs) const {
        return is_decremented ? *key > rhs : *key >= rhs;
    }
};

/* `table_config_and_shards_t` exists because the `table_config_t` needs to be changed in
sync with the `table_shard_scheme_t` and the server name mapping. */

class table_config_and_shards_t {
public:
    table_config_t config;
};

RDB_DECLARE_SERIALIZABLE(table_config_and_shards_t);
RDB_DECLARE_EQUALITY_COMPARABLE(table_config_and_shards_t);

class table_config_and_shards_change_t {
public:
    class set_table_config_and_shards_t {
    public:
        table_config_and_shards_t new_config_and_shards;
    };

    class write_hook_create_t {
    public:
        write_hook_config_t config;
    };

    class write_hook_drop_t {
    public:
    };

    class sindex_create_t {
    public:
        std::string name;
        sindex_config_t config;
    };

    class sindex_drop_t {
    public:
        std::string name;
    };

    class sindex_rename_t {
    public:
        std::string name;
        std::string new_name;
        bool overwrite;
    };

    table_config_and_shards_change_t() { }

    explicit table_config_and_shards_change_t(set_table_config_and_shards_t &&_change)
        : change(std::move(_change)) { }
    explicit table_config_and_shards_change_t(sindex_create_t &&_change)
        : change(std::move(_change)) { }
    explicit table_config_and_shards_change_t(sindex_drop_t &&_change)
        : change(std::move(_change)) { }
    explicit table_config_and_shards_change_t(sindex_rename_t &&_change)
        : change(std::move(_change)) { }
    explicit table_config_and_shards_change_t(write_hook_create_t &&_change)
        : change(std::move(_change)) { }
    explicit table_config_and_shards_change_t(write_hook_drop_t &&_change)
        : change(std::move(_change)) { }


    /* Note, it's important that `apply_change` does not change
    `table_config_and_shards` if it returns false. */
    bool apply_change(table_config_and_shards_t *table_config_and_shards) const;

    bool name_and_database_equal(const table_basic_config_t &table_basic_config) const;

    RDB_MAKE_ME_SERIALIZABLE_1(table_config_and_shards_change_t, change);

private:
    boost::variant<
        set_table_config_and_shards_t,
        sindex_create_t,
        sindex_drop_t,
        sindex_rename_t,
        write_hook_create_t,
        write_hook_drop_t> change;

    class apply_change_visitor_t;
};

RDB_DECLARE_SERIALIZABLE(table_config_and_shards_change_t::set_table_config_and_shards_t);
RDB_DECLARE_SERIALIZABLE(table_config_and_shards_change_t::write_hook_create_t);
RDB_DECLARE_SERIALIZABLE(table_config_and_shards_change_t::write_hook_drop_t);
RDB_DECLARE_SERIALIZABLE(table_config_and_shards_change_t::sindex_create_t);
RDB_DECLARE_SERIALIZABLE(table_config_and_shards_change_t::sindex_drop_t);
RDB_DECLARE_SERIALIZABLE(table_config_and_shards_change_t::sindex_rename_t);

#endif /* CLUSTERING_ADMINISTRATION_TABLES_TABLE_METADATA_HPP_ */
