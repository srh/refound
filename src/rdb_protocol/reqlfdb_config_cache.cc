#include "rdb_protocol/reqlfdb_config_cache.hpp"
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"

#include "clustering/administration/tables/table_metadata.hpp"
#include "containers/archive/string_stream.hpp"
#include "fdb/index.hpp"
#include "fdb/reql_fdb.hpp"
#include "fdb/reql_fdb_utils.hpp"

RDB_IMPL_SERIALIZABLE_1_SINCE_v2_5(reqlfdb_config_version, value);

reqlfdb_config_cache::reqlfdb_config_cache()
    : config_version{0} {}
reqlfdb_config_cache::~reqlfdb_config_cache() {}

void config_cache_wipe(reqlfdb_config_cache *cache) {
    reqlfdb_config_cache tmp;
    *cache = std::move(tmp);
}

std::string db_by_id_key(const uuid_u &db_id) {
    // We make an aesthetic key.
    return uuid_to_str(db_id);
}

reqlfdb_config_version future_block_on_config_version(
        FDBFuture *fut, const signal_t *interruptor) {
    fdb_value cv_value = future_block_on_value(fut, interruptor);
    reqlfdb_config_version cv;
    bool present = deserialize_off_fdb_value(cv_value, &cv);
    guarantee(present, "config version not present");  // TODO?
    return cv;
}

config_info<optional<database_id_t>>
config_cache_db_by_name(
        reqlfdb_config_cache *cache, FDBTransaction *txn,
        const name_string_t &db_name, const signal_t *interruptor) {
    auto it = cache->db_name_index.find(db_name);
    if (it != cache->db_name_index.end()) {
        // TODO: Some sort of mutex assertion is in order.
        config_info<optional<database_id_t>> ret;
        ret.value = make_optional(it->second);
        ret.check_later.expected_config_version = cache->config_version;
        ret.check_later.config_version_future
            = transaction_get_c_str(txn, REQLFDB_CONFIG_VERSION_KEY);
        return ret;
    }

    // We couldn't find the db name.  Maybe it's just uncached.

    fdb_future fut = transaction_lookup_unique_index(
        txn, REQLFDB_DB_CONFIG_BY_NAME, db_name.str());
    fdb_future cv_fut = transaction_get_c_str(txn, REQLFDB_CONFIG_VERSION_KEY);

    // We block here!
    fdb_value value = future_block_on_value(fut.fut, interruptor);
    reqlfdb_config_version cv = future_block_on_config_version(cv_fut.fut, interruptor);

    ASSERT_NO_CORO_WAITING;

    if (cv.value < cache->config_version.value) {
        // Throw a retryable exception.
        throw fdb_transaction_exception(REQLFDB_not_committed);
    }

    if (cv.value > cache->config_version.value) {
        config_cache_wipe(cache);
        cache->config_version = cv;
    }

    database_id_t id;
    bool present = deserialize_off_fdb_value(value, &id);

    if (!present) {
        config_info<optional<database_id_t>> ret;
        ret.value = r_nullopt;
        return ret;
    } else {
        cache->db_id_index.emplace(id, db_name);
        cache->db_name_index.emplace(db_name, id);
        config_info<optional<database_id_t>> ret;
        ret.value = make_optional(id);
        return ret;
    }
}

bool config_cache_db_create(
        FDBTransaction *txn,
        const name_string_t &db_name, const signal_t *interruptor) {
    // TODO: This function must read and verify user permissions when performing this
    // operation.
    guarantee(db_name.str() != "rethinkdb",
        "config_cache_db_create should never get queries for system tables");
    // TODO: Ensure caller doesn't pass "rethinkdb".

    fdb_future cv_fut = transaction_get_c_str(txn, REQLFDB_CONFIG_VERSION_KEY);
    fdb_future fut = transaction_lookup_unique_index(
        txn, REQLFDB_DB_CONFIG_BY_NAME, db_name.str());

    fdb_value value = future_block_on_value(fut.fut, interruptor);

    if (value.present) {
        // A db with this name already exists.
        return false;
    }
    reqlfdb_config_version cv = future_block_on_config_version(cv_fut.fut, interruptor);

    ASSERT_NO_CORO_WAITING;

    database_id_t db_id = generate_uuid();
    // TODO: Use uniform reql datum primary key serialization, how about that idea?
    std::string db_id_key = db_by_id_key(db_id);
    std::string db_id_value = serialize_for_cluster_to_string(db_id);

    transaction_set_pkey_index(txn, REQLFDB_DB_CONFIG_BY_ID, db_id_key, db_name.str());
    transaction_set_unique_index(txn, REQLFDB_DB_CONFIG_BY_NAME, db_name.str(), db_id_value);

    cv.value++;
    serialize_and_set(txn, REQLFDB_CONFIG_VERSION_KEY, cv);
    return true;
}

bool config_cache_db_drop(
        FDBTransaction *txn, const name_string_t &db_name, const signal_t *interruptor) {
    // TODO: This function must read and verify user permissions when performing this
    // operation.

    // TODO: Ensure caller doesn't pass "rethinkdb".

    fdb_future cv_fut = transaction_get_c_str(txn, REQLFDB_CONFIG_VERSION_KEY);
    fdb_future fut = transaction_lookup_pkey_index(
        txn, REQLFDB_DB_CONFIG_BY_NAME, db_name.str());

    fdb_value value = future_block_on_value(fut.fut, interruptor);
    if (!value.present) {
        return false;
    }
    reqlfdb_config_version cv = future_block_on_config_version(cv_fut.fut, interruptor);

    // TODO: Finish implementing.
    return false;
}

std::string table_by_name_key(const uuid_u &db_uuid, const name_string_t &table_name) {
    // We make an aesthetic key.  UUID's are fixed-width so it's OK.
    // TODO: Use standard compound index key format, so db_list works well.
    return uuid_to_str(db_uuid) + "." + table_name.str();
}

std::string table_by_id_key(const uuid_u &table_id) {
    return uuid_to_str(table_id);
}

bool config_cache_table_create(
        FDBTransaction *txn,
        const table_config_t &config,
        const signal_t *interruptor) {
    // TODO: This function must read and verify user permissions when performing this
    // operation.

    const uuid_u db_id = config.basic.database;
    const name_string_t &table_name = config.basic.name;

    // TODO: Ensure caller doesn't try to create table for "rethinkdb" database.

    const std::string table_index_key = table_by_name_key(db_id, table_name);
    const std::string db_pkey_key = db_by_id_key(db_id);

    fdb_future cv_fut = transaction_get_c_str(txn, REQLFDB_CONFIG_VERSION_KEY);
    fdb_future table_by_name_fut = transaction_lookup_unique_index(
        txn, REQLFDB_TABLE_CONFIG_BY_NAME, table_index_key);
    fdb_future db_by_id_fut = transaction_lookup_pkey_index(
        txn, REQLFDB_DB_CONFIG_BY_ID, db_pkey_key);

    fdb_value table_by_name_value
        = future_block_on_value(table_by_name_fut.fut, interruptor);
    fdb_value db_by_id_value = future_block_on_value(db_by_id_fut.fut, interruptor);

    if (table_by_name_value.present) {
        // Table already exists.
        return false;
    }

    reqlfdb_config_version cv = future_block_on_config_version(cv_fut.fut, interruptor);

    ASSERT_NO_CORO_WAITING;

    if (!db_by_id_value.present) {
        // TODO: This might mean the id came from an out-of-date cache.  We should
        // report the error back to the user (which is broken) but with a distinguished
        // error return value than "table already exists".
        return false;
    }

    // Okay, the db's present, the table is not present.  Create the table.

    const uuid_u table_id = generate_uuid();

    // TODO: Figure out how to name these sorts of variables.
    std::string table_pkey = table_by_id_key(table_id);
    std::string table_config_value = serialize_for_cluster_to_string(config);
    std::string table_pkey_value = serialize_for_cluster_to_string(table_id);

    transaction_set_pkey_index(txn, REQLFDB_TABLE_CONFIG_BY_ID, table_pkey,
        table_config_value);
    transaction_set_unique_index(txn, REQLFDB_TABLE_CONFIG_BY_NAME, table_index_key,
        table_pkey_value);

    cv.value++;
    serialize_and_set(txn, REQLFDB_CONFIG_VERSION_KEY, cv);

    return true;
}


bool outer_config_cache_table_create(
        FDBTransaction *txn,
        const uuid_u &db_id,
        const name_string_t &table_name,
        const table_generate_config_params_t &config_params,
        const std::string &primary_key,
        write_durability_t durability,
        const signal_t *interruptor) {
    table_config_t config;
    config.basic.name = table_name;
    config.basic.database = db_id;
    config.basic.primary_key = primary_key;

    // TODO: Remove num_shards config.
    // TODO: Remove sharding UI.
    guarantee(config_params.num_shards == 1, "config params bad");

    // TODO: Remove table_config_t::shards.
    // TODO: Remove table_config_t::write_ack_config and -::durability.
    config.write_ack_config = write_ack_config_t::MAJORITY;
    config.durability = durability;
    config.user_data = default_user_data();

    return config_cache_table_create(txn, config, interruptor);
}

// TODO (out of context): ql::db_t should do nothing but hold info about whether it
// was by name or by value, don't actually do the lookup right away.


