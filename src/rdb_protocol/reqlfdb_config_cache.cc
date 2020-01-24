#include "rdb_protocol/reqlfdb_config_cache.hpp"
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"

#include "clustering/administration/tables/table_metadata.hpp"
#include "containers/archive/string_stream.hpp"
#include "fdb/index.hpp"
#include "fdb/jobs.hpp"
#include "fdb/reql_fdb.hpp"
#include "fdb/reql_fdb_utils.hpp"

RDB_IMPL_SERIALIZABLE_1_SINCE_v2_5(reqlfdb_config_version, value);

std::string table_key_prefix(const namespace_id_t &table_id) {
    // TODO: Use binary uuid's.  This is on a fast path...
    // Or don't even use uuid's.
    std::string ret = "tables/";
    ret += uuid_to_str(table_id);
    ret += '/';
    return ret;
}

reqlfdb_config_cache::reqlfdb_config_cache()
    : config_version{0} {}
reqlfdb_config_cache::~reqlfdb_config_cache() {}

void config_cache_wipe(reqlfdb_config_cache *cache) {
    reqlfdb_config_cache tmp;
    *cache = std::move(tmp);
}

ukey_string db_by_id_key(const uuid_u &db_id) {
    // We make an aesthetic key.
    return ukey_string{uuid_to_str(db_id)};
}

ukey_string db_by_name_key(const name_string_t &db_name) {
    return ukey_string{db_name.str()};
}

fdb_value_fut<reqlfdb_config_version> transaction_get_config_version(
        FDBTransaction *txn) {
    return fdb_value_fut<reqlfdb_config_version>(transaction_get_c_str(
        txn, REQLFDB_CONFIG_VERSION_KEY));
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
            = transaction_get_config_version(txn);
        return ret;
    }

    // We couldn't find the db name.  Maybe it's just uncached.

    fdb_future fut = transaction_lookup_unique_index(
        txn, REQLFDB_DB_CONFIG_BY_NAME, db_by_name_key(db_name));
    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

    // We block here!
    fdb_value value = future_block_on_value(fut.fut, interruptor);
    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);

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

    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);
    fdb_future fut = transaction_lookup_unique_index(
        txn, REQLFDB_DB_CONFIG_BY_NAME, db_by_name_key(db_name));

    fdb_value value = future_block_on_value(fut.fut, interruptor);

    if (value.present) {
        // A db with this name already exists.
        return false;
    }
    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);

    ASSERT_NO_CORO_WAITING;

    database_id_t db_id = generate_uuid();
    // TODO: Use uniform reql datum primary key serialization, how about that idea?
    ukey_string db_id_key = db_by_id_key(db_id);
    std::string db_id_value = serialize_for_cluster_to_string(db_id);

    transaction_set_pkey_index(txn, REQLFDB_DB_CONFIG_BY_ID, db_id_key, db_name.str());
    transaction_set_unique_index(txn, REQLFDB_DB_CONFIG_BY_NAME,
        db_by_name_key(db_name), db_id_value);

    cv.value++;
    serialize_and_set(txn, REQLFDB_CONFIG_VERSION_KEY, cv);
    return true;
}

bool config_cache_db_drop(
        FDBTransaction *txn, const name_string_t &db_name, const signal_t *interruptor) {
    // TODO: This function must read and verify user permissions when performing this
    // operation.

    // TODO: Ensure caller doesn't pass "rethinkdb".

    ukey_string db_name_key = db_by_name_key(db_name);

    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);
    fdb_future fut = transaction_lookup_unique_index(
        txn, REQLFDB_DB_CONFIG_BY_NAME, db_name_key);

    fdb_value value = future_block_on_value(fut.fut, interruptor);
    database_id_t db_id;
    if (!deserialize_off_fdb_value(value, &db_id)) {
        return false;
    }

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);

    // Add the db_drop job, and remove the db.

    // TODO: This node should claim the job (and add logic to immediately execute it).
    uuid_u claiming_node_id = nil_uuid();
    uuid_u task_id = generate_uuid();
    fdb_job_description desc{
        fdb_job_type::db_drop_job,
        fdb_job_db_drop::make(db_id)};

    // TODO: We could split up the read/write portion of add_fdb_job, mix with above,
    // and avoid double round-trip latency.

    // We _could_ pass in self_node_id, return the fdb_job_info, and claim the job at
    // creation.  Right now, we don't.
    fdb_job_info ignored = add_fdb_job(txn, task_id, claiming_node_id, std::move(desc), interruptor);
    (void)ignored;

    ukey_string db_id_key = db_by_id_key(db_id);

    transaction_erase_pkey_index(txn, REQLFDB_DB_CONFIG_BY_ID, db_id_key);
    transaction_erase_unique_index(txn, REQLFDB_DB_CONFIG_BY_NAME, db_name_key);

    cv.value++;
    serialize_and_set(txn, REQLFDB_CONFIG_VERSION_KEY, cv);

    return true;
}

// The thing to which we append the table name.
std::string table_by_name_ukey_prefix(const database_id_t db_id) {
    // We make an aesthetic key.  UUID's are fixed-width so it's OK.
    return uuid_to_str(db_id) + ".";
}

// Takes a std::string we don't know is a valid table name.  If the format ever changes
// such that an invalid name wouldn't work as a key, we'd have to remove this function.
ukey_string table_by_unverified_name_key(
        const database_id_t &db_id,
        const std::string &table_name) {
    // TODO: Use standard compound index key format, so db_list works well.
    return ukey_string{table_by_name_ukey_prefix(db_id) + table_name};
}

ukey_string table_by_name_key(
        const database_id_t &db_id,
        const name_string_t &table_name) {
    return table_by_unverified_name_key(db_id, table_name.str());
}

std::string unserialize_table_by_name_table_name(key_view key, database_id_t db_id) {
    std::string prefix = unique_index_fdb_key(REQLFDB_TABLE_CONFIG_BY_NAME,
        ukey_string{table_by_name_ukey_prefix(db_id)});

    guarantee(key.length >= int(prefix.size()));
    guarantee(0 == memcmp(prefix.data(), key.data, prefix.size()));

    key_view chopped = key.without_prefix(int(prefix.size()));
    return std::string(as_char(chopped.data), chopped.length);
}

ukey_string table_by_name_bound(
        const database_id_t &db_id,
        const std::string &table_name_bound) {
    // Follows the table_by_name_key format.
    return ukey_string{table_by_name_ukey_prefix(db_id) + table_name_bound};
}


ukey_string table_by_id_key(const uuid_u &table_id) {
    return ukey_string{uuid_to_str(table_id)};
}

// Returns TABLE_CONFIG_BY_NAME range in [min_table_name, +infinity) in database db_id.
fdb_future transaction_get_table_range(
        FDBTransaction *txn, const database_id_t db_id,
        const std::string &min_table_name,
        FDBStreamingMode streaming_mode) {

    std::string lower = unique_index_fdb_key(REQLFDB_TABLE_CONFIG_BY_NAME,
        table_by_name_bound(db_id, min_table_name));
    std::string upper = prefix_end(unique_index_fdb_key(REQLFDB_TABLE_CONFIG_BY_NAME,
        ukey_string{table_by_name_ukey_prefix(db_id)}));

    return fdb_future{fdb_transaction_get_range(txn,
        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(as_uint8(lower.data()), int(lower.size())),
        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(as_uint8(upper.data()), int(upper.size())),
        0,
        0,
        streaming_mode,
        0,
        false,
        false)};
}

// FYI, all callers right now do in fact pass a valid table name.  Doesn't touch the
// reqlfdb_config_version, because this is also used by the db_drop cleanup job.
bool help_remove_table_if_exists(
        FDBTransaction *txn,
        database_id_t db_id,
        const std::string &table_name,
        const signal_t *interruptor) {
    // TODO: Split this function up into future creation part and blocking part, to
    // avoid multiple latency round-trips.

    const ukey_string table_index_key = table_by_unverified_name_key(db_id, table_name);
    fdb_value_fut<namespace_id_t> table_by_name_fut{transaction_lookup_unique_index(
        txn, REQLFDB_TABLE_CONFIG_BY_NAME, table_index_key)};

    namespace_id_t table_id;
    bool table_present = table_by_name_fut.block_and_deserialize(interruptor, &table_id);
    if (!table_present) {
        return false;
    }

    ukey_string table_pkey = table_by_id_key(table_id);

    // Wipe table config (from pkey and indices), and wipe table contents.
    transaction_erase_pkey_index(txn, REQLFDB_TABLE_CONFIG_BY_ID, table_pkey);
    transaction_erase_unique_index(txn, REQLFDB_TABLE_CONFIG_BY_NAME, table_index_key);

    std::string prefix = table_key_prefix(table_id);
    std::string end = prefix_end(prefix);
    fdb_transaction_clear_range(txn, as_uint8(prefix.data()), int(prefix.size()),
        as_uint8(end.data()), int(end.size()));

    return true;
}

bool config_cache_table_drop(
        FDBTransaction *txn, database_id_t db_id, const name_string_t &table_name,
        const signal_t *interruptor) {

    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

    const ukey_string table_index_key = table_by_name_key(db_id, table_name);
    const ukey_string db_pkey_key = db_by_id_key(db_id);
    fdb_future table_by_name_fut = transaction_lookup_unique_index(
        txn, REQLFDB_TABLE_CONFIG_BY_NAME, table_index_key);
    fdb_future db_by_id_fut = transaction_lookup_pkey_index(
        txn, REQLFDB_DB_CONFIG_BY_ID, db_pkey_key);

    fdb_value table_by_name_value
        = future_block_on_value(table_by_name_fut.fut, interruptor);
    if (!table_by_name_value.present) {
        return false;
    }

    fdb_value db_by_id_value = future_block_on_value(db_by_id_fut.fut, interruptor);
    if (!db_by_id_value.present) {
        // TODO: This might mean the id came from an out-of-date cache.  We should
        // report the error back to the user (which is broken) but with a distinguished
        // error return value than "table doesn't exist".
        return false;
    }

    // Okay, the db's present, and the table's present.  Drop the table.

    bool table_present = help_remove_table_if_exists(
        txn, db_id, table_name.str(), interruptor);
    if (!table_present) {
        return false;
    }

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);

    cv.value++;
    serialize_and_set(txn, REQLFDB_CONFIG_VERSION_KEY, cv);
    return true;
}

bool config_cache_table_create(
        FDBTransaction *txn,
        const table_config_t &config,
        const signal_t *interruptor) {
    // TODO: This function must read and verify user permissions when performing this
    // operation.

    const database_id_t db_id = config.basic.database;
    const name_string_t &table_name = config.basic.name;

    // TODO: Ensure caller doesn't try to create table for "rethinkdb" database.

    const ukey_string table_index_key = table_by_name_key(db_id, table_name);
    const ukey_string db_pkey_key = db_by_id_key(db_id);

    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);
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

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);

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
    ukey_string table_pkey = table_by_id_key(table_id);
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


