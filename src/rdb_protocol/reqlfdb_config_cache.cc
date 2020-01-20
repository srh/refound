#include "rdb_protocol/reqlfdb_config_cache.hpp"
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"

#include "clustering/administration/tables/table_metadata.hpp"
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
    fdb_value cv_value = future_block_on_value(cv_fut.fut, interruptor);

    ASSERT_NO_CORO_WAITING;
    reqlfdb_config_version cv;
    {
        bool cv_present = deserialize_off_fdb_value(cv_value, &cv);
        guarantee(cv_present, "config version not present");
    }

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

// TODO: cache is unused.
bool config_cache_db_create(
        reqlfdb_config_cache *cache, FDBTransaction *txn,
        const name_string_t &db_name, const signal_t *interruptor) {
    // TODO: This function must read and verify user permissions when performing this
    // operation.
    guarantee(db_name.str() != "rethinkdb",
        "config_cache_db_create should never get queries for system tables");

    fdb_future cv_fut = transaction_get_c_str(txn, REQLFDB_CONFIG_VERSION_KEY);
    fdb_future fut = transaction_lookup_unique_index(
        txn, REQLFDB_DB_CONFIG_BY_NAME, db_name.str());

    fdb_value cv_value = future_block_on_value(cv_fut.fut, interruptor);
    fdb_value value = future_block_on_value(fut.fut, interruptor);

    if (value.present) {
        return false;
    }
    reqlfdb_config_version cv;
    {
        bool cv_present = deserialize_off_fdb_value(cv_value, &cv);
        guarantee(cv_present, "config version not present");
    }

    database_id_t db_id = generate_uuid();
    // TODO: Use uniform reql datum primary key serialization, how about that idea?
    std::string db_id_value = uuid_to_str(db_id);

    transaction_set_pkey_index(txn, REQLFDB_DB_CONFIG_BY_ID, db_id_value, db_name.str());
    transaction_set_unique_index(txn, REQLFDB_DB_CONFIG_BY_NAME, db_name.str(), db_id_value);

    cv.value++;
    serialize_and_set(txn, REQLFDB_CONFIG_VERSION_KEY, cv);
    return true;
}
