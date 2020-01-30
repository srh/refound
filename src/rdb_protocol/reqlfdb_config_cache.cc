#include "rdb_protocol/reqlfdb_config_cache.hpp"
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"

#include "clustering/administration/tables/table_metadata.hpp"
#include "clustering/id_types.hpp"
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

void reqlfdb_config_cache::wipe() {
    reqlfdb_config_cache tmp;
    *this = std::move(tmp);
}

void reqlfdb_config_cache::add_db(
        const database_id_t &db_id, const name_string_t &db_name) {
    db_id_index.emplace(db_id, db_name);
    db_name_index.emplace(db_name, db_id);
}

void reqlfdb_config_cache::add_table(
        const namespace_id_t &table_id, const table_config_t &config) {
    table_id_index.emplace(table_id, config);
    table_name_index.emplace(
        std::make_pair(config.basic.database, config.basic.name),
        table_id);
}

// TODO: Remove.
void config_cache_wipe(reqlfdb_config_cache *cache) {
    cache->wipe();
}

ukey_string db_by_id_key(const database_id_t &db_id) {
    // We make an aesthetic key.
    return ukey_string{uuid_to_str(db_id)};
}

ukey_string db_by_name_key(const name_string_t &db_name) {
    return ukey_string{db_name.str()};
}

name_string_t unparse_db_by_name_key(key_view k) {
    name_string_t str;
    bool success = str.assign_value(std::string(as_char(k.data), size_t(k.length)));
    guarantee(success, "unparse_db_by_name_key got bad name_string_t");
    return str;
}


constexpr const char *table_by_name_separator = ".";

// The thing to which we append the table name.
std::string table_by_name_ukey_prefix(const database_id_t db_id) {
    // We make an aesthetic key.  UUID's are fixed-width so it's OK.
    return uuid_to_str(db_id) + table_by_name_separator;
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

std::string unserialize_table_by_name_table_name_part(key_view table_name_part) {
    return std::string(as_char(table_name_part.data), table_name_part.length);
}

std::pair<database_id_t, std::string> unserialize_table_by_name_key(key_view key) {
    std::string prefix = REQLFDB_TABLE_CONFIG_BY_NAME;
    key_view chopped = key.guarantee_without_prefix(prefix);
    std::pair<database_id_t, std::string> ret;
    key_view table_name = chopped.without_prefix(uuid_u::kStringSize + strlen(table_by_name_separator));
    // TODO: rassert_prefix function, that I can lower to an assertion at some point.
    guarantee(chopped.data[uuid_u::kStringSize] == table_by_name_separator[0] &&
              strlen(table_by_name_separator) == 1);
    ret.second = unserialize_table_by_name_table_name_part(table_name);
    bool is_uuid = str_to_uuid(as_char(chopped.data), uuid_u::kStringSize, &ret.first.value);
    guarantee(is_uuid);
    return ret;
}

std::string unserialize_table_by_name_table_name(key_view key, database_id_t db_id) {
    std::string prefix = unique_index_fdb_key(REQLFDB_TABLE_CONFIG_BY_NAME,
        ukey_string{table_by_name_ukey_prefix(db_id)});

    key_view chopped = key.guarantee_without_prefix(prefix);
    return unserialize_table_by_name_table_name_part(chopped);
}

ukey_string table_by_name_bound(
        const database_id_t &db_id,
        const std::string &table_name_bound) {
    // Follows the table_by_name_key format.
    return ukey_string{table_by_name_ukey_prefix(db_id) + table_name_bound};
}


ukey_string table_by_id_key(const namespace_id_t &table_id) {
    return ukey_string{uuid_to_str(table_id)};
}


fdb_value_fut<reqlfdb_config_version> transaction_get_config_version(
        FDBTransaction *txn) {
    return fdb_value_fut<reqlfdb_config_version>(transaction_get_c_str(
        txn, REQLFDB_CONFIG_VERSION_KEY));
}

optional<config_info<database_id_t>> try_lookup_cached_db(
        const reqlfdb_config_cache *cache, const name_string_t &db_name) {
    optional<config_info<database_id_t>> ret;
    ASSERT_NO_CORO_WAITING;  // mutex assertion
    auto it = cache->db_name_index.find(db_name);
    if (it != cache->db_name_index.end()) {
        ret.emplace();
        ret->ci_value = it->second;
        ret->ci_cv = cache->config_version;
    }
    return ret;
}

optional<config_info<namespace_id_t>> try_lookup_cached_table(
        const reqlfdb_config_cache *cache,
        const std::pair<database_id_t, name_string_t> &table_name) {
    optional<config_info<namespace_id_t>> ret;
    ASSERT_NO_CORO_WAITING;  // mutex assertion
    auto it = cache->table_name_index.find(table_name);
    if (it == cache->table_name_index.end()) {
        ret.emplace();
        ret->ci_value = it->second;
        ret->ci_cv = cache->config_version;
    }
    return ret;
}



config_info<optional<database_id_t>>
config_cache_retrieve_db_by_name(
        const reqlfdb_config_cache *cache,
        FDBTransaction *txn,
        const name_string_t &db_name, const signal_t *interruptor) {
    fdb_future fut = transaction_lookup_unique_index(
        txn, REQLFDB_DB_CONFIG_BY_NAME, db_by_name_key(db_name));
    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

    // Block here (1st round-trip)
    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);

    if (cv.value < cache->config_version.value) {
        // Throw a retryable exception.
        throw fdb_transaction_exception(REQLFDB_not_committed);
    }

    // Block here (1st round-trip)
    fdb_value value = future_block_on_value(fut.fut, interruptor);

    database_id_t id;
    bool present = deserialize_off_fdb_value(value, &id);

    config_info<optional<database_id_t>> ret;
    ret.ci_cv = cv;
    if (present) {
        ret.ci_value.set(id);
    }
    return ret;
}

config_info<optional<std::pair<namespace_id_t, table_config_t>>>
config_cache_retrieve_table_by_name(
        const reqlfdb_config_cache *cc, FDBTransaction *txn,
        const std::pair<database_id_t, name_string_t> &db_table_name,
        const signal_t *interruptor) {
    const ukey_string table_index_key = table_by_name_key(
        db_table_name.first, db_table_name.second);

    fdb_future table_id_fut = transaction_lookup_unique_index(
        txn, REQLFDB_TABLE_CONFIG_BY_NAME, table_index_key);
    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

    // Block here (1st round-trip; for simplicity we always check config version)
    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);

    if (cv.value < cc->config_version.value) {
        // Throw a retryable exception.
        throw fdb_transaction_exception(REQLFDB_not_committed);
    }

    // Block here (still 1st round-trip)
    fdb_value table_id_value = future_block_on_value(table_id_fut.fut, interruptor);

    namespace_id_t table_id;
    bool present = deserialize_off_fdb_value(table_id_value, &table_id);

    config_info<optional<std::pair<namespace_id_t, table_config_t>>> ret;
    ret.ci_cv = cc->config_version;
    if (!present) {
        return ret;
    }

    // Table exists, gotta do second lookup.
    ukey_string table_id_key = table_by_id_key(table_id);

    fdb_future table_by_id_fut = transaction_lookup_pkey_index(
        txn, REQLFDB_TABLE_CONFIG_BY_ID, table_id_key);

    // Block here (2nd round-trip)
    fdb_value config_value = future_block_on_value(table_by_id_fut.fut, interruptor);

    ret.ci_value.emplace();
    ret.ci_value->first = table_id;

    bool config_present = deserialize_off_fdb_value(config_value, &ret.ci_value->second);
    guarantee(config_present);  // TODO: Nice error?  FDB in bad state.

    guarantee(db_table_name.first == ret.ci_value->second.basic.database);  // TODO: fdb in bad state
    guarantee(db_table_name.second == ret.ci_value->second.basic.name);  // TODO: fdb in bad state

    return ret;
}


bool config_cache_db_create(
        FDBTransaction *txn,
        const auth::user_context_t &user_context,
        const name_string_t &db_name,
        const database_id_t &new_db_id,
        const signal_t *interruptor) {
    // TODO: This function must read and verify user permissions when performing this
    // operation.
    guarantee(db_name.str() != "rethinkdb",
        "config_cache_db_create should never get queries for system tables");

    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);
    auth::fdb_user_fut<auth::config_permission> auth_fut
        = user_context.transaction_require_config_permission(txn);
    fdb_future fut = transaction_lookup_unique_index(
        txn, REQLFDB_DB_CONFIG_BY_NAME, db_by_name_key(db_name));

    auth_fut.block_and_check(interruptor);

    fdb_value value = future_block_on_value(fut.fut, interruptor);
    if (value.present) {
        // A db with this name already exists.
        return false;
    }
    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);

    ASSERT_NO_CORO_WAITING;

    // TODO: Use uniform reql datum primary key serialization, how about that idea?
    ukey_string db_id_key = db_by_id_key(new_db_id);
    std::string db_id_value = serialize_for_cluster_to_string(new_db_id);

    transaction_set_pkey_index(txn, REQLFDB_DB_CONFIG_BY_ID, db_id_key, db_name.str());
    transaction_set_unique_index(txn, REQLFDB_DB_CONFIG_BY_NAME,
        db_by_name_key(db_name), db_id_value);

    cv.value++;
    serialize_and_set(txn, REQLFDB_CONFIG_VERSION_KEY, cv);
    return true;
}

optional<database_id_t> config_cache_db_drop(
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
        return r_nullopt;
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

    return make_optional(db_id);
}

// Returns TABLE_CONFIG_BY_NAME range in database db_id, in [lower_bound_table_name,
// +infinity), if closed, and (lower_bound_table_name, +infinity), if open.
fdb_future transaction_get_table_range(
        FDBTransaction *txn, const database_id_t db_id,
        const std::string &lower_bound_table_name, bool closed,
        FDBStreamingMode streaming_mode) {

    std::string lower = unique_index_fdb_key(REQLFDB_TABLE_CONFIG_BY_NAME,
        table_by_name_bound(db_id, lower_bound_table_name));
    std::string upper = prefix_end(unique_index_fdb_key(REQLFDB_TABLE_CONFIG_BY_NAME,
        ukey_string{table_by_name_ukey_prefix(db_id)}));

    return fdb_future{fdb_transaction_get_range(txn,
        as_uint8(lower.data()), int(lower.size()), !closed, 1,
        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(as_uint8(upper.data()), int(upper.size())),
        0,
        0,
        streaming_mode,
        0,
        false,
        false)};
}

void help_remove_table(
        FDBTransaction *txn,
        database_id_t db_id,
        const namespace_id_t &table_id,
        const std::string &table_name) {
    ukey_string table_pkey = table_by_id_key(table_id);
    ukey_string table_index_key = table_by_unverified_name_key(db_id, table_name);

    // Wipe table config (from pkey and indices), and wipe table contents.
    transaction_erase_pkey_index(txn, REQLFDB_TABLE_CONFIG_BY_ID, table_pkey);
    transaction_erase_unique_index(txn, REQLFDB_TABLE_CONFIG_BY_NAME, table_index_key);

    std::string prefix = table_key_prefix(table_id);
    std::string end = prefix_end(prefix);
    fdb_transaction_clear_range(txn, as_uint8(prefix.data()), int(prefix.size()),
        as_uint8(end.data()), int(end.size()));
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

    ukey_string table_index_key = table_by_unverified_name_key(db_id, table_name);
    fdb_value_fut<namespace_id_t> table_by_name_fut{transaction_lookup_unique_index(
        txn, REQLFDB_TABLE_CONFIG_BY_NAME, table_index_key)};

    namespace_id_t table_id;
    bool table_present = table_by_name_fut.block_and_deserialize(interruptor, &table_id);
    if (!table_present) {
        return false;
    }

    help_remove_table(txn, db_id, table_id, table_name);
    return true;
}


optional<std::pair<namespace_id_t, table_config_t>> config_cache_table_drop(
        FDBTransaction *txn, database_id_t db_id, const name_string_t &table_name,
        const signal_t *interruptor) {

    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

    const ukey_string table_index_key = table_by_name_key(db_id, table_name);
    fdb_future table_by_name_fut = transaction_lookup_unique_index(
        txn, REQLFDB_TABLE_CONFIG_BY_NAME, table_index_key);
    const ukey_string db_pkey_key = db_by_id_key(db_id);
    fdb_future db_by_id_fut = transaction_lookup_pkey_index(
        txn, REQLFDB_DB_CONFIG_BY_ID, db_pkey_key);

    fdb_value db_by_id_value = future_block_on_value(db_by_id_fut.fut, interruptor);
    if (!db_by_id_value.present) {
        throw config_version_exc_t();
    }

    fdb_value table_by_name_value
        = future_block_on_value(table_by_name_fut.fut, interruptor);
    namespace_id_t table_id;
    if (!deserialize_off_fdb_value(table_by_name_value, &table_id)) {
        return r_nullopt;
    }

    ukey_string table_pkey = table_by_id_key(table_id);
    // Now for an extra round-trip (to produce pretty output for the user), we read the
    // table config!
    fdb_future table_by_id_fut = transaction_lookup_pkey_index(
        txn, REQLFDB_TABLE_CONFIG_BY_ID, table_pkey);

    fdb_value table_by_id_value = future_block_on_value(table_by_id_fut.fut, interruptor);
    table_config_t config;
    if (!deserialize_off_fdb_value(table_by_id_value, &config)) {
        // TODO: graceful error handling for corrupt fdb
        crash("No table_config_by_id for key found in index");
    }

    // Okay, the db's present, and the table's present.  Drop the table.

    help_remove_table(txn, db_id, table_id, table_name.str());

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);

    cv.value++;
    serialize_and_set(txn, REQLFDB_CONFIG_VERSION_KEY, cv);
    return make_optional(std::make_pair(table_id, std::move(config)));
}

bool config_cache_table_create(
        FDBTransaction *txn,
        const auth::user_context_t &user_context,
        const namespace_id_t &new_table_id,
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
    auth::fdb_user_fut<auth::db_config_permission> auth_fut
        = user_context.transaction_require_db_config_permission(txn, db_id);
    fdb_future table_by_name_fut = transaction_lookup_unique_index(
        txn, REQLFDB_TABLE_CONFIG_BY_NAME, table_index_key);
    fdb_future db_by_id_fut = transaction_lookup_pkey_index(
        txn, REQLFDB_DB_CONFIG_BY_ID, db_pkey_key);

    auth_fut.block_and_check(interruptor);
    fdb_value table_by_name_value
        = future_block_on_value(table_by_name_fut.fut, interruptor);
    fdb_value db_by_id_value = future_block_on_value(db_by_id_fut.fut, interruptor);

    if (table_by_name_value.present) {
        // Table already exists.
        return false;
    }

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);

    if (!db_by_id_value.present) {
        // TODO: We can throw this from within a retry loop, right?
        throw config_version_exc_t();
    }

    ASSERT_NO_CORO_WAITING;

    // Okay, the db's present, the table is not present.  Create the table.

    // TODO: Figure out how to name these sorts of variables.
    ukey_string table_pkey = table_by_id_key(new_table_id);
    std::string table_config_value = serialize_for_cluster_to_string(config);
    std::string table_pkey_value = serialize_for_cluster_to_string(new_table_id);

    transaction_set_pkey_index(txn, REQLFDB_TABLE_CONFIG_BY_ID, table_pkey,
        table_config_value);
    transaction_set_unique_index(txn, REQLFDB_TABLE_CONFIG_BY_NAME, table_index_key,
        table_pkey_value);

    cv.value++;
    serialize_and_set(txn, REQLFDB_CONFIG_VERSION_KEY, cv);

    return true;
}


template <class Callable>
void transaction_read_whole_range_coro(FDBTransaction *txn,
        std::string begin, const std::string &end,
        const signal_t *interruptor,
        Callable &&cb) {
    bool or_equal = true;
    // We need to get elements greater than (or_equal, if true) begin, less than end.
    for (;;) {
        fdb_future fut{
            fdb_transaction_get_range(txn,
                    as_uint8(begin.data()), int(begin.size()), !or_equal, 1,
                    FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(as_uint8(end.data()), int(end.size())),
                    0,
                    0,
                    FDB_STREAMING_MODE_WANT_ALL,
                    0,
                    false,
                    false)};
        fut.block_coro(interruptor);

        const FDBKeyValue *kvs;
        int kv_count;
        fdb_bool_t more;
        fdb_error_t err = fdb_future_get_keyvalue_array(fut.fut, &kvs, &kv_count, &more);
        check_for_fdb_transaction(err);
        or_equal = false;
        for (int i = 0; i < kv_count; ++i) {
            if (!cb(kvs[i])) {
                return;
            }
        }
        if (more) {
            if (kv_count > 0) {
                const FDBKeyValue &kv = kvs[kv_count - 1];
                begin = std::string(void_as_char(kv.key), size_t(kv.key_length));
            }
        } else {
            return;
        }
    }
}

// TODO (out of context): ql::db_t should do nothing but hold info about whether it
// was by name or by value, don't actually do the lookup right away.

std::vector<counted_t<const ql::db_t>> config_cache_db_list(
        FDBTransaction *txn,
        const signal_t *interruptor) {
    std::string db_by_name_prefix = REQLFDB_DB_CONFIG_BY_NAME;
    std::string db_by_name_end = prefix_end(db_by_name_prefix);
    std::vector<counted_t<const ql::db_t>> dbs;
    transaction_read_whole_range_coro(txn,
        db_by_name_prefix, db_by_name_end, interruptor,
        [&dbs, &db_by_name_prefix](const FDBKeyValue &kv) {
            key_view whole_key{void_as_uint8(kv.key), kv.key_length};
            key_view key = whole_key.guarantee_without_prefix(db_by_name_prefix);
            name_string_t name = unparse_db_by_name_key(key);
            database_id_t db_id = database_id_t{str_to_uuid(void_as_char(kv.value), size_t(kv.value_length))};
            dbs.push_back(make_counted<ql::db_t>(db_id, name));
            return true;
        });
    return dbs;
}

// TODO: If we can't iterate the tables in a single txn, we could do a snapshot read
// or check the config version, or something.

// This is listed in ascending order.
std::vector<name_string_t> config_cache_table_list(
        FDBTransaction *txn,
        const database_id_t &db_id,
        const signal_t *interruptor) {

    const ukey_string db_pkey_key = db_by_id_key(db_id);
    fdb_future db_by_id_fut = transaction_lookup_pkey_index(
        txn, REQLFDB_DB_CONFIG_BY_ID, db_pkey_key);

    std::vector<name_string_t> table_names;

    std::string prefix = unique_index_fdb_key(REQLFDB_TABLE_CONFIG_BY_NAME,
        ukey_string{table_by_name_ukey_prefix(db_id)});
    std::string end = prefix_end(prefix);

    transaction_read_whole_range_coro(txn, prefix, end, interruptor,
    [&prefix, &table_names](const FDBKeyValue &kv) {
        key_view whole_key{void_as_uint8(kv.key), kv.key_length};
        key_view table_name_part = whole_key.guarantee_without_prefix(prefix);
        // Basically unserialize_table_by_name_table_name without recomputing
        // the prefix.
        std::string table_name
            = unserialize_table_by_name_table_name_part(table_name_part);
        name_string_t name;
        bool res = name.assign_value(table_name);
        guarantee(res, "invalid table name unserialized from table_by_name key");
        table_names.push_back(std::move(name));
        return true;
    });


    fdb_value db_by_id_value = future_block_on_value(db_by_id_fut.fut, interruptor);
    if (!db_by_id_value.present) {
        throw config_version_exc_t();
    }

    return table_names;
}

ukey_string username_pkey(const auth::username_t &username) {
    return ukey_string{username.to_string()};
}

fdb_value_fut<auth::user_t> transaction_get_user(
        FDBTransaction *txn,
        const auth::username_t &username) {
    ukey_string pkey = username_pkey(username);
    return fdb_value_fut<auth::user_t>{transaction_lookup_pkey_index(
        txn, REQLFDB_USERS_BY_USERNAME, pkey)};
}
