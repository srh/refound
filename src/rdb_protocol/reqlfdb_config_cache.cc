#include "rdb_protocol/reqlfdb_config_cache.hpp"
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"

#include "clustering/administration/auth/user_fut.hpp"
#include "clustering/administration/tables/table_metadata.hpp"
#include "clustering/id_types.hpp"
#include "containers/archive/string_stream.hpp"
#include "fdb/btree_utils.hpp"
#include "fdb/index.hpp"
#include "fdb/jobs.hpp"
#include "fdb/jobs/index_create.hpp"
#include "fdb/reql_fdb.hpp"
#include "fdb/reql_fdb_utils.hpp"
#include "fdb/system_tables.hpp"

struct db_config_by_id {
    using ukey_type = database_id_t;
    using value_type = name_string_t;
    static constexpr const char *prefix = REQLFDB_DB_CONFIG_BY_ID;

    static ukey_string ukey_str(const ukey_type &k) {
        // We make an aesthetic key.
        return ukey_string{uuid_to_str(k)};
    }
};

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

std::string table_config_by_name_prefix(const database_id_t &db_id) {
    return unique_index_fdb_key(REQLFDB_TABLE_CONFIG_BY_NAME,
        ukey_string{table_by_name_ukey_prefix(db_id)});
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
    if (it != cache->table_name_index.end()) {
        ret.emplace();
        ret->ci_value = it->second;
        ret->ci_cv = cache->config_version;
    }
    return ret;
}

optional<config_info<auth::user_t>>
try_lookup_cached_user(
        const reqlfdb_config_cache *cache, const auth::username_t &username) {
    optional<config_info<auth::user_t>> ret;
    ASSERT_NO_CORO_WAITING;  // mutex assertion
    auto it = cache->auth_index.find(username);
    if (it != cache->auth_index.end()) {
        ret.emplace();
        ret->ci_value = it->second;
        ret->ci_cv = cache->config_version;
    }
    return ret;
}


config_info<optional<database_id_t>>
config_cache_retrieve_db_by_name(
        const reqlfdb_config_version config_cache_cv,
        FDBTransaction *txn,
        const name_string_t &db_name, const signal_t *interruptor) {
    fdb_future fut = transaction_lookup_unique_index(
        txn, REQLFDB_DB_CONFIG_BY_NAME, db_by_name_key(db_name));
    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

    // Block here (1st round-trip)
    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);

    if (cv.value < config_cache_cv.value) {
        // Throw a retryable exception.
        // TODO: Should be impossible, just a guarantee failure.
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

// OOO: Caller of these three fns need to check cv and wipe/refresh config cache.
config_info<optional<std::pair<namespace_id_t, table_config_t>>>
config_cache_retrieve_table_by_name(
        const reqlfdb_config_version config_cache_cv, FDBTransaction *txn,
        const std::pair<database_id_t, name_string_t> &db_table_name,
        const signal_t *interruptor) {
    const ukey_string table_index_key = table_by_name_key(
        db_table_name.first, db_table_name.second);

    fdb_future table_id_fut = transaction_lookup_unique_index(
        txn, REQLFDB_TABLE_CONFIG_BY_NAME, table_index_key);
    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);

    if (cv.value < config_cache_cv.value) {
        // Throw a retryable exception.
        // TODO: Should be impossible, just a guarantee failure.
        throw fdb_transaction_exception(REQLFDB_not_committed);
    }
    // TODO: Examine this, the db function, and the user function for some config_version exception?

    fdb_value table_id_value = future_block_on_value(table_id_fut.fut, interruptor);

    namespace_id_t table_id;
    bool present = deserialize_off_fdb_value(table_id_value, &table_id);

    config_info<optional<std::pair<namespace_id_t, table_config_t>>> ret;
    ret.ci_cv = cv;
    if (!present) {
        return ret;
    }

    // Table exists, gotta do second lookup.
    fdb_value_fut<table_config_t> table_by_id_fut
        = transaction_lookup_uq_index<table_config_by_id>(txn, table_id);

    ret.ci_value.emplace();
    ret.ci_value->first = table_id;

    // Block here (2nd round-trip)
    bool config_present = table_by_id_fut.block_and_deserialize(interruptor, &ret.ci_value->second);
    guarantee(config_present);  // TODO: Nice error?  FDB in bad state.

    guarantee(db_table_name.first == ret.ci_value->second.basic.database);  // TODO: fdb in bad state
    guarantee(db_table_name.second == ret.ci_value->second.basic.name);  // TODO: fdb in bad state

    return ret;
}

config_info<optional<auth::user_t>>
config_cache_retrieve_user_by_name(
        const reqlfdb_config_version config_cache_cv, FDBTransaction *txn,
        const auth::username_t &username, const signal_t *interruptor) {
    fdb_value_fut<auth::user_t> user_fut = transaction_get_user(txn, username);
    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
    if (cv.value < config_cache_cv.value) {
        // Throw a retryable exception.
        throw fdb_transaction_exception(REQLFDB_not_committed);
    }

    config_info<optional<auth::user_t>> ret;
    ret.ci_cv = cv;

    auth::user_t user;
    if (user_fut.block_and_deserialize(interruptor, &user)) {
        ret.ci_value.set(std::move(user));
    }
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
    std::string db_id_value = serialize_for_cluster_to_string(new_db_id);

    transaction_set_uq_index<db_config_by_id>(txn, new_db_id, db_name);
    transaction_set_unique_index(txn, REQLFDB_DB_CONFIG_BY_NAME,
        db_by_name_key(db_name), db_id_value);

    cv.value++;
    transaction_set_config_version(txn, cv);
    return true;
}


optional<database_id_t> config_cache_db_drop(
        FDBTransaction *txn, const auth::user_context_t &user_context,
        const name_string_t &db_name, const signal_t *interruptor) {
    ukey_string db_name_key = db_by_name_key(db_name);

    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);
    fdb_future fut = transaction_lookup_unique_index(
        txn, REQLFDB_DB_CONFIG_BY_NAME, db_name_key);

    fdb_value value = future_block_on_value(fut.fut, interruptor);
    database_id_t db_id;
    if (!deserialize_off_fdb_value(value, &db_id)) {
        return r_nullopt;
    }

    // TODO: We could get the table id's concurrently (in a coro).
    std::vector<namespace_id_t> table_ids;
    {
        std::string prefix = table_config_by_name_prefix(db_id);
        transaction_read_whole_range_coro(txn, prefix, prefix_end(prefix), interruptor,
        [&](const FDBKeyValue &kv) {
            namespace_id_t table_id;
            deserialize_off_fdb(void_as_uint8(kv.value), kv.value_length, &table_id);
            table_ids.push_back(table_id);
            return true;
        });
    }

    // TODO: We could totally get the user fut concurrently above, pass it in here.
    auth::fdb_user_fut<auth::db_multi_table_config_permission> auth_fut
        = user_context.transaction_require_db_multi_table_config_permission(txn,
            db_id,
            std::move(table_ids));

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);

    // Add the db_drop job, and remove the db.

    // TODO: This node should claim the job (and add logic to immediately execute it).
    fdb_node_id claiming_node_id{nil_uuid()};
    fdb_shared_task_id task_id{generate_uuid()};
    fdb_job_description desc{
        fdb_job_type::db_drop_job,
        fdb_job_db_drop::make(db_id),
        fdb_job_index_create{}};

    // TODO: We could split up the read/write portion of add_fdb_job, mix with above,
    // and avoid double round-trip latency.

    // We _could_ pass in self_node_id, return the fdb_job_info, and claim the job at
    // creation.  Right now, we don't.
    fdb_job_info ignored = add_fdb_job(txn, task_id, claiming_node_id, std::move(desc), interruptor);
    (void)ignored;

    // Check the auth fut after a round-trip in add_fdb_job.
    auth_fut.block_and_check(interruptor);

    transaction_erase_uq_index<db_config_by_id>(txn, db_id);
    transaction_erase_unique_index(txn, REQLFDB_DB_CONFIG_BY_NAME, db_name_key);

    cv.value++;
    transaction_set_config_version(txn, cv);

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

void transaction_clear_prefix_range(FDBTransaction *txn, const std::string &prefix) {
    std::string end = prefix_end(prefix);
    fdb_transaction_clear_range(txn, as_uint8(prefix.data()), int(prefix.size()),
        as_uint8(end.data()), int(end.size()));
}

void help_remove_table(
        FDBTransaction *txn,
        const namespace_id_t &table_id,
        const table_config_t &config,
        const signal_t *interruptor) {
    ukey_string table_index_key
        = table_by_name_key(config.basic.database, config.basic.name);

    // Wipe table config (from pkey and indices), and wipe table contents.
    transaction_erase_uq_index<table_config_by_id>(txn, table_id);
    transaction_erase_unique_index(txn, REQLFDB_TABLE_CONFIG_BY_NAME, table_index_key);

    // TODO: Parallelize this.
    // For any sindexes with jobs, remove their jobs.
    for (auto &&pair : config.fdb_sindexes) {
        fdb_shared_task_id task = pair.second.creation_task_or_nil;
        if (!task.value.is_nil()) {
            remove_fdb_task_and_jobs(txn, task, interruptor);
        }
    }

    std::string prefix = rfdb::table_key_prefix(table_id);
    transaction_clear_prefix_range(txn, prefix);
}

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

    fdb_value_fut<table_config_t> table_by_id_fut
        = transaction_lookup_uq_index<table_config_by_id>(txn, table_id);
    table_config_t config = table_by_id_fut.block_and_deserialize(interruptor);

    help_remove_table(txn, table_id, config, interruptor);
    return true;
}


optional<std::pair<namespace_id_t, table_config_t>> config_cache_table_drop(
        FDBTransaction *txn,
        reqlfdb_config_version expected_cv,
        const auth::user_context_t &user_context,
        const database_id_t &db_id, const name_string_t &table_name,
        const signal_t *interruptor) {

    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

    const ukey_string table_index_key = table_by_name_key(db_id, table_name);
    fdb_future table_by_name_fut = transaction_lookup_unique_index(
        txn, REQLFDB_TABLE_CONFIG_BY_NAME, table_index_key);
    fdb_future db_by_id_fut = transaction_lookup_uq_index<db_config_by_id>(
        txn, db_id);

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
    check_cv(expected_cv, cv);

    {
        fdb_value db_by_id_value = future_block_on_value(db_by_id_fut.fut, interruptor);
        guarantee(db_by_id_value.present, "db missing, fdb state invalid");  // TODO: Fdb, error message, etc.
    }

    fdb_value table_by_name_value
        = future_block_on_value(table_by_name_fut.fut, interruptor);
    namespace_id_t table_id;
    if (!deserialize_off_fdb_value(table_by_name_value, &table_id)) {
        return r_nullopt;
    }

    // We use the table_id to read the table config to produce pretty output for the user.
    // We use the table_id to check permissions, too.
    auth::fdb_user_fut<auth::db_table_config_permission> auth_fut
        = user_context.transaction_require_db_and_table_config_permission(txn, db_id, table_id);

    fdb_value_fut<table_config_t> table_by_id_fut
        = transaction_lookup_uq_index<table_config_by_id>(txn, table_id);

    auth_fut.block_and_check(interruptor);
    fdb_value table_by_id_value = future_block_on_value(table_by_id_fut.fut, interruptor);
    table_config_t config;
    if (!deserialize_off_fdb_value(table_by_id_value, &config)) {
        // TODO: graceful error handling for corrupt fdb
        crash("No table_config_by_id for key found in index");
    }

    // Okay, the db's present, and the table's present.  Drop the table.

    help_remove_table(txn, table_id, config, interruptor);

    cv.value++;
    transaction_set_config_version(txn, cv);
    return make_optional(std::make_pair(table_id, std::move(config)));
}

bool config_cache_table_create(
        FDBTransaction *txn,
        reqlfdb_config_version expected_cv,
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

    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);
    auth::fdb_user_fut<auth::db_config_permission> auth_fut
        = user_context.transaction_require_db_config_permission(txn, db_id);
    fdb_future table_by_name_fut = transaction_lookup_unique_index(
        txn, REQLFDB_TABLE_CONFIG_BY_NAME, table_index_key);
    fdb_future db_by_id_fut
        = transaction_lookup_uq_index<db_config_by_id>(txn, db_id);

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
    check_cv(expected_cv, cv);

    auth_fut.block_and_check(interruptor);
    fdb_value table_by_name_value
        = future_block_on_value(table_by_name_fut.fut, interruptor);

    if (table_by_name_value.present) {
        // Table already exists.
        return false;
    }

    {
        fdb_value db_by_id_value = future_block_on_value(db_by_id_fut.fut, interruptor);
        guarantee(db_by_id_value.present, "Db by id missing, invalid fdb state");  // TODO: fdb, msg, etc.
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
    transaction_set_config_version(txn, cv);

    return true;
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
        reqlfdb_config_version expected_cv,
        const database_id_t &db_id,
        const signal_t *interruptor) {

    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

    std::vector<name_string_t> table_names;

    std::string prefix = table_config_by_name_prefix(db_id);

    transaction_read_whole_range_coro(txn, prefix, prefix_end(prefix), interruptor,
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

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
    check_cv(expected_cv, cv);

    return table_names;
}

MUST_USE bool config_cache_sindex_create(
        FDBTransaction *txn,
        const auth::user_context_t &user_context,
        reqlfdb_config_version expected_cv,
        const database_id_t &db_id,
        const namespace_id_t &table_id,
        const std::string &index_name,
        const sindex_id_t &new_sindex_id,
        const fdb_shared_task_id &new_index_create_task_id,
        const sindex_config_t &sindex_config,
        const signal_t *interruptor) {
    // TODO: We need to verify db name -> id, and table name -> id mapping that was used (or config version that was used) still applies.

    auth::fdb_user_fut<auth::db_table_config_permission> auth_fut
        = user_context.transaction_require_db_and_table_config_permission(
            txn, db_id, table_id);
    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

    fdb_value_fut<table_config_t> table_config_fut
        = transaction_lookup_uq_index<table_config_by_id>(txn, table_id);

    const std::string pkey_prefix = rfdb::table_pkey_prefix(table_id);
    const std::string pkey_prefix_end = prefix_end(pkey_prefix);
    fdb_future last_key_fut{fdb_transaction_get_key(txn,
        FDB_KEYSEL_LAST_LESS_THAN(
            as_uint8(pkey_prefix_end.data()),
            int(pkey_prefix_end.size())),
        false)};

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
    check_cv(expected_cv, cv);

    auth_fut.block_and_check(interruptor);

    table_config_t table_config;
    if (!table_config_fut.block_and_deserialize(interruptor, &table_config)) {
        crash("table config not present, when id matched config version");
    }

    {
        bool inserted = table_config.sindexes.emplace(index_name, sindex_config).second;
        if (!inserted) {
            return false;
        }
    }

    // Two common situations:  (1) the table is empty, (2) the table is not empty.
    const key_view last_key_view = future_block_on_key(last_key_fut.fut, interruptor);
    const bool table_has_data = last_key_view.has_prefix(pkey_prefix);

    const fdb_shared_task_id task_id_or_nil
        = table_has_data ? new_index_create_task_id : fdb_shared_task_id{nil_uuid()};

    bool inserted = table_config.fdb_sindexes.emplace(index_name,
        sindex_metaconfig_t{new_sindex_id, task_id_or_nil}).second;
    guarantee(inserted, "table_config::fdb_sindexes is inconsistent with sindexes");

    if (table_has_data) {
        // TODO: This node should claim the job.
        fdb_node_id claiming_node_id{nil_uuid()};

        fdb_job_description desc{
            fdb_job_type::index_create_job,
            fdb_job_db_drop{},
            fdb_job_index_create{table_id, index_name, new_sindex_id},
        };

        // TODO: We could split up the read/write portion of add_fdb_job, mix with above,
        // and avoid double round-trip latency.

        fdb_job_info ignored = add_fdb_job(txn, new_index_create_task_id, claiming_node_id,
            std::move(desc), interruptor);
        (void)ignored;

        key_view pkey_only = last_key_view.without_prefix(int(pkey_prefix.size()));
        std::string upper_bound_str(as_char(pkey_only.data), size_t(pkey_only.length));
        upper_bound_str.push_back('\0');
        fdb_index_jobstate jobstate{ukey_string{""}, ukey_string{upper_bound_str}};
        transaction_set_uq_index<index_jobstate_by_task>(txn, new_index_create_task_id, jobstate);
    }

    // Table by name index unchanged.
    transaction_set_uq_index<table_config_by_id>(txn, table_id, table_config);

    cv.value++;
    transaction_set_config_version(txn, cv);
    return true;
}


void help_erase_sindex_content(
        FDBTransaction *txn,
        const namespace_id_t &table_id,
        const sindex_metaconfig_t &cfg,
        const signal_t *interruptor) {
    if (!cfg.creation_task_or_nil.value.is_nil()) {
        remove_fdb_task_and_jobs(txn, cfg.creation_task_or_nil, interruptor);
        transaction_erase_uq_index<index_jobstate_by_task>(txn, cfg.creation_task_or_nil);
    }

    transaction_clear_prefix_range(txn, rfdb::table_index_prefix(table_id, cfg.sindex_id));
}

// TODO: Users' db/table config permissions ought to get cleaned up when we drop a db or table.

bool config_cache_sindex_drop(
        FDBTransaction *txn,
        const auth::user_context_t &user_context,
        reqlfdb_config_version expected_cv,
        const database_id_t &db_id,
        const namespace_id_t &table_id,
        const std::string &index_name,
        const signal_t *interruptor) {
    auth::fdb_user_fut<auth::db_table_config_permission> auth_fut
        = user_context.transaction_require_db_and_table_config_permission(
            txn, db_id, table_id);
    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

    fdb_value_fut<table_config_t> table_config_fut
        = transaction_lookup_uq_index<table_config_by_id>(txn, table_id);

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
    check_cv(expected_cv, cv);

    auth_fut.block_and_check(interruptor);

    table_config_t table_config;
    if (!table_config_fut.block_and_deserialize(interruptor, &table_config)) {
        crash("table config not present, when id matched config version");
    }

    auto sindexes_it = table_config.sindexes.find(index_name);
    if (sindexes_it == table_config.sindexes.end()) {
        // Index simply doesn't exist.
        return false;
    }

    auto fdb_sindexes_it = table_config.fdb_sindexes.find(index_name);
    guarantee(fdb_sindexes_it != table_config.fdb_sindexes.end());  // TODO: fdb, msg, etc.

    help_erase_sindex_content(txn, table_id, fdb_sindexes_it->second, interruptor);

    table_config.sindexes.erase(sindexes_it);
    table_config.fdb_sindexes.erase(fdb_sindexes_it);

    // Table by name index unchanged.
    transaction_set_uq_index<table_config_by_id>(txn, table_id, table_config);

    cv.value++;
    transaction_set_config_version(txn, cv);
    return true;
}

table_config_t config_cache_get_table_config(
        FDBTransaction *txn,
        reqlfdb_config_version expected_cv,
        const namespace_id_t &table_id,
        const signal_t *interruptor) {
    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

    fdb_value_fut<table_config_t> table_config_fut
        = transaction_lookup_uq_index<table_config_by_id>(txn, table_id);

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
    check_cv(expected_cv, cv);

    table_config_t table_config;
    if (!table_config_fut.block_and_deserialize(interruptor, &table_config)) {
        crash("table config not present, when id matched config version");  // TODO: fdb, msg, etc.
    }
    return table_config;
}

optional<table_config_t> config_cache_get_table_config_without_cv_check(
        FDBTransaction *txn,
        const namespace_id_t &table_id,
        const signal_t *interruptor) {
    fdb_value_fut<table_config_t> table_config_fut
        = transaction_lookup_uq_index<table_config_by_id>(txn, table_id);

    optional<table_config_t> ret;
    ret.emplace();
    if (!table_config_fut.block_and_deserialize(interruptor, &ret.get())) {
        ret.reset();
    }
    return ret;
}

rename_result config_cache_sindex_rename(
        FDBTransaction *txn,
        const auth::user_context_t &user_context,
        reqlfdb_config_version expected_cv,
        const database_id_t &db_id,
        const namespace_id_t &table_id,
        const std::string &old_name,
        const std::string &new_name,
        bool overwrite,
        const signal_t *interruptor) {
    // TODO: Copy/pasted config_cache_sindex_drop.
    auth::fdb_user_fut<auth::db_table_config_permission> auth_fut
        = user_context.transaction_require_db_and_table_config_permission(
            txn, db_id, table_id);
    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

    fdb_value_fut<table_config_t> table_config_fut
        = transaction_lookup_uq_index<table_config_by_id>(txn, table_id);

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
    check_cv(expected_cv, cv);

    auth_fut.block_and_check(interruptor);

    table_config_t table_config;
    if (!table_config_fut.block_and_deserialize(interruptor, &table_config)) {
        crash("table config not present, when id matched config version");
    }

    auto sindexes_it = table_config.sindexes.find(old_name);
    if (sindexes_it == table_config.sindexes.end()) {
        // Index simply doesn't exist.
        return rename_result::old_not_found;
    }

    auto fdb_sindexes_it = table_config.fdb_sindexes.find(old_name);
    guarantee(fdb_sindexes_it != table_config.fdb_sindexes.end());  // TODO: fdb, msg, etc.

    if (old_name == new_name) {
        // Avoids sindex drop/overwrite logic below, but we did confirm the index
        // actually exists.
        return rename_result::success;
    }

    sindex_config_t sindex_config = std::move(sindexes_it->second);
    sindex_metaconfig_t fdb_sindex_config = std::move(fdb_sindexes_it->second);

    table_config.sindexes.erase(sindexes_it);
    table_config.fdb_sindexes.erase(fdb_sindexes_it);

    if (overwrite) {
        if (table_config.sindexes.erase(new_name) == 1) {
            // We have to delete the overwritten sindex job if it exists, and its content.
            auto it = table_config.fdb_sindexes.find(new_name);
            guarantee(it != table_config.fdb_sindexes.end()); // TODO: fdb, msg, etc.
            sindex_metaconfig_t removed_metaconfig = it->second;

            help_erase_sindex_content(txn, table_id, removed_metaconfig, interruptor);

            table_config.fdb_sindexes.erase(it);
        }
    }

    bool inserted = table_config.sindexes.emplace(new_name, std::move(sindex_config)).second;
    if (!inserted) {
        return rename_result::new_already_exists;
    }
    inserted = table_config.fdb_sindexes.emplace(new_name, std::move(fdb_sindex_config)).second;
    guarantee(inserted);  // TODO: fdb, msg, etc.

    // Table by name index unchanged.
    transaction_set_uq_index<table_config_by_id>(txn, table_id, table_config);

    cv.value++;
    transaction_set_config_version(txn, cv);
    return rename_result::success;
}

// Returns if the write hook config previously existed.
bool config_cache_set_write_hook(
        FDBTransaction *txn,
        const auth::user_context_t &user_context,
        reqlfdb_config_version expected_cv,
        const database_id_t &db_id,
        const namespace_id_t &table_id,
        const optional<write_hook_config_t> &new_write_hook_config,
        const signal_t *interruptor) {
    auth::fdb_user_fut<auth::db_table_config_permission> auth_fut
        = user_context.transaction_require_db_and_table_config_permission(
            txn, db_id, table_id);

    table_config_t cfg = config_cache_get_table_config(txn, expected_cv, table_id,
        interruptor);
    // We checked expected_cv in config_cache_get_table_config.
    reqlfdb_config_version cv = expected_cv;

    auth_fut.block_and_check(interruptor);

    bool old_existed = cfg.write_hook.has_value();

    cfg.write_hook = new_write_hook_config;
    transaction_set_uq_index<table_config_by_id>(txn, table_id, cfg);

    cv.value++;
    transaction_set_config_version(txn, cv);

    return old_existed;
}

void config_cache_cv_check(
        FDBTransaction *txn,
        reqlfdb_config_version expected_cv,
        const signal_t *interruptor) {
    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

    reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
    check_cv(expected_cv, cv);
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

void transaction_set_user(
        FDBTransaction *txn,
        const auth::username_t &username,
        const auth::user_t &user) {
    ukey_string pkey = username_pkey(username);
    std::string value = serialize_for_cluster_to_string(user);
    transaction_set_pkey_index(txn, REQLFDB_USERS_BY_USERNAME, pkey, value);
}
