#ifndef RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_FUNCTIONS_HPP_
#define RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_FUNCTIONS_HPP_

#include "buffer_cache/types.hpp"
#include "containers/counted.hpp"
#include "containers/optional.hpp"
#include "containers/uuid.hpp"
#include "fdb/reql_fdb.hpp"
#include "fdb/typed.hpp"
#include "rdb_protocol/reqlfdb_config_cache.hpp"

namespace ql {
class db_t;
}

// TODO: Move this into reqlfdb_config_cache.hpp

// These functions are declared here because reqlfdb_config_cache is used by context.hpp
// which triggers a big rebuild when they change.

// Implementations are in reqlfdb_config_cache.cc.

config_info<optional<database_id_t>>
config_cache_retrieve_db_by_name(
    const reqlfdb_config_cache *cc, FDBTransaction *txn,
    const name_string_t &db_name, const signal_t *interruptor);

config_info<optional<std::pair<namespace_id_t, table_config_t>>>
config_cache_retrieve_table_by_name(
    const reqlfdb_config_cache *cc, FDBTransaction *txn,
    const std::pair<database_id_t, name_string_t> &db_table_name,
    const signal_t *interruptor);

config_info<optional<auth::user_t>>
config_cache_retrieve_user_by_name(
    const reqlfdb_config_cache *cc, FDBTransaction *txn,
    const auth::username_t &username, const signal_t *interruptor);

MUST_USE bool config_cache_db_create(
    FDBTransaction *txn,
    const auth::user_context_t &user_context,
    const name_string_t &db_name,
    const database_id_t &new_db_id,
    const signal_t *interruptor);

MUST_USE bool config_cache_table_create(
    FDBTransaction *txn,
    reqlfdb_config_version expected_cv,
    const auth::user_context_t &user_context,
    const namespace_id_t &new_table_id,
    const table_config_t &config,
    const signal_t *interruptor);

fdb_future transaction_get_table_range(
    FDBTransaction *txn, const database_id_t db_id,
    const std::string &lower_bound_table_name, bool closed,
    FDBStreamingMode streaming_mode);

std::string unserialize_table_by_name_table_name(key_view key, database_id_t db_id);

// Doesn't update config version!
MUST_USE bool help_remove_table_if_exists(
    FDBTransaction *txn,
    database_id_t db_id,
    const std::string &table_name,
    const signal_t *interruptor);

MUST_USE optional<std::pair<namespace_id_t, table_config_t>> config_cache_table_drop(
        FDBTransaction *txn,
        reqlfdb_config_version expected_cv,
        const auth::user_context_t &user_context,
        const database_id_t &db_id, const name_string_t &table_name,
        const signal_t *interruptor);

MUST_USE optional<database_id_t> config_cache_db_drop(
    FDBTransaction *txn,
    const auth::user_context_t &user_context,
    const name_string_t &db_name, const signal_t *interruptor);

// TODO: This could just return name_string_t's.
std::vector<counted_t<const ql::db_t>> config_cache_db_list(
    FDBTransaction *txn,
    const signal_t *interruptor);


std::vector<name_string_t> config_cache_table_list(
    FDBTransaction *txn,
    reqlfdb_config_version expected_cv,
    const database_id_t &db_id,
    const signal_t *interruptor);

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
    const signal_t *interruptor);

MUST_USE bool config_cache_sindex_drop(
    FDBTransaction *txn,
    const auth::user_context_t &user_context,
    reqlfdb_config_version expected_cv,
    const database_id_t &db_id,
    const namespace_id_t &table_id,
    const std::string &index_name,
    const signal_t *interruptor);

table_config_t config_cache_get_table_config(
    FDBTransaction *txn,
    reqlfdb_config_version expected_cv,
    const namespace_id_t &table_id,
    const signal_t *interruptor);

enum class rename_result {
    success,
    old_not_found,
    new_already_exists,
};

rename_result config_cache_sindex_rename(
    FDBTransaction *txn,
    const auth::user_context_t &user_context,
    reqlfdb_config_version expected_cv,
    const database_id_t &db_id,
    const namespace_id_t &table_id,
    const std::string &old_name,
    const std::string &new_name,
    bool overwrite,
    const signal_t *interruptor);

bool config_cache_set_write_hook(
        FDBTransaction *txn,
        const auth::user_context_t &user_context,
        reqlfdb_config_version expected_cv,
        const database_id_t &db_id,
        const namespace_id_t &table_id,
        const optional<write_hook_config_t> &new_write_hook_config,
        const signal_t *interruptor);

void config_cache_cv_check(
    FDBTransaction *txn,
    reqlfdb_config_version expected_cv,
    const signal_t *interruptor);

optional<table_config_t> config_cache_get_table_config_without_cv_check(
    FDBTransaction *txn,
    const namespace_id_t &table_id,
    const signal_t *interruptor);

fdb_value_fut<auth::user_t> transaction_get_user(
    FDBTransaction *txn,
    const auth::username_t &username);

void transaction_set_user(
    FDBTransaction *txn,
    const auth::username_t &username,
    const auth::user_t &user);

#endif  // RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_FUNCTIONS_HPP_
