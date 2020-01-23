#ifndef RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_FUNCTIONS_HPP_
#define RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_FUNCTIONS_HPP_

#include "buffer_cache/types.hpp"
#include "containers/optional.hpp"
#include "containers/uuid.hpp"
#include "fdb/reql_fdb.hpp"
#include "fdb/typed.hpp"
#include "rdb_protocol/reqlfdb_config_cache.hpp"

// TODO: Move this into reqlfdb_config_cache.hpp

// These functions are declared here because reqlfdb_config_cache is used by context.hpp
// which triggers a big rebuild when they change.

// Implementations are in reqlfdb_config_cache.cc.

class table_generate_config_params_t;

class config_version_check_later {
public:
    // UINT64_MAX means no future, no check
    reqlfdb_config_version expected_config_version = {UINT64_MAX};
    fdb_value_fut<reqlfdb_config_version> config_version_future;
};

// Carries config information, but possibly also an fdb_future for the
// config_version that you need to block on and check later.
template <class T>
class config_info {
public:
    T value;
    config_version_check_later check_later;
};

config_info<optional<database_id_t>>
config_cache_db_by_name(
    reqlfdb_config_cache *cache, FDBTransaction *txn,
    const name_string_t &db_name, const signal_t *interruptor);

MUST_USE bool config_cache_db_create(
    FDBTransaction *txn,
    const name_string_t &db_name, const signal_t *interruptor);

MUST_USE bool config_cache_table_create(
    FDBTransaction *txn,
    const table_config_t &config,
    const signal_t *interruptor);

// TODO: Remove this, push table_config_t construction to caller(s).
MUST_USE bool outer_config_cache_table_create(
    FDBTransaction *txn,
    const uuid_u &db_id,
    const name_string_t &table_name,
    const table_generate_config_params_t &config_params,
    const std::string &primary_key,
    write_durability_t durability,
    const signal_t *interruptor);

fdb_future transaction_get_table_range(
    FDBTransaction *txn, const database_id_t db_id,
    const std::string &min_table_name,
    FDBStreamingMode streaming_mode);

std::string unserialize_table_by_name_table_name(key_view key, database_id_t db_id);

// Doesn't update config version!
MUST_USE bool help_remove_table_if_exists(
    FDBTransaction *txn,
    database_id_t db_id,
    const std::string &table_name,
    const signal_t *interruptor);

bool config_cache_db_drop(
    FDBTransaction *txn, const name_string_t &db_name, const signal_t *interruptor);


#endif  // RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_FUNCTIONS_HPP_
