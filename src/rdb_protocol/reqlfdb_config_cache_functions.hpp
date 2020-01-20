#ifndef RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_FUNCTIONS_HPP_
#define RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_FUNCTIONS_HPP_

#include "containers/optional.hpp"
#include "containers/uuid.hpp"
#include "fdb/reql_fdb.hpp"
#include "rdb_protocol/reqlfdb_config_cache.hpp"

// TODO: Move this into reqlfdb_config_cache.hpp

// These functions are declared here because reqlfdb_config_cache is used by context.hpp
// which triggers a big rebuild when they change.

// Implementations are in reqlfdb_config_cache.cc.


class config_version_check_later {
public:
    // UINT64_MAX means no future, no check
    reqlfdb_config_version expected_config_version = {UINT64_MAX};
    fdb_future config_version_future;
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

#endif  // RETHINKDB_RDB_PROTOCOL_REQLFDB_CONFIG_CACHE_FUNCTIONS_HPP_
