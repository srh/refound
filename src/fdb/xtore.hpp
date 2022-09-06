#ifndef RETHINKDB_FDB_XTORE_HPP_
#define RETHINKDB_FDB_XTORE_HPP_

// TODO: Rename this file to store.hpp once the other store.hpp's are gone.n

#include "errors.hpp"
#include "fdb/fdb.hpp"

namespace auth {
class user_context_t;
}
class cv_check_fut;
struct reqlfdb_config_version;
class interrupted_exc_t;
struct namespace_id_t;
enum class profile_bool_t;
class rdb_context_t;
class signal_t;
struct store_key_t;
class table_config_t;
struct read_t;
struct read_response_t;
struct write_t;
struct write_response_t;

read_response_t apply_point_read(FDBTransaction *txn,
        cv_check_fut &&cvc,
        const namespace_id_t &table_id,
        const store_key_t &pkey,
        const profile_bool_t profile,
        const signal_t *interruptor);

read_response_t apply_read(FDBTransaction *txn,
    rdb_context_t *ctx,
    reqlfdb_config_version prior_cv,
    const auth::user_context_t &user_context,
    const namespace_id_t &table_id,
    const table_config_t &table_config,
    const read_t &read,
    const signal_t *interruptor);

write_response_t apply_write(FDBTransaction *txn,
    cv_check_fut &&cvc,
    const namespace_id_t &table_id,
    const table_config_t &table_config,
    const write_t &write,
    const signal_t *interruptor);

bool needs_config_permission(const write_t &write);





#endif  // RETHINKDB_FDB_XTORE_HPP_
