#ifndef RETHINKDB_FDB_XTORE_HPP_
#define RETHINKDB_FDB_XTORE_HPP_

// TODO: Rename this file to store.hpp once the other store.hpp's are gone.

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
        const signal_t *interruptor) THROWS_ONLY(
            interrupted_exc_t, cannot_perform_query_exc_t,
            provisional_assumption_exception);

read_response_t apply_read(FDBDatabase *fdb,
    rdb_context_t *ctx,
    reqlfdb_config_version prior_cv,
    const auth::user_context_t &user_context,
    const namespace_id_t &table_id,
    const table_config_t &table_config,
    const read_t &read,
    const signal_t *interruptor);

write_response_t apply_write(
    FDBDatabase *fdb,
    reqlfdb_config_version prior_cv,
    const auth::user_context_t &user_context,
    const namespace_id_t &table_id,
    const table_config_t &table_config,
    const write_t &w,
    const signal_t *interruptor);

bool needs_config_permission(const write_t &write);





#endif  // RETHINKDB_FDB_XTORE_HPP_
