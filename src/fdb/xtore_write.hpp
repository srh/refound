#ifndef RETHINKDB_FDB_XTORE_WRITE_HPP_
#define RETHINKDB_FDB_XTORE_WRITE_HPP_

#include "fdb/xtore.hpp"

#include "clustering/auth/user_context.hpp"
#include "clustering/auth/user_fut.hpp"
#include "clustering/tables/table_metadata.hpp"
#include "fdb/retry_loop.hpp"
#include "rdb_protocol/reqlfdb_config_cache.hpp"

struct cv_auth_check_fut_write {
    cv_auth_check_fut_write(FDBTransaction *txn, reqlfdb_config_version prior_cv, const auth::user_context_t &user_context,
            const namespace_id_t &table_id, const table_config_t &table_config, bool needed_config_permission)
        : cvc(txn, prior_cv),
          auth_fut(user_context.transaction_require_write_permission(txn, table_config.basic.database, table_id)),
          conf_fut(needed_config_permission
                  ? user_context.transaction_require_db_and_table_config_permission(txn, table_config.basic.database, table_id)
                  : auth::fdb_user_fut<auth::db_table_config_permission>::success()) {}

    cv_check_fut cvc;
    auth::fdb_user_fut<auth::write_permission> auth_fut;
    auth::fdb_user_fut<auth::db_table_config_permission> conf_fut;

    void block_and_check_auths(const signal_t *interruptor) THROWS_ONLY(permission_error_t, interrupted_exc_t) {
        auth_fut.block_and_check(interruptor);
        conf_fut.block_and_check(interruptor);
    }
};

template <class return_type, class Callable>
return_type perform_write_operation(FDBDatabase *fdb, const signal_t *interruptor, reqlfdb_config_version prior_cv,
        const auth::user_context_t &user_context, const namespace_id_t &table_id, const table_config_t &table_config,
        bool needed_config_permission, Callable &&c) {
    return_type ret;
    try {
        fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor,
                [&](FDBTransaction *txn) {

            // QQQ: Make auth check happen (and abort) as soon as future is ready (but after
            // we check_cv?), not after entire write op.
            cv_auth_check_fut_write cva(txn, prior_cv, user_context, table_id, table_config, needed_config_permission);

            return_type response = c(interruptor, txn, std::move(cva));

            ret = std::move(response);
        });
        // NNN: Make sure this exception ends up in the write response.
        rcheck_fdb_datum(loop_err, "writing table");
    } catch (const provisional_assumption_exception &exc) {
        throw config_version_exc_t();
    }
    return ret;
}


template <class return_type, class Callable>
return_type perform_write_operation_with_counter(FDBDatabase *fdb, const signal_t *interruptor, reqlfdb_config_version prior_cv,
        const auth::user_context_t &user_context, const namespace_id_t &table_id, const table_config_t &table_config,
        bool needed_config_permission, Callable &&c) {
    return_type ret;
    try {
        fdb_error_t loop_err = txn_retry_loop_coro_with_counter(fdb, interruptor,
                [&](FDBTransaction *txn, size_t count) {

            // QQQ: Make auth check happen (and abort) as soon as future is ready (but after
            // we check_cv?), not after entire write op.
            cv_auth_check_fut_write cva(txn, prior_cv, user_context, table_id, table_config, needed_config_permission);

            return_type response = c(interruptor, txn, count, std::move(cva));

            ret = std::move(response);
        });
        // NNN: Make sure this exception ends up in the write response.
        rcheck_fdb_datum(loop_err, "writing table");
    } catch (const provisional_assumption_exception &exc) {
        throw config_version_exc_t();
    }
    return ret;
}








#endif  // RETHINKDB_FDB_XTORE_WRITE_HPP_
