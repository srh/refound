#ifndef RETHINKDB_FDB_XTORE_READ_HPP_
#define RETHINKDB_FDB_XTORE_READ_HPP_

#include "fdb/xtore.hpp"

#include "clustering/auth/user_context.hpp"
#include "clustering/auth/user_fut.hpp"
#include "clustering/tables/table_metadata.hpp"
#include "fdb/retry_loop.hpp"
#include "rdb_protocol/reqlfdb_config_cache.hpp"


struct cv_auth_check_fut_read {
    cv_auth_check_fut_read(FDBTransaction *txn, reqlfdb_config_version prior_cv, const auth::user_context_t &user_context,
            const namespace_id_t &table_id, const table_config_t &table_config)
        : cvc(txn, prior_cv),
          auth_fut(user_context.transaction_require_read_permission(txn, table_config.basic.database, table_id)) {}

    cv_check_fut cvc;
    auth::fdb_user_fut<auth::read_permission> auth_fut;
};

template <class return_type, class Callable>
return_type perform_read_operation(FDBDatabase *fdb, const signal_t *interruptor, reqlfdb_config_version prior_cv,
        const auth::user_context_t &user_context, const namespace_id_t &table_id, const table_config_t &table_config,
        Callable &&c) {
    return_type ret;
    try {
        fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor,
                [&](FDBTransaction *txn) {

            // QQQ: Make auth check happen (and abort) as soon as future is ready (but after
            // we check_cv?), not after entire read op.  In each callee, now, of course.
            cv_auth_check_fut_read cva(txn, prior_cv, user_context, table_id, table_config);

            ret = c(interruptor, txn, std::move(cva));
        });
        rcheck_fdb_datum(loop_err, "reading table");
    } catch (const provisional_assumption_exception &exc) {
        throw config_version_exc_t();
    }
    return ret;
}

#endif  // RETHINKDB_FDB_XTORE_READ_HPP_
