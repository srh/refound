#ifndef RETHINKDB_FDB_RETRY_LOOP_HPP_
#define RETHINKDB_FDB_RETRY_LOOP_HPP_

#include "fdb/reql_fdb.hpp"


// You'll want to use txn_retry_loop_coro.
template <class C>
MUST_USE fdb_error_t txn_retry_loop_pthread(
        FDBDatabase *fdb,
        C &&fn) {
    fdb_transaction txn{fdb};
    for (;;) {
        try {
            fn(txn.txn);
            return 0;
        } catch (const fdb_transaction_exception &exc) {
            fdb_error_t orig_err = exc.error();
            if (orig_err == REQLFDB_commit_unknown_result) {
                // From fdb documentation:  if there is an unknown result,
                // the txn must be an idempotent operation.
                return orig_err;
            }

            fdb_future fut{fdb_transaction_on_error(txn.txn, orig_err)};
            // The exponential backoff strategy is what blocks the pthread.
            fut.block_pthread();
            fdb_error_t err = fdb_future_get_error(fut.fut);
            if (err != 0) {
                // TODO: Remove this guarantee.
                guarantee(err == exc.error());
                return err;
            }
        }
    }
}


template <class C>
MUST_USE fdb_error_t txn_retry_loop_coro(
        FDBDatabase *fdb, const signal_t *interruptor,
        C &&fn) {
    fdb_transaction txn{fdb};
    for (;;) {
        try {
            fn(txn.txn);
            return 0;
        } catch (const fdb_transaction_exception &exc) {
            fdb_error_t orig_err = exc.error();
            if (orig_err == REQLFDB_commit_unknown_result) {
                // From fdb documentation:  if there is an unknown result,
                // the txn must be an idempotent operation.
                return orig_err;
            }

            fdb_future fut{fdb_transaction_on_error(txn.txn, orig_err)};
            // The exponential backoff strategy is what blocks the coro.
            fut.block_coro(interruptor);
            fdb_error_t err = fdb_future_get_error(fut.fut);
            if (err != 0) {
                // TODO: Remove this guarantee.
                guarantee(err == exc.error());
                return err;
            }
        }
    }
}

#endif  // RETHINKDB_FDB_RETRY_LOOP_HPP_
