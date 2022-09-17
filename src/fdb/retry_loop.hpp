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
        fdb_error_t orig_err;
        try {
            fn(txn.txn);
            return 0;
        } catch (const fdb_transaction_exception &exc) {
            // Fall out of catch block so that we're consistent with txn_retry_loop_coro
            // code (which needs to do this).
            orig_err = exc.error();
        }

        // From fdb documentation: if there is an unknown result, the txn must be an
        // idempotent operation.  This could be commit_unknown_result or
        // cluster_version_changed (as of the last version I looked at).
        if (op_indeterminate(orig_err)) {
            return orig_err;
        }

        fdb_future fut{fdb_transaction_on_error(txn.txn, orig_err)};
        // The exponential backoff strategy is what blocks the pthread.
        fut.block_pthread();
        fdb_error_t err = fdb_future_get_error(fut.fut);
        if (err != 0) {
            // This is not a guarantee, in case FDB changes behavior in future versions.
            rassert(err == orig_err);
            return err;
        }
    }
}


template <class C>
MUST_USE fdb_error_t txn_retry_loop_coro(
        FDBDatabase *fdb, const signal_t *interruptor,
        C &&fn) {
    fdb_transaction txn{fdb};
    for (;;) {
        fdb_error_t orig_err;
        try {
            fn(txn.txn);
            return 0;
        } catch (const fdb_transaction_exception &exc) {
            // Fall-through out of catch block, so that we may block the coroutine.
            orig_err = exc.error();
        }

        // From fdb documentation: if there is an unknown result, the txn must be an
        // idempotent operation.  This could be commit_unknown_result or
        // cluster_version_changed (as of the last version I looked at).
        if (op_indeterminate(orig_err)) {
            return orig_err;
        }

        fdb_future fut{fdb_transaction_on_error(txn.txn, orig_err)};
        // The exponential backoff strategy is what blocks the coro.
        fut.block_coro(interruptor);
        fdb_error_t err = fdb_future_get_error(fut.fut);
        if (err != 0) {
            // This is not a guarantee, in case FDB changes behavior in future versions.
            rassert(err == orig_err);
            return err;
        }
    }
}

// Just passes counter parameter to the retry loop.
template <class C>
MUST_USE fdb_error_t txn_retry_loop_coro_with_counter(
        FDBDatabase *fdb, const signal_t *interruptor,
        C &&fn) {
    fdb_transaction txn{fdb};
    size_t count = 0;
    for (;; ++count) {
        fdb_error_t orig_err;
        try {
            fn(txn.txn, count);
            return 0;
        } catch (const fdb_transaction_exception &exc) {
            // Fall-through out of catch block, so that we may block the coroutine.
            orig_err = exc.error();
        }

        // From fdb documentation: if there is an unknown result, the txn must be an
        // idempotent operation.  This could be commit_unknown_result or
        // cluster_version_changed (as of the last version I looked at).
        if (op_indeterminate(orig_err)) {
            return orig_err;
        }

        fdb_future fut{fdb_transaction_on_error(txn.txn, orig_err)};
        // The exponential backoff strategy is what blocks the coro.
        fut.block_coro(interruptor);
        fdb_error_t err = fdb_future_get_error(fut.fut);
        if (err != 0) {
            // This is not a guarantee, in case FDB changes behavior in future versions.
            rassert(err == orig_err);
            return err;
        }
    }
}

#endif  // RETHINKDB_FDB_RETRY_LOOP_HPP_
