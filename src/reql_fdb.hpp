#ifndef RETHINKDB_REQL_FDB_HPP_
#define RETHINKDB_REQL_FDB_HPP_

#include "errors.hpp"
#include "fdb.hpp"

/* The general idea here is these types exist for RAII.  Users should still pass
parameters `FDBDatabase *` and `FDBTransaction *` to anything that doesn't take
ownership. */

// This is only to be used for errors that "should not" happen.
// TODO: Do whatever guarantee() actually does.
#define guarantee_fdb(err, fn) do { \
        fdb_error_t reql_fdb_guarantee_err_var = (err); \
        if (reql_fdb_guarantee_err_var != 0) { \
            const char *msg = fdb_get_error(reql_fdb_guarantee_err_var); \
            printf("ERROR: %s failed: %s", (fn), msg); \
            abort(); \
        } \
    } while (false)

// This is for code that should be refactored to handle the error (later).
// TODO: Remove this, of course.
#define guarantee_fdb_TODO(err, msg) guarantee_fdb((err), (msg))


struct fdb_database {
    fdb_database() : db(nullptr) {
        // nullptr as the cluster file path means the default cluster file gets used.
        fdb_error_t err = fdb_create_database(nullptr, &db);
        if (err != 0) {
            const char *msg = fdb_get_error(err);
            printf("ERROR: fdb_create_database failed: %s", msg);
            abort();  // TODO: abort?
        }
    }
    ~fdb_database() {
        fdb_database_destroy(db);
    }

    FDBDatabase *db;

    DISABLE_COPYING(fdb_database);
};

struct fdb_transaction {
    explicit fdb_transaction(FDBDatabase *db) : txn(nullptr) {
        fdb_error_t err = fdb_database_create_transaction(db, &txn);
        if (err != 0) {
            const char *msg = fdb_get_error(err);
            printf("ERROR: fdb_database_create_transaction failed: %s", msg);
            abort();  // TODO: abort?
        }
    }

    ~fdb_transaction() {
        fdb_transaction_destroy(txn);
    }

    FDBTransaction *txn;
    DISABLE_COPYING(fdb_transaction);
};

struct fdb_future {
    fdb_future() : fut(nullptr) {}
    explicit fdb_future(FDBFuture *_fut) : fut(_fut) {}
    ~fdb_future() {
        if (fut != nullptr) {
            fdb_future_destroy(fut);
        }
    }

    fdb_future(fdb_future &&movee) noexcept : fut(movee.fut) {
        movee.fut = nullptr;
    }
    fdb_future &operator=(fdb_future &&movee) noexcept {
        fdb_future tmp{std::move(movee)};
        std::swap(fut, tmp.fut);
        return *this;
    }

    void block_pthread() const {
        fdb_error_t err = fdb_future_block_until_ready(fut);
        guarantee_fdb(err, "fdb_future_block_until_ready");
    }

    FDBFuture *fut;
    DISABLE_COPYING(fdb_future);
};

// TODO: Return a string_view or something.

// REQLFDB_VERSION_KEY is guaranteed to be the smallest key that appears in a reqlfdb
// database.
inline const char *REQLFDB_VERSION_KEY() { return ""; }
inline const char *REQLFDB_VERSION_VALUE() { return "reqlfdb 0.1"; }


#endif  // RETHINKDB_FDB_HPP_
