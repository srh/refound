#ifndef RETHINKDB_REQL_FDB_HPP_
#define RETHINKDB_REQL_FDB_HPP_

#include "errors.hpp"
#include "fdb.hpp"

/* The general idea here is these types exist for RAII.  Users should still pass
parameters `FDBDatabase *` and `FDBTransaction *` to anything that doesn't take
ownership. */

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

    FDBFuture *fut;
    DISABLE_COPYING(fdb_future);
};


#endif  // RETHINKDB_FDB_HPP_
