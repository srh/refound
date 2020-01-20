#ifndef RETHINKDB_FDB_REQL_FDB_HPP_
#define RETHINKDB_FDB_REQL_FDB_HPP_

#include "concurrency/cond_var.hpp"  // TODO: Remove, unused.
#include "containers/uuid.hpp"
#include "errors.hpp"
#include "fdb/fdb.hpp"

class signal_t;

/* The general idea here is these types exist for RAII.  Users should still pass
parameters `FDBDatabase *` and `FDBTransaction *` to anything that doesn't take
ownership. */

// This is only to be used for errors that "should not" happen.
// TODO: Do whatever guarantee() actually does.
#define guarantee_fdb(err, fn) do { \
        fdb_error_t reql_fdb_guarantee_err_var = (err); \
        if (reql_fdb_guarantee_err_var != 0) { \
            const char *msg = fdb_get_error(reql_fdb_guarantee_err_var); \
            printf("ERROR: %s failed: %s\n", (fn), msg); \
            abort(); \
        } \
    } while (false)

// This is for code that should be refactored to handle the error (later).
// TODO: Remove this, of course.
#define guarantee_fdb_TODO(err, msg) guarantee_fdb((err), (msg))

// These are a bit safer than naked reinterpret_casts.
inline const uint8_t *as_uint8(const char *s) {
    return reinterpret_cast<const uint8_t *>(s);
}

inline const char *as_char(const uint8_t *s) {
    return reinterpret_cast<const char *>(s);
}

void future_block_coro(FDBFuture *fut, const signal_t *interruptor);

class fdb_future {
public:
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

    void block_coro(const signal_t *interruptor) const {
        future_block_coro(fut, interruptor);
    }

    FDBFuture *fut;
    DISABLE_COPYING(fdb_future);
};


class fdb_database {
public:
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

class fdb_transaction_exception : public std::exception {
public:
    explicit fdb_transaction_exception(fdb_error_t err)
        : err_(err) {}

    const char *what() const noexcept override {
        return fdb_get_error(err_);
    }

    fdb_error_t error() const { return err_; }

private:
    fdb_error_t err_;
};

// TODO: Check every fdb_transaction func result.
// Throws fdb_transaction_exception.
inline void check_for_fdb_transaction(fdb_error_t err) {
    if (err != 0) {
        throw fdb_transaction_exception(err);
    }
}

class fdb_transaction {
public:
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

constexpr int REQLFDB_commit_unknown_result = 1021;  // from fdb documentation

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
            // The exponential backoff strategy is what blocks the ~coro~ pthread.
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

inline fdb_future transaction_get_c_str(FDBTransaction *txn, const char *key) {
    return fdb_future{fdb_transaction_get(
        txn,
        as_uint8(key),
        strlen(key),
        false)};
}

fdb_future transaction_get_std_str(FDBTransaction *txn, const std::string &key);

struct fdb_value {
    fdb_bool_t present;
    const uint8_t *value;
    int length;
};

MUST_USE inline fdb_error_t future_get_value(FDBFuture *fut, fdb_value *out) {
    return fdb_future_get_value(fut, &out->present, &out->value, &out->length);
}

// Throws fdb_transaction_exception.
fdb_value future_block_on_value(FDBFuture *fut, const signal_t *interruptor);

// TODO: Remove (except for commit) and make callers use retry loop.
MUST_USE fdb_error_t commit_fdb_block_coro(
    FDBTransaction *txn, const signal_t *interruptor);

// Throws fdb_transaction_exception.
inline void commit(FDBTransaction *txn, const signal_t *interruptor) {
    fdb_error_t err = commit_fdb_block_coro(txn, interruptor);
    check_for_fdb_transaction(err);
}

// TODO: Make callers use a commit/retry loop and remove this.
inline void commit_TODO_retry(FDBTransaction *txn) {
    cond_t non_interruptor;
    fdb_error_t commit_err = commit_fdb_block_coro(txn, &non_interruptor);
    guarantee_fdb_TODO(commit_err, "db_create commit failed");
}

// TODO: Return a string_view or something.

// REQLFDB_VERSION_KEY is guaranteed to be the smallest key that appears in a reqlfdb
// database.
constexpr const char *REQLFDB_VERSION_KEY = "";
constexpr const char *REQLFDB_VERSION_VALUE_PREFIX = "reqlfdb 0.1.0 ";
constexpr const char *REQLFDB_CLOCK_KEY = "rethinkdb/clock";

constexpr const char *REQLFDB_NODES_TABLE = "rethinkdb/nodes/";
constexpr const char *REQLFDB_NODES_COUNT_KEY = "rethinkdb/nodes_count";

constexpr const char *REQLFDB_DB_CONFIG_TABLE = "rethinkdb/db_config/";

constexpr const char *REQLFDB_CONFIG_VERSION_KEY = "rethinkdb/config_version";

constexpr size_t REQLFDB_CLOCK_SIZE = 8;
constexpr size_t REQLFDB_NODES_COUNT_SIZE = 8;
constexpr size_t REQLFDB_CONFIG_VERSION_COUNT_SIZE = 8;

constexpr uint64_t REQLFDB_NODE_LEASE_DURATION = 10;

constexpr uint64_t REQLFDB_TIMESTEP_MS = 5000;

// TODO: Is this used?
inline std::string REQLFDB_TABLE_CONFIG(namespace_id_t table_id) {
    std::string ret;
    ret.reserve(200);  // TODO: how much?
    ret += "rethinkdb/table_config/";
    uuid_onto_str(table_id, &ret);
    ret += "/config";
    return ret;
}

#endif  // RETHINKDB_FDB_REQL_FDB_HPP_
