#ifndef RETHINKDB_FDB_REQL_FDB_HPP_
#define RETHINKDB_FDB_REQL_FDB_HPP_

#include "concurrency/cond_var.hpp"  // TODO: Remove, unused.
#include "containers/uuid.hpp"
#include "errors.hpp"
#include "fdb/fdb.hpp"
#include "rpc/serialize_macros.hpp"
#include "rpc/semilattice/joins/macros.hpp"

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
// QQQ: Hook the indeterminate case up to fail with base_exc_t::OP_INDETERMINATE.
#define guarantee_fdb_TODO(err, msg) guarantee_fdb((err), (msg))

// These are a bit safer than naked reinterpret_casts.
inline const uint8_t *as_uint8(const char *s) {
    return reinterpret_cast<const uint8_t *>(s);
}

inline const uint8_t *void_as_uint8(const void *s) {
    return static_cast<const uint8_t *>(s);
}

inline const char *void_as_char(const void *s) {
    return static_cast<const char *>(s);
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

    bool empty() const {
        return fut == nullptr;
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

inline fdb_future transaction_get_c_str(FDBTransaction *txn, const char *key) {
    return fdb_future{fdb_transaction_get(
        txn,
        as_uint8(key),
        strlen(key),
        false)};
}

fdb_future transaction_get_std_str(FDBTransaction *txn, const std::string &key);
void transaction_clear_std_str(FDBTransaction *txn, const std::string &key);
void transaction_set_std_str(FDBTransaction *txn, const std::string &key, const std::string &vaue);

// TODO: Rename to "maybe_value_view" or "fdb_view" or something
struct fdb_value {
    fdb_bool_t present;
    const uint8_t *data;
    int length;
};

struct key_view {
    const uint8_t *data;
    int length;

    MUST_USE bool has_prefix(const std::string &prefix) const {
        return int(prefix.size()) <= length &&
            0 == memcmp(prefix.data(), data, prefix.size());
    }

    key_view without_prefix(int prefix_length) const {
        guarantee(prefix_length <= length);  // TODO: fail msg
        return key_view{data + prefix_length, length - prefix_length};
    }
    // TODO: Remove most usages/demote to rassert after some testing.
    // Checks prefix matches.
    key_view guarantee_without_prefix(const std::string &prefix) const {
        // TODO: fail msg
        guarantee(int(prefix.size()) <= length);
        guarantee(0 == memcmp(prefix.data(), data, int(prefix.size())));
        return key_view{data + prefix.size(), length - int(prefix.size())};
    }
};

MUST_USE inline fdb_error_t future_get_value(FDBFuture *fut, fdb_value *out) {
    return fdb_future_get_value(fut, &out->present, &out->data, &out->length);
}

// Throws fdb_transaction_exception.
fdb_value future_block_on_value(FDBFuture *fut, const signal_t *interruptor);

MUST_USE inline fdb_error_t future_get_key(FDBFuture *fut, key_view *out) {
    return fdb_future_get_key(fut, &out->data, &out->length);
}

// Throws fdb_transaction_exception.
key_view future_block_on_key(FDBFuture *fut, const signal_t *interruptor);

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

constexpr const char *REQLFDB_NODES_BY_ID = "rethinkdb/nodes//";
constexpr const char *REQLFDB_NODES_COUNT_KEY = "rethinkdb/nodes_count";

constexpr const char *REQLFDB_DB_CONFIG_TABLE = "rethinkdb/db_config/";
constexpr const char *REQLFDB_DB_CONFIG_BY_ID = "rethinkdb/db_config//";
// indexed uniquely by name.
constexpr const char *REQLFDB_DB_CONFIG_BY_NAME = "rethinkdb/db_config/by_name/";

constexpr const char *REQLFDB_TABLE_CONFIG_TABLE = "rethinkdb/table_config/";
constexpr const char *REQLFDB_TABLE_CONFIG_BY_ID = "rethinkdb/table_config//";
// indexed uniquely by {db_id,name}
constexpr const char *REQLFDB_TABLE_CONFIG_BY_NAME = "rethinkdb/table_config/by_name/";

constexpr const char *REQLFDB_INDEX_JOBSTATE_BY_TASK = "rethinkdb/index_jobstate//";

constexpr const char *REQLFDB_JOBS_TABLE = "rethinkdb/jobs/";
constexpr const char *REQLFDB_JOBS_BY_ID = "rethinkdb/jobs//";
constexpr const char *REQLFDB_JOBS_BY_LEASE_EXPIRATION = "rethinkdb/jobs/by_expiration/";
constexpr const char *REQLFDB_JOBS_BY_TASK = "rethinkdb/jobs/by_task/";

constexpr const char *REQLFDB_USERS_BY_USERNAME = "rethinkdb/users//";

constexpr const char *REQLFDB_CONFIG_VERSION_KEY = "rethinkdb/config_version";

constexpr size_t REQLFDB_CLOCK_SIZE = 8;
struct reqlfdb_clock {
    uint64_t value;
};
// reqlfdb_clock must be serialized in little-endian order.
RDB_DECLARE_SERIALIZABLE(reqlfdb_clock);
RDB_MAKE_EQUALITY_COMPARABLE_1(reqlfdb_clock, value);

constexpr size_t REQLFDB_NODES_COUNT_SIZE = 8;
constexpr size_t REQLFDB_CONFIG_VERSION_COUNT_SIZE = 8;

constexpr uint64_t REQLFDB_NODE_LEASE_DURATION = 10;

// TODO: Calculate this rationally using constants like TIMESTEP_MS and how fdb
// transaction duration is configured.
constexpr const int REQLFDB_JOB_LEASE_DURATION = 10;

constexpr uint64_t REQLFDB_TIMESTEP_MS = 5000;

// from fdb documentation
constexpr int REQLFDB_commit_unknown_result = 1021;
// This is retryable.  Maybe transaction_too_old is better?  Idk.
constexpr int REQLFDB_not_committed = 1020;

struct fdb_node_id {
    uuid_u value;
};
RDB_DECLARE_SERIALIZABLE(fdb_node_id);
RDB_MAKE_EQUALITY_COMPARABLE_1(fdb_node_id, value);

// TODO: Is this used?
inline std::string REQLFDB_TABLE_CONFIG(namespace_id_t table_id) {
    std::string ret;
    ret.reserve(200);  // TODO: how much?
    ret += "rethinkdb/table_config/";
    uuid_onto_str(table_id.value, &ret);
    ret += "/config";
    return ret;
}

#endif  // RETHINKDB_FDB_REQL_FDB_HPP_
