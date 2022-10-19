#ifndef RETHINKDB_FDB_REQL_FDB_HPP_
#define RETHINKDB_FDB_REQL_FDB_HPP_

#include "config/args.hpp"
#include "errors.hpp"
#include "fdb/fdb.hpp"
#include "logger.hpp"
#include "rpc/serialize_macros.hpp"
#include "rpc/equality_macros.hpp"

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
            logERR("%s failed: %s\n", (fn), msg); \
            abort(); \
        } \
    } while (false)

// This is for code that should be refactored to handle the error (later).
// TODO: Remove this, of course.
// QQQ: Hook the indeterminate case up to fail with base_exc_t::OP_INDETERMINATE.
#define guarantee_fdb_TODO(err, msg) guarantee_fdb((err), (msg))

inline bool op_indeterminate(fdb_error_t err) {
    return fdb_error_predicate(FDB_ERROR_PREDICATE_MAYBE_COMMITTED, err);
}

#define rcheck_fdb(err, where) do { \
        fdb_error_t reql_fdb_rfail_fdb_err_val = (err); \
        if (reql_fdb_rfail_fdb_err_val != 0) { \
            const char *msg = fdb_get_error(reql_fdb_rfail_fdb_err_val); \
            rfail(op_indeterminate(reql_fdb_rfail_fdb_err_val) ?  \
                ql::base_exc_t::OP_INDETERMINATE : ql::base_exc_t::OP_FAILED, \
                "FoundationDB error in %s: %s", (where), msg); \
        } \
    } while (false)

#define rcheck_fdb_datum(err, where) do { \
        fdb_error_t reql_fdb_rfail_fdb_err_val = (err); \
        if (reql_fdb_rfail_fdb_err_val != 0) { \
            const char *msg = fdb_get_error(reql_fdb_rfail_fdb_err_val); \
            rfail_datum(op_indeterminate(reql_fdb_rfail_fdb_err_val) ?  \
                ql::base_exc_t::OP_INDETERMINATE : ql::base_exc_t::OP_FAILED, \
                "FoundationDB error in %s: %s", (where), msg); \
        } \
    } while (false)

#define rcheck_fdb_src(bt, err, where) do {             \
        fdb_error_t reql_fdb_rfail_fdb_err_val = (err); \
        if (reql_fdb_rfail_fdb_err_val != 0) { \
            const char *msg = fdb_get_error(reql_fdb_rfail_fdb_err_val); \
            rfail_src(bt, op_indeterminate(reql_fdb_rfail_fdb_err_val) ? \
                ql::base_exc_t::OP_INDETERMINATE : ql::base_exc_t::OP_FAILED, \
                "FoundationDB error in %s: %s", (where), msg); \
        } \
    } while (false)

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
        reset();
    }

    bool empty() const {
        return fut == nullptr;
    }

    bool has() const {
        return !empty();
    }

    // Beware that data pointers (FDBKeyValue's, const uint8*'s, and such) will be
    // invalidated if you reset().
    void reset() {
        if (fut != nullptr) {
            fdb_future_destroy(fut);
        }
        fut = nullptr;
    }

    fdb_future(fdb_future &&movee) noexcept : fut(movee.fut) {
        movee.fut = nullptr;
    }
    fdb_future &operator=(fdb_future &&movee) noexcept {
        fdb_future tmp{std::move(movee)};
        std::swap(fut, tmp.fut);
        return *this;
    }

    // TODO: Pass back the error, fdb, graceful?
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
    explicit fdb_database(const char *cluster_file_param) : db(nullptr) {
        // nullptr or "" as the cluster file path means the default cluster file or an
        // environment variable or the current directory cluster file gets used.
        fdb_error_t err = fdb_create_database(cluster_file_param, &db);
        if (err != 0) {
            const char *msg = fdb_get_error(err);
            logERR("fdb_create_database failed: %s\n", msg);
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
            logERR("fdb_database_create_transaction failed: %s", msg);
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

// Used to hold _our_ approximation that certain txn operations add to the size.
struct approx_txn_size {
    size_t value = 0;
};

fdb_future transaction_get_std_str(FDBTransaction *txn, const std::string &key, bool snapshot);
fdb_future transaction_get_std_str(FDBTransaction *txn, const std::string &key);
void transaction_clear_std_str(FDBTransaction *txn, const std::string &key);
void transaction_set_std_str(FDBTransaction *txn, const std::string &key, const std::string &vaue);
void transaction_set_buf(FDBTransaction *txn, const std::string &key,
        const char *value, size_t value_length);
approx_txn_size transaction_clear_prefix_range(FDBTransaction *txn, const std::string &prefix);


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

    std::string to_string() const {
        return std::string(reinterpret_cast<const char *>(data), size_t(length));
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

// TODO: Return a string_view or something.

// REQLFDB_VERSION_KEY is guaranteed to be the smallest key that appears in a reqlfdb
// database.
constexpr const char *REQLFDB_VERSION_KEY = "";
#define REQLFDB_VERSION_STRING "0.2.0"
constexpr const char *REQLFDB_VERSION_VALUE_PREFIX = "ReFound " REQLFDB_VERSION_STRING " ";
// This is the version value prefix _all_ reql-on-fdb clusters have (i.e. without the version number)
constexpr const char *REQLFDB_VERSION_VALUE_UNIVERSAL_PREFIX = "ReFound ";
constexpr const char *REQLFDB_CLOCK_KEY = "rethinkdb/clock";

constexpr const char *REQLFDB_NODES_BY_ID = "rethinkdb/nodes//";
constexpr const char *REQLFDB_NODES_BY_LEASE_EXPIRATION = "rethinkdb/nodes/by_lease_expiration/";
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
constexpr const char *REQLFDB_USERS_BY_IDS = "rethinkdb/users/ids/";

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

struct node_info {
    reqlfdb_clock lease_expiration;
};
RDB_DECLARE_SERIALIZABLE(node_info);

bool is_node_expired(reqlfdb_clock current_clock, reqlfdb_clock node_lease_expiration);
bool is_node_expired(reqlfdb_clock current_clock, const node_info& info);

// TODO: Calculate this rationally using constants like TIMESTEP_MS and how fdb
// transaction duration is configured.
constexpr int REQLFDB_JOB_LEASE_DURATION = 10;
constexpr uint64_t REQLFDB_CONNECTIVITY_COMPLAINT_TIMEOUT_MS = 3000;

constexpr uint64_t REQLFDB_TIMESTEP_MS = 5000;

// We now use fdb_error_predicate instead of using commit_unknown_result directly.
constexpr int REQLFDB_commit_unknown_result = 1021;
// This is retryable.  Maybe transaction_too_old is better?  Idk.
constexpr int REQLFDB_not_committed = 1020;

// FoundationDB txn limit is 10 MB, and they recommend keeping below more like 1 MB for
// general transactions.
constexpr size_t REQLFDB_MAX_LARGE_VALUE_SIZE = 9 * MEGABYTE;
#define REQLFDB_MAX_LARGE_VALUE_SIZE_STR "9,437,184 bytes"
constexpr size_t REQLFDB_WRITE_TXN_SOFT_LIMIT = 1 * MEGABYTE;

constexpr bool REQLFDB_KEY_PREFIX_NOT_IMPLEMENTED = true;

#endif  // RETHINKDB_FDB_REQL_FDB_HPP_
