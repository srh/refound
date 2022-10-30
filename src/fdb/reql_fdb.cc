#include "fdb/reql_fdb.hpp"

#include <limits.h>
#include <string.h>

#include "errors.hpp"
#include <boost/detail/endian.hpp>

#include "concurrency/cond_var.hpp"
#include "concurrency/interruptor.hpp"
#include "concurrency/wait_any.hpp"
#include "containers/death_runner.hpp"
#include "do_on_thread.hpp"

/*

fdb storage format:

    "" => "ReFound 2.4.0 <uuid>" -- the reqlfdb version we're on and a random string UUID, the fdb_cluster_id.

    rethinkdb/... => rethinkdb system db

Node presence and clock configuration:

    rethinkdb/clock => monotonic integer, meanderingly increments approx. every REQLFDB_TIMESTEP
    rethinkdb/nodes_count => number of keys under rethinkdb/nodes/
    rethinkdb/nodes/ => table of node_info by server uuid
      indexed with total count in place of rethinkdb/node_count?
      indexed by lease expiration?

Jobs:

    rethinkdb/jobs/ => table of fdb_job_info by job uuid
      indexed by lease expiration timestamp (see rethinkdb/clock)
      indexed by node?

Log:

    rethinkdb/log/ => table of log entries by [node_id, counter]
      possibly indexed by timestamp
      possibly indexed by [node_id, timestamp]

Db config:

    rethinkdb/db_config/ => table of db_config_t
      indexed by name

Table config:

    rethinkdb/table_config/ => table of table_config_t
      indexed by [db_uuid,table_name] => namespace_id_t.

Index building:

    rethinkdb/index_jobstate// => table of fdb_index_jobstate by shared_task_id

User config:

    rethinkdb/users/ => table of user_t, primary key being username_t.
    Admin user is present (since we have password stuff).
      multi-indexed by uuid -- every database id or table id referenced by the user_t, with keys [uuid, username]
      -- Yes, it's gross that we overlap table and db uuid's in the same namespace in the index,
         but they're uuid's.

Config version:

    rethinkdb/config_version => monotonic integer
      Gets incremented whenever table, db, or index availability decreases (or changes?).
      Gets incremented whenever user access decreases (or changes?).

    (We don't mind querying upon rare failure case.)

Issues:

    We read outdated index issues by scanning the table config.

    We might have a place to write "cannot write log entry" issues, or generally, local
    issues.

TODO: Implement logging to FDB.
TODO: Implement issues.

New large value table format:

----
TODO: (For impling large values.)
2. Add the \x00, \x30, and \x31

----

    Given a table prefix like "tables/<uuid>/",

    We use a new primary key encoding that lets us append to it (like the secondary keys have)

    Primary kv prefix:

    tables/<uuid>//<store_key_t>\00

    (^^ <table prefix> includes a trailing slash, so we add one more slash)
    (^^ store_key_t is the primary key as a store_key_t, which already never has nul characters)

    Secondary index kv:
    <table prefix><index id>/<secondary_key_t><store_key_t> => <empty>

    Underneath the primary kv prefix (maybe out-of-date, see btree_utils.cc):

    <prefix>\x30<number of entries u16 in big-endian hex><more stuff...> => first part
    <prefix>\x31<u16 in big-endian hex><more stuff...> => remaining parts

    (^^ every part but the last has size MAX_PART_SIZE)
    (^^ just concatenate them to get the serialized value)
    (^^ MAX_PART_SIZE should be 16384 or 10000)
    (^^ the \x31-prefixed keys start with 0001, not zero (because zero is the \x30 part index).)

    // TODO: After this is working and tested, replace \x30/\x31 with \x00/\x01, remove
    // hexadecimal (from everything).  Perhaps replace \x00 with \x00\x00 for "nice"
    // extensibility, though idk how that would help.

    Count: The table count value is stored at "tables/<uuid>/_count" and is atomically
    incremented.  The underscore represents that as of some later refactoring we want some
    prefix code to distinguish us from UUID's.  This is just a handy informative value.

    Count index: TODO.  Would allow efficient .between().count() queries.

    General aggregate indexes: TODO.  They don't exist, but they'd just be at
    "tables/<uuid>/<index id>/" like regular secondary indexes.

    Changefeed index:  TODO

*/

fdb_future transaction_get_std_str(FDBTransaction *txn, const std::string &key, bool snapshot) {
    // TODO: Look at all callers and make an rassert.
    guarantee(key.size() <= INT_MAX);
    return fdb_future{fdb_transaction_get(
        txn,
        as_uint8(key.data()),
        int(key.size()),
        snapshot)};
}

fdb_future transaction_get_std_str(FDBTransaction *txn, const std::string &key) {
    return transaction_get_std_str(txn, key, false);
}

void transaction_clear_std_str(FDBTransaction *txn, const std::string &key) {
    // TODO: Look at all callers and make an rassert.
    guarantee(key.size() <= INT_MAX);
    fdb_transaction_clear(txn, as_uint8(key.data()), int(key.size()));
}

void transaction_set_std_str(FDBTransaction *txn, const std::string &key,
        const std::string &value) {
    // TODO: Look at all callers and make an rassert.
    guarantee(key.size() <= INT_MAX);
    guarantee(value.size() <= INT_MAX);
    fdb_transaction_set(txn, as_uint8(key.data()), int(key.size()),
        as_uint8(value.data()), int(value.size()));
}

void transaction_set_buf(FDBTransaction *txn, const std::string &key,
        const char *value, size_t value_length) {
    // TODO: Look at all callers and make an rassert.
    guarantee(key.size() <= INT_MAX);
    guarantee(value_length <= INT_MAX);
    fdb_transaction_set(txn, as_uint8(key.data()), int(key.size()),
        as_uint8(value), int(value_length));
}


approx_txn_size transaction_clear_prefix_range(FDBTransaction *txn, const std::string &prefix) {
    std::string end = prefix_end(prefix);
    fdb_transaction_clear_range(txn, as_uint8(prefix.data()), int(prefix.size()),
        as_uint8(end.data()), int(end.size()));
    return approx_txn_size{prefix.size() + end.size()};
}

fdb_value future_block_on_value(FDBFuture *fut, const signal_t *interruptor) {
    future_block_coro(fut, interruptor);
    fdb_value value;
    fdb_error_t err = future_get_value(fut, &value);
    check_for_fdb_transaction(err);
    return value;
}

key_view future_block_on_key(FDBFuture *fut, const signal_t *interruptor) {
    future_block_coro(fut, interruptor);
    key_view ret;
    fdb_error_t err = fdb_future_get_key(fut, &ret.data, &ret.length);
    if (err != 0) {
        throw fdb_transaction_exception(err);
    }
    return ret;
}

fdb_error_t commit_fdb_block_coro(FDBTransaction *txn, const signal_t *interruptor) {
    fdb_future fut{fdb_transaction_commit(txn)};
    fut.block_coro(interruptor);

    return fdb_future_get_error(fut.fut);
}

struct block_coro_state {
    linux_thread_pool_t *pool = nullptr;
    int refcount = 2;
    cond_t cond;
};

void decr_refcount(block_coro_state *state) {
    if (--state->refcount == 0) {
        delete state;
    }
}

extern "C" void block_coro_fdb_callback(FDBFuture *, void *ctx) {
    block_coro_state *state = static_cast<block_coro_state *>(ctx);
    threadnum_t thread = state->cond.home_thread();
    // TODO: We might not be on an external thread; the cb might have been called
    // inline.
    do_on_thread_from_external(state->pool, thread, [state] {
        state->cond.pulse();
        decr_refcount(state);
    });
}

void future_block_coro(FDBFuture *fut, const signal_t *interruptor) {
    block_coro_state *state = new block_coro_state;
    state->pool = linux_thread_pool_t::get_thread_pool();

    fdb_error_t err = fdb_future_set_callback(fut, block_coro_fdb_callback, state);
    // TODO: Ensure guarantee_fdb doesn't throw.
    guarantee_fdb(err, "fdb_future_set_callback failed");

    // Basically wait_interruptible below, except we want to call decr_refcount.
    // Avoids the overhead/complication of death_runner_t.
    {
        wait_any_t waiter(&state->cond, interruptor);
        waiter.wait_lazily_unordered();
    }
    decr_refcount(state);
    if (interruptor->is_pulsed()) {
        throw interrupted_exc_t();
    }
}

// FoundationDB uses little endian values for atomic ops, and serialization here
// is platform-specific.
#if !defined(BOOST_LITTLE_ENDIAN)
#error "reqlfdb_clock serialization broken on big endian"
#endif

RDB_IMPL_SERIALIZABLE_1_SINCE_v2_5(reqlfdb_clock, value);
RDB_IMPL_SERIALIZABLE_1_SINCE_v2_5(node_info, lease_expiration);

// Do these really belong.. here?
bool is_node_expired(reqlfdb_clock current_clock, reqlfdb_clock node_lease_expiration) {
    return node_lease_expiration.value <= current_clock.value;
}

bool is_node_expired(reqlfdb_clock current_clock, const node_info& info) {
    return is_node_expired(current_clock, info.lease_expiration);
}

void read_8byte_count(const signal_t *interruptor, FDBTransaction *txn, const char *key, size_t key_size, uint64_t *out) {
    fdb_future count_fut{fdb_transaction_get(txn, as_uint8(key), static_cast<int>(key_size), false)};
    count_fut.block_coro(interruptor);
    fdb_value count;
    fdb_error_t err = future_get_value(count_fut.fut, &count);
    check_for_fdb_transaction(err);
    guarantee(count.present);
    guarantee(count.length == 8);   // TODO fdb, graceful
    *out = read_LE_uint64(count.data);
}

