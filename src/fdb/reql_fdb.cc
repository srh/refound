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

    "" => "reqlfdb 2.4.0 <uuid>" -- the reqlfdb version we're on and a random UUID

    rethinkdb/... => rethinkdb system db

Node presence and clock configuration:

    rethinkdb/clock => monotonic integer, meanderingly increments approx. every REQLFDB_TIMESTEP
    rethinkdb/nodes_count => number of keys under rethinkdb/nodes/
    rethinkdb/nodes/ => table of node_info_t by server uuid
      indexed with total count in place of rethinkdb/node_count?
      indexed by lease expiration?

Jobs:

    rethinkdb/jobs/ => table of fdb_job_info_t by job uuid
      indexed by lease expiration timestamp (see rethinkdb/clock)
      indexed by node?

Log:

    rethinkdb/log/ => table of log entries
      not sure how indexed

Db config:

    rethinkdb/db_config/ => table of db_config_t
      indexed by name

Table config:

    rethinkdb/table_config/ => table of table_config_t
      indexed by {db_uuid,table_name} => namespace_id_t.

User config:

    rethinkd/user_config/ => table of users
      indexed by name?

Config version:

    rethinkdb/config_version => monotonic integer
      Gets incremented whenever table, db, or index availability decreases (or changes?).
      Gets incremented whenever user access decreases (or changes?).

    (We don't mind querying upon rare failure case.)

Table format:

    Given a table prefix like "tables/uuid",

    Primary index:
    <table prefix>//<pkey> => <value>

    Secondary index:
    <table prefix>/<index id>/<sindex key>/<pkey> => <portion of value>

    Aggregate (count) index:
    <table prefix>/<index id> => number of records in primary index (I guess)

    Changefeed index:  TODO

*/

fdb_future transaction_get_std_str(FDBTransaction *txn, const std::string &key) {
    guarantee(key.size() <= INT_MAX);
    return fdb_future{fdb_transaction_get(
        txn,
        as_uint8(key.data()),
        int(key.size()),
        false)};
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
