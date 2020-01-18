#include "reql_fdb.hpp"

#include <string.h>

#include "concurrency/cond_var.hpp"

/*

fdb storage format:

    "" => "reqlfdb 2.4.0" -- the reqlfdb version we're on

    rethinkdb/... => rethinkdb system db

Node presence and clock configuration:

    rethinkdb/clock => monotonic integer, meanderingly increments approx. every REQLFDB_TIMESTEP
    rethinkdb/node_count => number of keys under rethinkdb/nodes/
    rethinkdb/nodes/ => table of node_info_t by server uuid
      indexed with total count in place of rethinkdb/node_count?

Jobs:

    rethinkdb/jobs/ => table of job_info_t by job uuid
      indexed by lease expiration timestamp (see rethinkdb/clock)

Log:

    rethinkdb/log/ => table of log entries
      not sure how indexed

Db config:

    rethinkdb/db_config/ => table of db_config_t
      indexed by name

Table config:

    rethinkdb/table_config/ => table of table_config_t
      indexed by name

User config:

    rethinkd/user/config => table of users
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

fdb_future get_c_str(FDBTransaction *txn, const char *key) {
    return fdb_future{fdb_transaction_get(
        txn,
        reinterpret_cast<const uint8_t *>(key),
        strlen(key),
        false)};
}

fdb_error_t commit_fdb_block_coro(FDBTransaction *txn) {
    fdb_future fut{fdb_transaction_commit(txn)};
    fut.block_coro();

    return fdb_future_get_error(fut.fut);
}

extern "C" void block_coro_fdb_callback(FDBFuture *, void *ctx) {
    one_waiter_cond_t *cond = static_cast<one_waiter_cond_t *>(ctx);
    cond->pulse();
}

void fdb_future::block_coro() const {
    one_waiter_cond_t cond;
    fdb_error_t err = fdb_future_set_callback(fut, block_coro_fdb_callback, &cond);
    guarantee_fdb(err, "fdb_future_set_callback failed");
    cond.wait_ordered();
}

