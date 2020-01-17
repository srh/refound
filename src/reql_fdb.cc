#include "reql_fdb.hpp"

#include <string.h>

#include "concurrency/cond_var.hpp"

/*

fdb storage format:

"" => "reqlfdb 2.4.0" -- the reqlfdb version we're on

rethinkdb/... => rethinkdb system db

Databases configuration:

Now: rethinkdb/db_config => databases_semilattice_metadata_t blob
TODO:
rethinkdb/db_config//<db id> => {id: <db id>, name: "dbname"}
rethinkdb/db_config/name/<name> => <db id>

Now: rethinkdb/table_config/<id>/config => table_config_t(?) blob
// TODO: something.

(read slowly upon table usage to confirm table config matches cached value)

- rethinkdb/table_config/<id>/... => ...
  (other table config information, unsure of the format)


Primary index:
tables/<table id>//<btree key> => <value> -- deal with large values at some point

Secondary index:
tables/<table id>/<index id>/<btree key> => <value>

Secondary index map:
tables/<table id>/metadata/sindex_map => std::map<sindex_name_t, secondary_index_t> -- or not, that just goes into system db

Change feeds:
<TODO>

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

