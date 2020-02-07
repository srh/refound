#ifndef RETHINKDB_FDB_XTORE_HPP_
#define RETHINKDB_FDB_XTORE_HPP_

// TODO: Rename this file to store.hpp once the other store.hpp's are gone.n

#include "errors.hpp"
#include "fdb/fdb.hpp"

struct reqlfdb_config_version;
class interrupted_exc_t;
struct namespace_id_t;
class signal_t;
struct read_t;
struct read_response_t;
struct write_t;
struct write_response_t;

read_response_t apply_read(FDBTransaction *txn,
    reqlfdb_config_version expected_cv,
    const namespace_id_t &table_id,
    const read_t &read,
    const signal_t *interruptor);

write_response_t apply_write(FDBTransaction *txn,
    reqlfdb_config_version expected_cv,
    const namespace_id_t &table_id,
    const write_t &write,
    const signal_t *interruptor);







#endif  // RETHINKDB_FDB_XTORE_HPP_
