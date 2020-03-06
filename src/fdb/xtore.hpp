#ifndef RETHINKDB_FDB_XTORE_HPP_
#define RETHINKDB_FDB_XTORE_HPP_

// TODO: Rename this file to store.hpp once the other store.hpp's are gone.n

#include "errors.hpp"
#include "fdb/fdb.hpp"

class cv_check_fut;
struct reqlfdb_config_version;
class interrupted_exc_t;
struct namespace_id_t;
class signal_t;
class table_config_t;
struct read_t;
struct read_response_t;
struct write_t;
struct write_response_t;

read_response_t apply_read(FDBTransaction *txn,
    cv_check_fut &&cvc,
    const namespace_id_t &table_id,
    const table_config_t &table_config,
    const read_t &read,
    const signal_t *interruptor);

write_response_t apply_write(FDBTransaction *txn,
    cv_check_fut &&cvc,
    const namespace_id_t &table_id,
    const table_config_t &table_config,
    const write_t &write,
    const signal_t *interruptor);

bool needs_config_permission(const write_t &write);





#endif  // RETHINKDB_FDB_XTORE_HPP_
