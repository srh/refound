#ifndef RETHINKDB_FDB_JOBS_DB_DROP_HPP_
#define RETHINKDB_FDB_JOBS_DB_DROP_HPP_

#include "containers/optional.hpp"
#include "fdb/jobs.hpp"

MUST_USE optional<fdb_job_info> execute_db_drop_job(FDBTransaction *txn, const fdb_job_info &info,
        const fdb_job_db_drop &db_drop_info, const signal_t *interruptor);


#endif  // RETHINKDB_FDB_JOBS_DB_DROP_HPP_
