#ifndef RETHINKDB_FDB_JOBS_INDEX_CREATE_HPP_
#define RETHINKDB_FDB_JOBS_INDEX_CREATE_HPP_

#include "containers/optional.hpp"
#include "fdb/jobs.hpp"

MUST_USE optional<fdb_job_info> execute_index_create_job(
        FDBTransaction *txn, const fdb_job_info &info,
        const fdb_job_index_create &index_create_info, const signal_t *interruptor);


#endif  // RETHINKDB_FDB_JOBS_INDEX_CREATE_HPP_
