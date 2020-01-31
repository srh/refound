#ifndef RETHINKDB_FDB_JOBS_JOB_UTILS_HPP_
#define RETHINKDB_FDB_JOBS_JOB_UTILS_HPP_

#include "fdb/jobs.hpp"
#include "fdb/typed.hpp"

bool block_and_check_info(
        const fdb_job_info &expected_info,
        fdb_value_fut<fdb_job_info> &&real_info_fut,
        const signal_t *interruptor);

void replace_fdb_job(FDBTransaction *txn,
        const fdb_job_info &old_info, const fdb_job_info &new_info);

fdb_value_fut<fdb_job_info> transaction_get_real_job_info(
        FDBTransaction *txn, const fdb_job_info &info);


#endif  // RETHINKDB_FDB_JOBS_JOB_UTILS_HPP_
