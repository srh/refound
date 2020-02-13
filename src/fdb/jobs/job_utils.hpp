#ifndef RETHINKDB_FDB_JOBS_JOB_UTILS_HPP_
#define RETHINKDB_FDB_JOBS_JOB_UTILS_HPP_

#include "fdb/index.hpp"
#include "fdb/jobs.hpp"
#include "fdb/typed.hpp"

struct jobs_by_id {
    static constexpr const char *prefix = REQLFDB_JOBS_BY_ID;
    using ukey_type = fdb_job_id;
    using value_type = fdb_job_info;

    static ukey_string ukey_str(const ukey_type &k) {
        return ukey_string{uuid_to_str(k.value)};
    }
};

bool block_and_check_info(
        const fdb_job_info &expected_info,
        fdb_value_fut<fdb_job_info> &&real_info_fut,
        const signal_t *interruptor);

void replace_fdb_job(FDBTransaction *txn,
        const fdb_job_info &old_info, const fdb_job_info &new_info);

fdb_value_fut<fdb_job_info> transaction_get_real_job_info(
        FDBTransaction *txn, const fdb_job_info &info);

fdb_job_info update_job_counter(FDBTransaction *txn, reqlfdb_clock current_clock,
    const fdb_job_info &old_info);


#endif  // RETHINKDB_FDB_JOBS_JOB_UTILS_HPP_
