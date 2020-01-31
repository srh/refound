#ifndef RETHINKDB_FDB_JOBS_INDEX_CREATE_HPP_
#define RETHINKDB_FDB_JOBS_INDEX_CREATE_HPP_

#include "containers/optional.hpp"
#include "fdb/index.hpp"
#include "fdb/jobs.hpp"
#include "rpc/serialize_macros.hpp"

struct fdb_index_jobstate {
    // Right now, index building goes left-to-right, with no parallelization.  This is
    // either the empty string or a last-seen pkey with '\0' on the end.
    ukey_string unindexed_lower_bound;
};
RDB_MAKE_SERIALIZABLE_1(fdb_index_jobstate, unindexed_lower_bound);

struct index_jobstate_by_task {
    using ukey_type = fdb_shared_task_id;
    using value_type = fdb_index_jobstate;
    static constexpr const char *prefix = REQLFDB_INDEX_JOBSTATE_BY_TASK;
    static ukey_string ukey_str(const ukey_type &k) {
        return uuid_primary_key(k.value);
    }
};

MUST_USE optional<fdb_job_info> execute_index_create_job(
        FDBTransaction *txn, const fdb_job_info &info,
        const fdb_job_index_create &index_create_info, const signal_t *interruptor);


#endif  // RETHINKDB_FDB_JOBS_INDEX_CREATE_HPP_
