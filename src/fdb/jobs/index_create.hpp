#ifndef RETHINKDB_FDB_JOBS_INDEX_CREATE_HPP_
#define RETHINKDB_FDB_JOBS_INDEX_CREATE_HPP_

#include "containers/archive/optional.hpp"
#include "containers/optional.hpp"
#include "fdb/index.hpp"
#include "fdb/jobs.hpp"
#include "rpc/serialize_macros.hpp"

struct fdb_index_jobstate {
    // ["", unindexed_upper_bound) describes the content of the pkey store that has not
    // been indexed.

    // This is the open upper bound of the range of unindexed pkeys.  r_nullopt means
    // +infinity.
    optional<ukey_string> unindexed_upper_bound;

    // TODO: Verify FDB limits on value length are at least twice that of key length.
};
RDB_MAKE_SERIALIZABLE_1(fdb_index_jobstate, unindexed_upper_bound);

struct index_jobstate_by_task {
    using ukey_type = fdb_shared_task_id;
    using value_type = fdb_index_jobstate;
    static constexpr const char *prefix = REQLFDB_INDEX_JOBSTATE_BY_TASK;
    static ukey_string ukey_str(const ukey_type &k) {
        return uuid_primary_key(k.value);
    }
};

MUST_USE fdb_error_t execute_index_create_job(
        const signal_t *interruptor,
        FDBDatabase *fdb,
        const fdb_job_info &info,
        const fdb_job_index_create &index_create_info,
        job_execution_result *result_out);



#endif  // RETHINKDB_FDB_JOBS_INDEX_CREATE_HPP_
