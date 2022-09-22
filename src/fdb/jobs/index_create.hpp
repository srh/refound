#ifndef RETHINKDB_FDB_JOBS_INDEX_CREATE_HPP_
#define RETHINKDB_FDB_JOBS_INDEX_CREATE_HPP_

#include "containers/optional.hpp"
#include "fdb/index.hpp"
#include "fdb/jobs.hpp"
#include "rpc/serialize_macros.hpp"

struct fdb_index_jobstate {
    // [unindexed_lower_bound, unindexed_upper_bound) describes the content
    // of the pkey store that has not been indexed.

    // Either the empty string or a last-seen pkey with '\0' on the end.  TODO: Might be '\1' now, see increment1.
    ukey_string unindexed_lower_bound;
    // This is the last pkey in the store, with '\0' added on the end.  TODO: Might be '\1' now, see increment1.
    ukey_string unindexed_upper_bound;

    // TODO: unindexed_upper_bound never changes as long as the jobstate exists, so we
    // could have two jobstate keys, one getting cached by the clients.

    // TODO: Verify FDB limits on value length are at least twice that of key length.
};
RDB_MAKE_SERIALIZABLE_2(fdb_index_jobstate, unindexed_lower_bound, unindexed_upper_bound);

struct index_jobstate_by_task {
    using ukey_type = fdb_shared_task_id;
    using value_type = fdb_index_jobstate;
    static constexpr const char *prefix = REQLFDB_INDEX_JOBSTATE_BY_TASK;
    static ukey_string ukey_str(const ukey_type &k) {
        return uuid_primary_key(k.value);
    }
};

MUST_USE job_execution_result execute_index_create_job(
        const signal_t *interruptor,
        FDBTransaction *txn,
        uint64_t retry_count,
        const fdb_job_info &info,
        const fdb_job_index_create &index_create_info);


#endif  // RETHINKDB_FDB_JOBS_INDEX_CREATE_HPP_
