#include "fdb/jobs/index_create.hpp"


optional<fdb_job_info> execute_index_create_job(
        FDBTransaction *txn, const fdb_job_info &info,
        const fdb_job_index_create &index_create_info, const signal_t *interruptor) {

    // OOO: Implement.
    return r_nullopt;
}

