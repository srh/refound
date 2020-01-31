#include "fdb/jobs/job_utils.hpp"

#include "containers/archive/string_stream.hpp"
#include "fdb/index.hpp"

bool block_and_check_info(
        const fdb_job_info &expected_info,
        fdb_value_fut<fdb_job_info> &&real_info_fut,
        const signal_t *interruptor) {
    fdb_job_info real_info;
    if (!real_info_fut.block_and_deserialize(interruptor, &real_info)) {
        // The job is not present.  Something else must have claimed it.
        return false;
    }

    if (real_info.counter != expected_info.counter) {
        // The job is not the same.  Another node must have claimed it.
        // (Possibly ourselves?  Who knows.)
        return false;
    }

    // This is impossible, but it could happen in a scenario with duplicate
    // generate_uuid(), I guess.
    guarantee(real_info == expected_info,
        "Job info is different with the same counter value.");
    return true;
}


void replace_fdb_job(FDBTransaction *txn,
        const fdb_job_info &old_info, const fdb_job_info &new_info) {
    // Basic sanity check.
    guarantee(new_info.job_id == old_info.job_id);
    // Sanity check: we don't have to update the task index.
    guarantee(new_info.shared_task_id == old_info.shared_task_id);

    std::string new_job_info_str = serialize_for_cluster_to_string(new_info);
    skey_string old_lease_expiration_key = reqlfdb_clock_sindex_key(old_info.lease_expiration);
    skey_string new_lease_expiration_key = reqlfdb_clock_sindex_key(new_info.lease_expiration);

    ukey_string job_id_key = job_id_pkey(new_info.job_id);

    transaction_set_pkey_index(txn, REQLFDB_JOBS_BY_ID, job_id_key, new_job_info_str);
    transaction_erase_plain_index(txn, REQLFDB_JOBS_BY_LEASE_EXPIRATION,
        old_lease_expiration_key, job_id_key);
    transaction_set_plain_index(txn, REQLFDB_JOBS_BY_LEASE_EXPIRATION,
        old_lease_expiration_key, job_id_key, "");
    // Task index untouched.
}

fdb_value_fut<fdb_job_info> transaction_get_real_job_info(
        FDBTransaction *txn, const fdb_job_info &info) {
    ukey_string job_id_key = job_id_pkey(info.job_id);
    return fdb_value_fut<fdb_job_info>{
        transaction_lookup_pkey_index(txn, REQLFDB_JOBS_BY_ID, job_id_key)};
}

