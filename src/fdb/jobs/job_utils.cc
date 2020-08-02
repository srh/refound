#include "fdb/jobs/job_utils.hpp"

#include "containers/archive/string_stream.hpp"
#include "debug.hpp"
#include "fdb/index.hpp"
#include "logger.hpp"

bool block_and_check_info(
        const fdb_job_info &expected_info,
        fdb_value_fut<fdb_job_info> &&real_info_fut,
        const signal_t *interruptor) {
    fdb_job_info real_info;
    if (!real_info_fut.block_and_deserialize(interruptor, &real_info)) {
        debugf("block_and_check_info job not present\n");
        // The job is not present.  Something else must have claimed it.
        return false;
    }

    if (real_info.counter != expected_info.counter) {
        debugf("block_and_check_info job is not same\n");
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

    skey_string old_lease_expiration_key = reqlfdb_clock_sindex_key(old_info.lease_expiration);
    skey_string new_lease_expiration_key = reqlfdb_clock_sindex_key(new_info.lease_expiration);

    ukey_string job_id_key = jobs_by_id::ukey_str(new_info.job_id);

    transaction_set_uq_index<jobs_by_id>(txn, new_info.job_id, new_info);
    transaction_erase_plain_index(txn, REQLFDB_JOBS_BY_LEASE_EXPIRATION,
        old_lease_expiration_key, job_id_key);
    transaction_set_plain_index(txn, REQLFDB_JOBS_BY_LEASE_EXPIRATION,
        new_lease_expiration_key, job_id_key, "");
    // Task index untouched.
}

fdb_value_fut<fdb_job_info> transaction_get_real_job_info(
        FDBTransaction *txn, const fdb_job_info &info) {
    return transaction_lookup_uq_index<jobs_by_id>(txn, info.job_id);
}

fdb_job_info update_job_counter(FDBTransaction *txn, reqlfdb_clock current_clock,
        const fdb_job_info &old_info) {
    fdb_job_info new_info = old_info;
    new_info.counter++;
    new_info.lease_expiration = reqlfdb_clock{current_clock.value + REQLFDB_JOB_LEASE_DURATION};

    replace_fdb_job(txn, old_info, new_info);

    return new_info;
}

void execute_job_failed(FDBTransaction *txn, const fdb_job_info &info,
        const std::string &message, const signal_t *interruptor) {
    fdb_value_fut<fdb_job_info> real_info_fut
        = transaction_get_real_job_info(txn, info);

    if (!block_and_check_info(info, std::move(real_info_fut), interruptor)) {
        logWRN("Job info was updated after this node failed, for job %s, after failure '%s'",
            uuid_to_str(info.job_id.value).c_str(), message.c_str());
        return;
    }

    fdb_job_info new_info = info;
    new_info.lease_expiration = reqlfdb_clock{UINT64_MAX};
    new_info.failed.set(message);
    replace_fdb_job(txn, info, new_info);
    commit(txn, interruptor);
}
