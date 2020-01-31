#include "fdb/jobs/db_drop.hpp"

#include "fdb/index.hpp"
#include "fdb/jobs/job_utils.hpp"
#include "fdb/typed.hpp"
// TODO: Gross dependency order.
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"

// Returns new job info if we have re-claimed this job and want to execute it again.
MUST_USE optional<fdb_job_info> execute_db_drop_job(FDBTransaction *txn, const fdb_job_info &info,
        const fdb_job_db_drop &db_drop_info, const signal_t *interruptor) {
    // TODO: Maybe caller can pass clock.
    fdb_value_fut<reqlfdb_clock> clock_fut = transaction_get_clock(txn);
    fdb_value_fut<fdb_job_info> real_info_fut{
        transaction_lookup_pkey_index(txn, REQLFDB_JOBS_BY_ID, job_id_pkey(info.job_id))};
    // We always make it a closed interval, because we deleted the last-used table name
    // anyway.
    fdb_future range_fut = transaction_get_table_range(
        txn, db_drop_info.database_id, db_drop_info.min_table_name, true,
        FDB_STREAMING_MODE_SMALL);

    if (!block_and_check_info(info, std::move(real_info_fut), interruptor)) {
        return r_nullopt;
    }

    range_fut.block_coro(interruptor);

    const FDBKeyValue *kv;
    int kv_count;
    fdb_bool_t more;
    fdb_error_t err = fdb_future_get_keyvalue_array(range_fut.fut, &kv, &kv_count, &more);
    check_for_fdb_transaction(err);

    std::string last_table;
    for (int i = 0; i < kv_count; ++i) {
        key_view key{void_as_uint8(kv[i].key), kv[i].key_length};
        std::string table_name
            = unserialize_table_by_name_table_name(key, db_drop_info.database_id);

        bool exists = help_remove_table_if_exists(
            txn, db_drop_info.database_id, table_name, interruptor);
        guarantee(exists, "Table was just seen to exist, now it doesn't.");

        last_table = table_name;
    }

    optional<fdb_job_info> ret;
    if (more) {
        reqlfdb_clock current_clock = clock_fut.block_and_deserialize(interruptor);

        fdb_job_info new_info = info;
        new_info.counter++;
        new_info.lease_expiration = reqlfdb_clock{current_clock.value + REQLFDB_JOB_LEASE_DURATION};

        replace_fdb_job(txn, info, new_info);

        ret.set(std::move(new_info));
    } else {
        remove_fdb_job(txn, info);
        ret = r_nullopt;
    }

    commit(txn, interruptor);
    return ret;
}

