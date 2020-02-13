// Copyright 2010-2014 RethinkDB, all rights reserved.

#include "clustering/administration/jobs/jobs_backend.hpp"

#include <set>

// OOO: Remove needless includes
#include "clustering/administration/datum_adapter.hpp"
#include "clustering/administration/jobs/manager.hpp"
#include "clustering/administration/jobs/report.hpp"
#include "clustering/administration/main/watchable_fields.hpp"
#include "clustering/administration/metadata.hpp"
#include "clustering/administration/tables/name_resolver.hpp"
#include "concurrency/cross_thread_signal.hpp"
#include "containers/lifetime.hpp"
#include "fdb/jobs.hpp"
#include "fdb/retry_loop.hpp"
#include "rpc/mailbox/disconnect_watcher.hpp"

jobs_artificial_table_fdb_backend_t::jobs_artificial_table_fdb_backend_t(
        admin_identifier_format_t _identifier_format)
#if RDB_CF
    : timer_cfeed_artificial_table_fdb_backend_t(
        name_string_t::guarantee_valid("jobs")),
#else
    : artificial_table_fdb_backend_t(
        name_string_t::guarantee_valid("jobs")),
#endif
      identifier_format(_identifier_format) {
}

jobs_artificial_table_fdb_backend_t::~jobs_artificial_table_fdb_backend_t() {
#if RDB_CF
    begin_changefeed_destruction();
#endif
}

std::string jobs_artificial_table_fdb_backend_t::get_primary_key_name() const {
    return "id";
}

ql::datum_t clock_datum(reqlfdb_clock clk) {
    return ql::datum_t(double(clk.value));
}

ql::datum_t job_info_to_datum(const fdb_job_info &info,
        admin_identifier_format_t identifier_format) {
    // OOO: Make use of identifier_format.
    // "info" is the old pre-fdb name for fdb_job_description data.
    ql::datum_t info_datum;
    std::string type_str;
    switch (info.job_description.type) {
    case fdb_job_type::dummy_job: {
        type_str = "dummy";
        info_datum = ql::datum_t::empty_object();
    } break;
    case fdb_job_type::db_drop_job: {
        type_str = "db_drop";
        ql::datum_object_builder_t info_builder;
        info_builder.overwrite("db", ql::datum_t(uuid_to_str(info.job_description.db_drop.database_id.value)));
        // TODO: A complete hack around the string incrementing thing.  That's OK, but... meh.
        std::string progress = info.job_description.db_drop.min_table_name;
        if (!progress.empty() && progress.back() == '\0') {
            progress.pop_back();
        }
        info_builder.overwrite("progress",
            ql::datum_t(datum_string_t(progress)));
        info_datum = std::move(info_builder).to_datum();
    } break;
    case fdb_job_type::index_create_job: {
        const fdb_job_index_create &index_create = info.job_description.index_create;
        type_str = "index_construction";
        ql::datum_object_builder_t info_builder;
        info_builder.overwrite("table", ql::datum_t(uuid_to_str(index_create.table_id.value)));
        info_builder.overwrite("sindex", ql::datum_t(uuid_to_str(index_create.sindex_id.value)));
        info_builder.overwrite("sindex_name", ql::datum_t(index_create.sindex_name));
        info_datum = std::move(info_builder).to_datum();
        // TODO: Formulate additional fdb query to get progress info.
    } break;
    default:
        unreachable();
    }

    ql::datum_object_builder_t builder;
    builder.overwrite("type", ql::datum_t(type_str));
    builder.overwrite("id", ql::datum_t(uuid_to_str(info.job_id.value)));
    // QQQ builder.overwrite("duration_sec", ___);
    builder.overwrite("task_id", ql::datum_t(uuid_to_str(info.shared_task_id.value)));
    builder.overwrite("claiming_node", info.claiming_node_or_nil.value.is_nil() ?
        ql::datum_t::null() : ql::datum_t(uuid_to_str(info.claiming_node_or_nil.value)));
    builder.overwrite("counter", ql::datum_t(double(info.counter)));
    builder.overwrite("lease_expiration", clock_datum(info.lease_expiration));
    builder.overwrite("info", std::move(info_datum));
    return std::move(builder).to_datum();
}

// Doesn't even take jobs as a parameter. -- because right now, the only jobs are
// index construction and db drop jobs, both of which are admin-only.  (There are no
// query jobs yet, and there probably won't be any time soon.)
MUST_USE bool filter_jobs_by_permissions(const auth::user_context_t &user_context) {
    return user_context.is_admin_user();
}

bool jobs_artificial_table_fdb_backend_t::read_all_rows_as_vector(
        FDBDatabase *fdb,
        auth::user_context_t const &user_context,
        const signal_t *interruptor,
        std::vector<ql::datum_t> *rows_out,
        UNUSED admin_err_t *error_out) {
    rows_out->clear();

    if (!filter_jobs_by_permissions(user_context)) {
        return true;
    }

    std::vector<ql::datum_t> rows;
    // job_reports is naturally sorted by job id.
    fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor, [&](FDBTransaction *txn) {
        std::vector<fdb_job_info> job_infos = lookup_all_fdb_jobs(txn, interruptor);
        std::vector<ql::datum_t> job_reports;
        for (fdb_job_info &info : job_infos) {
            job_reports.push_back(job_info_to_datum(info, identifier_format));
        }

        rows = std::move(job_reports);
    });
    guarantee_fdb_TODO(loop_err, "jobs_artificial_table_fdb_backend_t::read_all_rows_as_vector");

    *rows_out = std::move(rows);
    return true;
}

bool jobs_artificial_table_fdb_backend_t::read_row(
        FDBTransaction *txn,
        auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        const signal_t *interruptor,
        ql::datum_t *row_out,
        admin_err_t *error_out) {
    *row_out = ql::datum_t();

    // TODO: Obviously, questions like what about shared task id, but that's more important when we get to job deletion.
    uuid_u job_id;
    if (!convert_uuid_from_datum(primary_key, &job_id, error_out)) {
        return false;
    }

    if (!filter_jobs_by_permissions(user_context)) {
        return true;
    }

    optional<fdb_job_info> info = lookup_fdb_job(txn, fdb_job_id{job_id}, interruptor);
    if (!info.has_value()) {
        // We assigned row_out above, and we successfully read that there is no job.
        // (Preserving existing behavior, if this is a bug.)
        return true;
    }

    ql::datum_t job_report = job_info_to_datum(*info, identifier_format);

    *row_out = std::move(job_report);
    return true;
}

bool jobs_artificial_table_fdb_backend_t::write_row(
        auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        bool pkey_was_autogenerated,
        ql::datum_t *new_value_inout,
        UNUSED const signal_t *interruptor_on_caller,
        admin_err_t *error_out) {
    *error_out = admin_err_t{
        "The `rethinkdb.jobs` system table does not allow writes.",
        query_state_t::FAILED};

    // TODO: For all jobs so far, it would be possible to make general changes like kicking servers off of jobs, or temporarily pausing them.
    // (That might not be true for query jobs, if they ever get exposed.)

    // Possible job interruptions:

    // For index creation jobs, we could delete the index (but implementing that is low
    // priority).

    // For db drop jobs, we could abort the db drop operation and salvage the orphaned
    // tables.  It probably goes too fast for that to be useful.

    return false;
}
