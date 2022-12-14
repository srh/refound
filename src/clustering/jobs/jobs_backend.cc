// Copyright 2010-2014 RethinkDB, all rights reserved.

#include "clustering/jobs/jobs_backend.hpp"

#include <string>

#include "clustering/auth/user_context.hpp"
#include "clustering/auth/user_fut.hpp"
#include "clustering/admin_op_exc.hpp"
#include "clustering/datum_adapter.hpp"
#include "clustering/tables/table_metadata.hpp"
#include "fdb/index.hpp"
#include "fdb/jobs.hpp"
#include "fdb/retry_loop.hpp"
#include "fdb/system_tables.hpp"

jobs_artificial_table_fdb_backend_t::jobs_artificial_table_fdb_backend_t(
        admin_identifier_format_t _identifier_format)
    : artificial_table_fdb_backend_t(
        name_string_t::guarantee_valid("jobs")),
      identifier_format(_identifier_format) {
}

jobs_artificial_table_fdb_backend_t::~jobs_artificial_table_fdb_backend_t() {
}

ql::datum_t clock_datum(reqlfdb_clock clk) {
    return ql::datum_t(double(clk.value));
}

ql::datum_t job_info_to_datum(
        const fdb_job_info &info,
        admin_identifier_format_t identifier_format,
        const std::unordered_map<namespace_id_t, name_string_t> &table_names) {
    // "info" is the old pre-fdb name for fdb_job_description data.
    ql::datum_t info_datum;
    std::string type_str;
    switch (info.job_description.type) {
    case fdb_job_type::db_drop_job: {
        type_str = "db_drop";
        ql::datum_object_builder_t info_builder;
        const fdb_job_db_drop &db_drop = boost::get<fdb_job_db_drop>(info.job_description.v);
        info_builder.overwrite("db", convert_name_or_uuid_to_datum(db_drop.database_name,
                                                                   db_drop.database_id.value,
                                                                   identifier_format));
        const optional<std::string> &last_table_name
            = db_drop.last_table_name;
        info_builder.overwrite("progress",
            last_table_name.has_value() ?
                ql::datum_t(*last_table_name) : ql::datum_t::null());
        info_datum = std::move(info_builder).to_datum();
    } break;
    case fdb_job_type::index_create_job: {
        const fdb_job_index_create &index_create = boost::get<fdb_job_index_create>(info.job_description.v);
        type_str = "index_construction";
        ql::datum_object_builder_t info_builder;
        if (identifier_format == admin_identifier_format_t::name) {
            info_builder.overwrite("table", convert_name_to_datum(table_names.at(index_create.table_id)));
        } else {
            info_builder.overwrite("table", convert_uuid_to_datum(index_create.table_id.value));
        }
        info_builder.overwrite("sindex", ql::datum_t(uuid_to_str(index_create.sindex_id.value)));
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
    builder.overwrite("failed", ql::datum_t::boolean(info.failed.has_value()));
    builder.overwrite("message", info.failed.has_value() ?
        ql::datum_t(*info.failed) : ql::datum_t::null());
    builder.overwrite("info", std::move(info_datum));
    return std::move(builder).to_datum();
}

// Doesn't even take jobs as a parameter. -- because right now, the only jobs are
// index construction and db drop jobs, both of which are admin-only.  (There are no
// query jobs yet, and there probably won't be any time soon.)
MUST_USE bool filter_jobs_by_permissions(const auth::user_context_t &user_context) {
    return user_context.is_admin_user();
}

std::unordered_map<namespace_id_t, name_string_t> lookup_all_table_names(FDBTransaction *txn, const signal_t *interruptor,
        const std::vector<fdb_job_info> &job_infos) {
    std::unordered_map<namespace_id_t, fdb_value_fut<table_config_t>> futs;
    for (auto &info : job_infos) {
        switch (info.job_description.type) {
        case fdb_job_type::db_drop_job: {
            // Do nothing.
        } break;
        case fdb_job_type::index_create_job: {
            const fdb_job_index_create &index_create = boost::get<fdb_job_index_create>(info.job_description.v);
            if (futs.find(index_create.table_id) == futs.end()) {
                futs.emplace(index_create.table_id, transaction_lookup_uq_index<table_config_by_id>(txn, index_create.table_id));
            }
        } break;
        default:
            unreachable();
        }
    }

    std::unordered_map<namespace_id_t, name_string_t> ret;
    for (auto &pair : futs) {
        table_config_t config = pair.second.block_and_deserialize(interruptor);
        ret.emplace(pair.first, std::move(config.basic.name));
    }
    return ret;
}

bool jobs_artificial_table_fdb_backend_t::read_all_rows_as_vector(
        FDBDatabase *fdb,
        auth::user_context_t const &user_context,
        const signal_t *interruptor,
        std::vector<ql::datum_t> *rows_out,
        admin_err_t *error_out) {
    rows_out->clear();

    if (!filter_jobs_by_permissions(user_context)) {
        return true;
    }

    // TODO: Break into multiple txn's?  And such.
    std::vector<ql::datum_t> rows;
    // job_reports is naturally sorted by job id.
    fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor, [&](FDBTransaction *txn) {
        auth::fdb_user_fut<auth::read_permission> auth_fut = get_read_permission(txn, user_context);
        std::vector<fdb_job_info> job_infos = lookup_all_fdb_jobs(txn, interruptor);

        std::unordered_map<namespace_id_t, name_string_t> table_names;
        if (identifier_format == admin_identifier_format_t::name) {
            table_names = lookup_all_table_names(txn, interruptor, job_infos);
        }

        std::vector<ql::datum_t> job_reports;
        for (fdb_job_info &info : job_infos) {
            job_reports.push_back(job_info_to_datum(info, identifier_format, table_names));
        }
        auth_fut.block_and_check(interruptor);
        rows = std::move(job_reports);
    });
    if (set_fdb_error(loop_err, error_out, "Error reading `rethinkdb.jobs` table")) {
        return false;
    }

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
        *row_out = ql::datum_t::null();
        return true;
    }

    optional<fdb_job_info> info = lookup_fdb_job(txn, fdb_job_id{job_id}, interruptor);
    if (!info.has_value()) {
        *row_out = ql::datum_t::null();
        return true;
    }

    std::unordered_map<namespace_id_t, name_string_t> table_names;
    if (identifier_format == admin_identifier_format_t::name) {
        table_names = lookup_all_table_names(txn, interruptor, {*info});
    }


    *row_out = job_info_to_datum(*info, identifier_format, table_names);
    return true;
}

bool jobs_artificial_table_fdb_backend_t::write_row(
        UNUSED FDBTransaction *txn,
        UNUSED auth::user_context_t const &user_context,
        UNUSED ql::datum_t primary_key,
        UNUSED bool pkey_was_autogenerated,
        UNUSED ql::datum_t *new_value_inout,
        UNUSED const signal_t *interruptor,
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
