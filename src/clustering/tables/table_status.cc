// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/tables/table_status.hpp"

#include "clustering/auth/user_fut.hpp"
#include "clustering/datum_adapter.hpp"
#include "clustering/tables/table_metadata.hpp"
#include "clustering/tables/table_config.hpp"
#include "containers/uuid.hpp"
#include "fdb/index.hpp"
#include "fdb/system_tables.hpp"
#include "fdb/typed.hpp"

table_status_artificial_table_fdb_backend_t::table_status_artificial_table_fdb_backend_t(
        admin_identifier_format_t _identifier_format)
    : artificial_table_fdb_backend_t(
        name_string_t::guarantee_valid("table_status")),
      identifier_format(_identifier_format) {
}

table_status_artificial_table_fdb_backend_t::~table_status_artificial_table_fdb_backend_t() {
}


bool table_status_artificial_table_fdb_backend_t::read_all_rows_as_vector(
        FDBDatabase *fdb,
        auth::user_context_t const &user_context,
        const signal_t *interruptor,
        std::vector<ql::datum_t> *rows_out,
        UNUSED admin_err_t *error_out) {

    std::vector<std::pair<namespace_id_t, table_config_t>> configs;
    std::unordered_map<database_id_t, name_string_t> db_name_table;
    if (!read_all_table_configs(this, interruptor, fdb, user_context, identifier_format,
                                &configs, &db_name_table, error_out)) {
        return false;
    }

    std::vector<ql::datum_t> rows;
    rows.reserve(configs.size());
    for (auto &pair : configs) {

        ql::datum_t db_name_or_uuid;
        switch (identifier_format) {
        case admin_identifier_format_t::name:
            db_name_or_uuid = convert_name_to_datum(db_name_table.at(pair.second.basic.database));
            break;
        case admin_identifier_format_t::uuid:
            db_name_or_uuid = convert_uuid_to_datum(pair.second.basic.database.value);
            break;
        }
        rows.push_back(format_row(
            pair.first,
            pair.second,
            db_name_or_uuid));
    }
    *rows_out = std::move(rows);
    return true;
}

bool table_status_artificial_table_fdb_backend_t::read_row(
        FDBTransaction *txn,
        UNUSED auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        const signal_t *interruptor,
        ql::datum_t *row_out,
        admin_err_t *error_out) {
    // TODO: Did we need to use user_context here?
    namespace_id_t table_id;
    admin_err_t dummy_error;
    if (!convert_uuid_from_datum(primary_key, &table_id.value, &dummy_error)) {
        /* If the primary key was not a valid UUID, then it must refer to a nonexistent
        row. */
        *row_out = ql::datum_t::null();
        return true;
    }

    // QQQ: This excludes the system tables, right?
    fdb_value_fut<table_config_t> config_fut
        = transaction_lookup_uq_index<table_config_by_id>(txn, table_id);

    table_config_t config;
    if (!config_fut.block_and_deserialize(interruptor, &config)) {
        *row_out = ql::datum_t::null();
        return true;
    }

    ql::datum_t db_name_or_uuid;
    if (!convert_database_id_to_datum(interruptor, txn, config.basic.database, identifier_format, &db_name_or_uuid, error_out)) {
        return false;
    }
    *row_out = format_row(
        table_id,
        config,
        db_name_or_uuid);
    return true;
}

// This is named to parallel o.g. RethinkDB.  But it actually takes no arguments and
// partially constructs a faux status datum object builder.
ql::datum_object_builder_t convert_table_status_to_datum() {
    ql::datum_object_builder_t builder;
    {
        ql::datum_array_builder_t shards_builder(ql::configured_limits_t::unlimited);
        /* In original RethinkDB, the shards was either null or a non-empty array.  We go
           with an array of length 1 with empty strings for server names.  This is
           designed to be compatible with hypothetical code that checks shard status
           e.g. after creating a table.  But we use null for server names, so that user
           code which expects an actual server name (instead of merely counting or
           checking readiness) breaks. */

        // Logic parallels convert_shard_status_to_datum in original RethinkDB.
        ql::datum_array_builder_t primary_replicas_builder(ql::configured_limits_t::unlimited);
        primary_replicas_builder.add(ql::datum_t::null());

        ql::datum_array_builder_t replicas_builder(ql::configured_limits_t::unlimited);
        ql::datum_object_builder_t replica;
        replica.overwrite("server", ql::datum_t::null());
        replica.overwrite("state", ql::datum_t("ready"));
        replicas_builder.add(std::move(replica).to_datum());

        ql::datum_object_builder_t shard_builder;
        shard_builder.overwrite("primary_replicas",
                                std::move(primary_replicas_builder).to_datum());
        shard_builder.overwrite("replicas",
                                std::move(replicas_builder).to_datum());
        shards_builder.add(std::move(shard_builder).to_datum());

        builder.overwrite("shards", std::move(shards_builder).to_datum());
    }

    ql::datum_object_builder_t status_builder;
    status_builder.overwrite("ready_for_outdated_reads", ql::datum_t::boolean(true));
    status_builder.overwrite("ready_for_reads", ql::datum_t::boolean(true));
    status_builder.overwrite("ready_for_writes", ql::datum_t::boolean(true));
    status_builder.overwrite("all_replicas_ready", ql::datum_t::boolean(true));
    builder.overwrite("status", std::move(status_builder).to_datum());
    return builder;
}

ql::datum_t table_status_artificial_table_fdb_backend_t::format_row(
        const namespace_id_t &table_id,
        const table_config_t &config,
        const ql::datum_t &db_name_or_uuid) const {
    ql::datum_object_builder_t builder = convert_table_status_to_datum();
    builder.overwrite("id", convert_uuid_to_datum(table_id.value));
    builder.overwrite("db", db_name_or_uuid);
    builder.overwrite("name", convert_name_to_datum(config.basic.name));

    return std::move(builder).to_datum();
}

bool table_status_artificial_table_fdb_backend_t::write_row(
        UNUSED FDBTransaction *txn,
        UNUSED auth::user_context_t const &user_context,
        UNUSED ql::datum_t primary_key,
        UNUSED bool pkey_was_autogenerated,
        UNUSED ql::datum_t *new_value_inout,
        UNUSED const signal_t *interruptor_on_caller,
        admin_err_t *error_out) {
    *error_out = admin_err_t{
        "It's illegal to write to the `rethinkdb.table_status` table.",
        query_state_t::FAILED};
    return false;
}
