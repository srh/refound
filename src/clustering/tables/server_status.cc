// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/tables/server_status.hpp"

#include "clustering/datum_adapter.hpp"
#include "clustering/tables/server_config.hpp"
#include "containers/uuid.hpp"
#include "fdb/index.hpp"
#include "fdb/retry_loop.hpp"
#include "fdb/system_tables.hpp"
#include "fdb/typed.hpp"
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"

server_status_artificial_table_fdb_backend_t::server_status_artificial_table_fdb_backend_t(
        admin_identifier_format_t _identifier_format)
    : artificial_table_fdb_backend_t(
        name_string_t::guarantee_valid("server_status")),
      identifier_format(_identifier_format) {
}

server_status_artificial_table_fdb_backend_t::~server_status_artificial_table_fdb_backend_t() {
}

ql::datum_t format_server_status_row(const fdb_node_id &node_id, const node_info &info, reqlfdb_clock current_clock) {
    ql::datum_object_builder_t builder;
    node_name_and_id nai = compute_node_name_and_id(node_id);
    builder.overwrite("name", ql::datum_t(nai.name));
    builder.overwrite("id", ql::datum_t(nai.id_string));

    double cache_size = 0;

    ql::datum_object_builder_t proc_builder;
    proc_builder.overwrite("time_started",
        convert_microtime_to_datum(info.proc_metadata.time_started));
    proc_builder.overwrite("version",
        ql::datum_t(datum_string_t(info.proc_metadata.version)));
    proc_builder.overwrite("pid", ql::datum_t(static_cast<double>(info.proc_metadata.pid)));
    proc_builder.overwrite("argv",
        convert_vector_to_datum<std::string>(&convert_string_to_datum, info.proc_metadata.argv));
    proc_builder.overwrite("cache_size_mb", ql::datum_t(cache_size));
    builder.overwrite("process", std::move(proc_builder).to_datum());

    ql::datum_object_builder_t net_builder;
    net_builder.overwrite("connected_to", ql::datum_object_builder_t().to_datum());
    net_builder.overwrite("hostname", ql::datum_t(datum_string_t(info.proc_metadata.hostname)));
    net_builder.overwrite("cluster_port", ql::datum_t::null());
    net_builder.overwrite("reql_port", convert_port_to_datum(info.proc_metadata.reql_port.value));
    net_builder.overwrite("http_admin_port", info.proc_metadata.http_admin_port.has_value()
        ? convert_port_to_datum(info.proc_metadata.http_admin_port->value)
        : ql::datum_t("<no http admin>"));
    net_builder.overwrite("canonical_addresses", ql::datum_t::null());
    net_builder.overwrite("time_connected", ql::datum_t::null());

    builder.overwrite("network", std::move(net_builder).to_datum());

    ql::datum_object_builder_t fdb;
    fdb.overwrite("lease_expiration", ql::datum_t(std::to_string(info.lease_expiration.value)));
    fdb.overwrite("current_clock", ql::datum_t(std::to_string(current_clock.value)));
    builder.overwrite("fdb", std::move(fdb).to_datum());

    return std::move(builder).to_datum();
}


bool server_status_artificial_table_fdb_backend_t::read_all_rows_as_vector(
        FDBDatabase *fdb,
        UNUSED auth::user_context_t const &user_context,
        const signal_t *interruptor,
        std::vector<ql::datum_t> *rows_out,
        admin_err_t *error_out) {
    std::vector<std::pair<fdb_node_id, node_info>> node_infos;
    reqlfdb_clock current_clock;
    fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor, [&](FDBTransaction *txn) {
        reqlfdb_clock clock;
        auto infos = read_all_node_infos(interruptor, txn, &clock);

        node_infos = std::move(infos);
        current_clock = clock;
    });
    if (set_fdb_error(loop_err, error_out, "Error reading `rethinkdb.server_status` table")) {
        return false;
    }

    std::vector<ql::datum_t> result;
    for (auto& pair : node_infos) {
        result.push_back(format_server_status_row(pair.first, pair.second, current_clock));
    }

    *rows_out = std::move(result);
    return true;
}

bool server_status_artificial_table_fdb_backend_t::read_row(
        FDBTransaction *txn,
        UNUSED auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        const signal_t *interruptor,
        ql::datum_t *row_out,
        UNUSED admin_err_t *error_out) {
    // This duplicates everything except formatting with
    // server_config_artificial_table_fdb_backend_t::read_row.
    //
    // In o.g. RethinkDb we had a server_common... base class.

    fdb_node_id node_id;
    admin_err_t dummy_error;
    if (!convert_uuid_from_datum(primary_key, &node_id.value, &dummy_error)) {
        /* If the primary key was not a valid UUID, then it must refer to a nonexistent
        row. */
        *row_out = ql::datum_t::null();
        return true;
    }

    fdb_value_fut<reqlfdb_clock> clock_fut = transaction_get_clock(txn);
    fdb_value_fut<node_info> fut = transaction_lookup_uq_index<node_info_by_id>(txn, node_id);

    node_info info;
    if (!fut.block_and_deserialize(interruptor, &info)) {
        *row_out = ql::datum_t::null();
        return true;
    }

    reqlfdb_clock clock = clock_fut.block_and_deserialize(interruptor);
    if (is_node_expired(clock, info)) {
        *row_out = ql::datum_t::null();
        return true;
    }

    *row_out = format_server_status_row(node_id, info, clock);
    return true;
}


bool server_status_artificial_table_fdb_backend_t::write_row(
        UNUSED FDBTransaction *txn,
        UNUSED auth::user_context_t const &user_context,
        UNUSED ql::datum_t primary_key,
        UNUSED bool pkey_was_autogenerated,
        UNUSED ql::datum_t *new_value_inout,
        UNUSED const signal_t *interruptor,
        admin_err_t *error_out) {
    *error_out = admin_err_t{
        "It's illegal to write to the `rethinkdb.server_status` table.",
        query_state_t::FAILED};
    return false;
}
