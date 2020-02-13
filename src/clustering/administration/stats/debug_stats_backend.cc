// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/administration/stats/debug_stats_backend.hpp"

#include "clustering/administration/datum_adapter.hpp"
#include "clustering/administration/main/watchable_fields.hpp"
#include "clustering/administration/metadata.hpp"
#include "clustering/administration/servers/config_client.hpp"
#include "clustering/administration/tables/name_resolver.hpp"
#include "containers/lifetime.hpp"

debug_stats_artificial_table_backend_t::debug_stats_artificial_table_backend_t(
        lifetime_t<name_resolver_t const &> name_resolver,
        watchable_map_t<peer_id_t, cluster_directory_metadata_t> *_directory_view,
        server_config_client_t *_server_config_client,
        mailbox_manager_t *_mailbox_manager)
    : common_server_artificial_table_backend_t(
        name_string_t::guarantee_valid("_debug_stats"),
        name_resolver,
        _server_config_client,
        _directory_view),
      directory_view(_directory_view),
      mailbox_manager(_mailbox_manager) {
}

debug_stats_artificial_table_backend_t::~debug_stats_artificial_table_backend_t() {
#if RDB_CF
    begin_changefeed_destruction();
#endif
}

bool debug_stats_artificial_table_backend_t::write_row(
        auth::user_context_t const &user_context,
        UNUSED ql::datum_t primary_key,
        UNUSED bool pkey_was_autogenerated,
        UNUSED ql::datum_t *new_value_inout,
        UNUSED const signal_t *interruptor_on_caller,
        admin_err_t *error_out) {
    user_context.require_admin_user();

    *error_out = admin_err_t{
        "It's illegal to write to the `rethinkdb._debug_stats` table.",
        query_state_t::FAILED};
    return false;
}

bool debug_stats_artificial_table_backend_t::format_row(
        auth::user_context_t const &user_context,
        server_id_t const & server_id,
        UNUSED peer_id_t const & peer_id,
        cluster_directory_metadata_t const & metadata,
        const signal_t *interruptor_on_home,
        ql::datum_t *row_out,
        UNUSED admin_err_t *error_out) {
    user_context.require_admin_user();

    ql::datum_object_builder_t builder;
    builder.overwrite("name", convert_name_to_datum(
        metadata.server_config.config.name));
    builder.overwrite("id", convert_uuid_to_datum(server_id.get_uuid()));

    ql::datum_t stats;
    admin_err_t stats_error;
    if (stats_for_server(server_id, interruptor_on_home, &stats, &stats_error)) {
        builder.overwrite("stats", stats);
    } else {
        builder.overwrite("error", ql::datum_t(datum_string_t(stats_error.msg)));
    }

    *row_out = std::move(builder).to_datum();
    return true;
}

bool debug_stats_artificial_table_backend_t::stats_for_server(
        const server_id_t &server_id,
        const signal_t *interruptor_on_home,
        ql::datum_t *stats_out,
        admin_err_t *error_out) {
    optional<peer_id_t> peer_id =
        server_config_client->get_server_to_peer_map()->get_key(server_id);
    if (!peer_id.has_value()) {
        *error_out = admin_err_t{"Server is not connected.", query_state_t::FAILED};
        return false;
    }

    get_stats_mailbox_address_t request_addr;
    directory_view->read_key(*peer_id, [&](const cluster_directory_metadata_t *md) {
        if (md != nullptr) {
            request_addr = md->get_stats_mailbox_address;
        }
    });
    if (request_addr.is_nil()) {
        *error_out = admin_err_t{"Server is not connected.", query_state_t::FAILED};
        return false;
    }

    /* Make a filter that includes everything */
    std::set<std::vector<std::string> > filter;
    filter.insert(std::vector<stat_manager_t::stat_id_t>());

    return fetch_stats_from_server(
        mailbox_manager,
        request_addr,
        filter,
        interruptor_on_home,
        stats_out,
        error_out);
}

