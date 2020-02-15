// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/administration/tables/debug_table_status.hpp"

#include "clustering/administration/datum_adapter.hpp"
#include "clustering/administration/metadata.hpp"
#include "clustering/administration/servers/config_client.hpp"
#include "clustering/administration/tables/table_config.hpp"
#include "clustering/table_manager/table_meta_client.hpp"

namespace ql {

// src/rdb_protocol/terms/sindex.cc
ql::datum_t sindex_status_to_datum(
        const std::string &,
        const sindex_config_t &,
        const sindex_status_t &);

} // namespace ql

debug_table_status_artificial_table_backend_t::
            debug_table_status_artificial_table_backend_t(
        lifetime_t<name_resolver_t const &> name_resolver,
        std::shared_ptr<semilattice_readwrite_view_t<
            cluster_semilattice_metadata_t> > _semilattice_view,
        table_meta_client_t *_table_meta_client)
    : common_table_artificial_table_backend_t(
        name_string_t::guarantee_valid("_debug_table_status"),
        name_resolver,
        _semilattice_view,
        _table_meta_client,
        admin_identifier_format_t::uuid) {
}

debug_table_status_artificial_table_backend_t::
        ~debug_table_status_artificial_table_backend_t() {
#if RDB_CF
    begin_changefeed_destruction();
#endif
}

bool debug_table_status_artificial_table_backend_t::write_row(
        auth::user_context_t const &user_context,
        UNUSED ql::datum_t primary_key,
        UNUSED bool pkey_was_autogenerated,
        UNUSED ql::datum_t *new_value_inout,
        UNUSED const signal_t *interruptor_on_caller,
        admin_err_t *error_out) {
    user_context.require_admin_user();

    *error_out = admin_err_t{
        "It's illegal to write to the `rethinkdb.table_status` table.",
        query_state_t::FAILED};
    return false;
}

ql::datum_t convert_debug_multi_table_manager_bcard_timestamp_to_datum(
        const multi_table_manager_timestamp_t &timestamp) {
    ql::datum_object_builder_t builder;
    builder.overwrite(
        "epoch", timestamp.epoch.to_datum());
    builder.overwrite(
        "log_index", ql::datum_t(static_cast<double>(timestamp.log_index)));
    return std::move(builder).to_datum();
}

ql::datum_t convert_debug_store_key_to_datum(const store_key_t &store_key) {
    return ql::datum_t::binary(datum_string_t(
        store_key.size(),
        reinterpret_cast<const char *>(store_key.data())));
}

ql::datum_t convert_debug_region_to_datum(const region_t &region) {
    ql::datum_object_builder_t builder;
    builder.overwrite("key_min",
        convert_debug_store_key_to_datum(region.left));
    builder.overwrite("key_max",
        region.right.unbounded
            ? ql::datum_t::null()
            : convert_debug_store_key_to_datum(region.right.key()));
    return std::move(builder).to_datum();
}

ql::datum_t convert_debug_contracts_to_datum(
        const std::map<contract_id_t, contract_t> &contracts) {
    ql::datum_array_builder_t builder(ql::configured_limits_t::unlimited);
    for (const auto &contract : contracts) {
        ql::datum_object_builder_t contract_builder;
        contract_builder.overwrite(
            "contract", convert_uuid_to_datum(contract.first.value));
        contract_builder.overwrite(
            "the_server", convert_uuid_to_datum(contract.second.the_server.get_uuid()));
        builder.add(std::move(contract_builder).to_datum());
    }
    return std::move(builder).to_datum();
}

ql::datum_t convert_debug_table_shard_scheme_to_datum() {
    ql::datum_object_builder_t builder;
    builder.overwrite("split_points", ql::datum_t(
        std::vector<ql::datum_t>(),
        ql::configured_limits_t::unlimited));
    return std::move(builder).to_datum();
}

ql::datum_t convert_debug_contract_ack_state_to_datum(
       const contract_ack_t::state_t &state) {
    switch (state) {
        case contract_ack_t::state_t::primary_need_branch:
            return convert_string_to_datum("primary_need_branch");
        case contract_ack_t::state_t::primary_in_progress:
            return convert_string_to_datum("primary_in_progress");
        case contract_ack_t::state_t::primary_ready:
            return convert_string_to_datum("primary_ready");
        default:
            unreachable();
    }
}

ql::datum_t convert_debug_version_to_datum(const version_t &version) {
    ql::datum_object_builder_t builder;
    builder.overwrite("branch", convert_uuid_to_datum(version.branch.value));
    builder.overwrite("timestamp", ql::datum_t(static_cast<double>(
        version.timestamp.to_repli_timestamp().longtime)));
    return std::move(builder).to_datum();
}

ql::datum_t convert_debug_version_map_to_datum(
        const region_map_t<version_t> &map) {
    ql::datum_array_builder_t builder(ql::configured_limits_t::unlimited);
    map.visit(
        map.get_domain(),
        [&](const region_t &region, const version_t &version) {
            ql::datum_object_builder_t pair_builder;
            pair_builder.overwrite("region",
                convert_debug_region_to_datum(region));
            pair_builder.overwrite("version",
                convert_debug_version_to_datum(version));
            builder.add(std::move(pair_builder).to_datum());
        });
    return std::move(builder).to_datum();
}

ql::datum_t convert_debug_current_branches_to_datum(
        const region_map_t<branch_id_t> &map) {
    ql::datum_array_builder_t builder(ql::configured_limits_t::unlimited);
    map.visit(
        map.get_domain(),
        [&](const region_t &region, const branch_id_t &branch) {
            ql::datum_object_builder_t pair_builder;
            pair_builder.overwrite("region",
                convert_debug_region_to_datum(region));
            pair_builder.overwrite("branch",
                convert_uuid_to_datum(branch.value));
            builder.add(std::move(pair_builder).to_datum());
        });
    return std::move(builder).to_datum();
}

ql::datum_t convert_debug_branch_birth_certificate_to_datum(
        const branch_birth_certificate_t &birth_certificate) {
    ql::datum_object_builder_t builder;
    builder.overwrite("initial_timestamp", ql::datum_t(static_cast<double>(
        birth_certificate.initial_timestamp.to_repli_timestamp().longtime)));
    builder.overwrite(
        "origin", convert_debug_version_to_datum(birth_certificate.origin));
    return std::move(builder).to_datum();
}

ql::datum_t convert_debug_branch_history_to_datum(
        const branch_history_t &branch_history) {
    ql::datum_array_builder_t builder(ql::configured_limits_t::unlimited);
    for (const auto &branch : branch_history.branches) {
        ql::datum_object_builder_t branch_builder;
        branch_builder.overwrite("branch", convert_uuid_to_datum(branch.first.value));
        branch_builder.overwrite(
            "branch_birth_certificate",
            convert_debug_branch_birth_certificate_to_datum(branch.second));
        builder.add(std::move(branch_builder).to_datum());
    }
    return std::move(builder).to_datum();
}

ql::datum_t convert_debug_contract_acks_to_datum(
        const std::map<contract_id_t, contract_ack_t> &contract_acks) {
    ql::datum_array_builder_t builder(ql::configured_limits_t::unlimited);
    for (const auto &contract_ack : contract_acks) {
        ql::datum_object_builder_t contract_builder;
        contract_builder.overwrite(
            "contract", convert_uuid_to_datum(contract_ack.first.value));
        contract_builder.overwrite(
            "state",
            convert_debug_contract_ack_state_to_datum(contract_ack.second.state));
        contract_builder.overwrite(
            "version",
            static_cast<bool>(contract_ack.second.version)
                ? convert_debug_version_map_to_datum(
                        contract_ack.second.version.get())
                : ql::datum_t::null());
        contract_builder.overwrite(
            "branch_history",
            convert_debug_branch_history_to_datum(contract_ack.second.branch_history));
        builder.add(std::move(contract_builder).to_datum());
    }
    return std::move(builder).to_datum();
}

ql::datum_t convert_debug_statuses_to_datum(
        const std::map<server_id_t, table_status_response_t> &statuses) {
    ql::datum_array_builder_t builder(ql::configured_limits_t::unlimited);
    for (const auto &peer : statuses) {
        ql::datum_object_builder_t peer_builder;
        peer_builder.overwrite("server", convert_uuid_to_datum(peer.first.get_uuid()));
        peer_builder.overwrite("timestamp",
            convert_debug_multi_table_manager_bcard_timestamp_to_datum(
                *peer.second.raft_state_timestamp));
        peer_builder.overwrite("contracts",
            convert_debug_contracts_to_datum(peer.second.raft_state->contracts));
        peer_builder.overwrite("contract_acks",
            convert_debug_contract_acks_to_datum(peer.second.contract_acks));
        peer_builder.overwrite("current_branches",
             convert_debug_current_branches_to_datum(
                peer.second.raft_state->current_branches));
        builder.add(std::move(peer_builder).to_datum());
    }
    return std::move(builder).to_datum();
}

void debug_table_status_artificial_table_backend_t::format_row(
        auth::user_context_t const &user_context,
        const namespace_id_t &table_id,
        const table_config_and_shards_t &config_and_shards,
        const ql::datum_t &db_name_or_uuid,
        const signal_t *interruptor_on_home,
        ql::datum_t *row_out)
        THROWS_ONLY(
            interrupted_exc_t,
            no_such_table_exc_t,
            failed_table_op_exc_t,
            auth::permission_error_t) {
    assert_thread();

    user_context.require_admin_user();

    std::map<server_id_t, table_status_response_t> statuses;
    table_meta_client->get_debug_status(
        table_id, all_replicas_ready_mode_t::INCLUDE_RAFT_TEST, interruptor_on_home,
        &statuses);

    ql::datum_object_builder_t builder;
    builder.overwrite("id", convert_uuid_to_datum(table_id.value));
    builder.overwrite("name",
        convert_name_to_datum(config_and_shards.config.basic.name));
    builder.overwrite("db", db_name_or_uuid);
    builder.overwrite(
        "config",
        convert_table_config_to_datum(
            table_id,
            db_name_or_uuid,
            config_and_shards.config,
            admin_identifier_format_t::uuid));
    builder.overwrite(
        "shard_scheme",
        convert_debug_table_shard_scheme_to_datum());
    builder.overwrite(
        "table_server_status",
        convert_debug_statuses_to_datum(statuses));
    *row_out = std::move(builder).to_datum();
}
