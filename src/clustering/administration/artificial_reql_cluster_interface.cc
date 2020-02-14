// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/administration/artificial_reql_cluster_interface.hpp"

#include "clustering/administration/admin_op_exc.hpp"
#include "clustering/administration/auth/grant.hpp"
#include "clustering/administration/main/watchable_fields.hpp"
#include "clustering/administration/metadata.hpp"
#include "clustering/administration/real_reql_cluster_interface.hpp"
#include "concurrency/cross_thread_signal.hpp"
#include "rdb_protocol/artificial_table/artificial_table.hpp"
#include "rdb_protocol/env.hpp"
#include "fdb/reql_fdb.hpp"
#include "rpc/semilattice/view/field.hpp"

/* static */ const name_string_t artificial_reql_cluster_interface_t::database_name =
    name_string_t::guarantee_valid("rethinkdb");

// Note, can't use the above as order of initialization isn't guaranteed
/* static */ const database_id_t artificial_reql_cluster_interface_t::database_id =
    database_id_t{uuid_u::from_hash(str_to_uuid("39a24924-14ec-4deb-99f1-742eda7aba5e"), "rethinkdb")};

artificial_reql_cluster_interface_t::artificial_reql_cluster_interface_t(
        std::shared_ptr<semilattice_readwrite_view_t<auth_semilattice_metadata_t>>
            auth_semilattice_view) :
    m_auth_semilattice_view(auth_semilattice_view),
    m_next(nullptr) {
}

admin_err_t db_already_exists_error(const name_string_t &db_name) {
    return admin_err_t{
            strprintf("Database `%s` already exists.", db_name.c_str()),
            query_state_t::FAILED};
}

bool artificial_reql_cluster_interface_t::db_config(
        auth::user_context_t const &user_context,
        const counted_t<const ql::db_t> &db,
        ql::backtrace_id_t bt,
        ql::env_t *env,
        scoped_ptr_t<ql::val_t> *selection_out,
        admin_err_t *error_out) {
    if (db->name == artificial_reql_cluster_interface_t::database_name) {
        *error_out = admin_err_t{
            strprintf("Database `%s` is special; you can't configure it.",
                      artificial_reql_cluster_interface_t::database_name.c_str()),
            query_state_t::FAILED};
        return false;
    }
    return next_or_error(error_out) && m_next->db_config(
        user_context, db, bt, env, selection_out, error_out);
}

std::vector<name_string_t> artificial_reql_cluster_interface_t::table_list_sorted() {
    std::vector<name_string_t> ret;
    for (auto&& pair : m_table_backends) {
        if (pair.first.str()[0] == '_') {
            /* If a table's name starts with `_`, don't show it to the user unless
            they explicitly request it. */
            continue;
        }
        ret.push_back(pair.first);
    }
    for (auto&& pair : m_table_fdb_backends) {
        if (pair.first.str()[0] == '_') {
            /* If a table's name starts with `_`, don't show it to the user unless
            they explicitly request it. */
            continue;
        }
        ret.push_back(pair.first);
    }
    std::sort(ret.begin(), ret.end());
    return ret;
}

bool artificial_reql_cluster_interface_t::table_find(
        const name_string_t &name,
        admin_identifier_format_t identifier_format,
        counted_t<base_table_t> *table_out,
        admin_err_t *error_out) {
    auto backend = get_table_backend(name, identifier_format);
    if (backend.first != nullptr) {
        table_out->reset(
            new artificial_table_t(backend.first));
        return true;
    } else if (backend.second != nullptr) {
        table_out->reset(
            new artificial_table_fdb_t(backend.second));
    } else {
        *error_out = table_not_found_error(
                      artificial_reql_cluster_interface_t::database_name, name);
        return false;
    }
}

bool artificial_reql_cluster_interface_t::table_config(
        auth::user_context_t const &user_context,
        counted_t<const ql::db_t> db,
        const name_string_t &name,
        ql::backtrace_id_t bt,
        ql::env_t *env,
        scoped_ptr_t<ql::val_t> *selection_out, admin_err_t *error_out) {
    if (db->name == artificial_reql_cluster_interface_t::database_name) {
        *error_out = admin_err_t{
            strprintf("Database `%s` is special; you can't configure the "
                      "tables in it.", artificial_reql_cluster_interface_t::database_name.c_str()),
            query_state_t::FAILED};
        return false;
    }
    return next_or_error(error_out) && m_next->table_config(
        user_context, db, name, bt, env, selection_out, error_out);
}

bool artificial_reql_cluster_interface_t::table_status(
        counted_t<const ql::db_t> db, const name_string_t &name,
        ql::backtrace_id_t bt, ql::env_t *env,
        scoped_ptr_t<ql::val_t> *selection_out, admin_err_t *error_out) {
    if (db->name == artificial_reql_cluster_interface_t::database_name) {
        *error_out = admin_err_t{
            strprintf("Database `%s` is special; the system tables in it don't "
                      "have meaningful status information.", artificial_reql_cluster_interface_t::database_name.c_str()),
            query_state_t::FAILED};
        return false;
    }
    return next_or_error(error_out) && m_next->table_status(
        db, name, bt, env, selection_out, error_out);
}

void artificial_reql_cluster_interface_t::set_next_reql_cluster_interface(
        reql_cluster_interface_t *next) {
    m_next = next;
}

std::pair<artificial_table_backend_t *, artificial_table_fdb_backend_t *>
artificial_reql_cluster_interface_t::get_table_backend(
        name_string_t const &table_name,
        admin_identifier_format_t admin_identifier_format) const {
    std::pair<artificial_table_backend_t *, artificial_table_fdb_backend_t *> ret
        = {nullptr, nullptr};
    auto table_backend = m_table_backends.find(table_name);
    if (table_backend != m_table_backends.end()) {
        switch (admin_identifier_format) {
        case admin_identifier_format_t::name:
            ret.first = table_backend->second.first;
        case admin_identifier_format_t::uuid:
            ret.first = table_backend->second.second;
        default:
            unreachable();
        }
    } else {
        auto table_fdb_backend = m_table_fdb_backends.find(table_name);
        if (table_fdb_backend != m_table_fdb_backends.end()) {
            switch (admin_identifier_format) {
            case admin_identifier_format_t::name:
                ret.second = table_fdb_backend->second.first;
            case admin_identifier_format_t::uuid:
                ret.second = table_fdb_backend->second.second;
            default:
                unreachable();
            }
        }
    }
    return ret;
}

artificial_reql_cluster_interface_t::table_backends_map_t *
artificial_reql_cluster_interface_t::get_table_backends_map_mutable() {
    return &m_table_backends;
}

artificial_reql_cluster_interface_t::table_backends_map_t const &
artificial_reql_cluster_interface_t::get_table_backends_map() const {
    return m_table_backends;
}

artificial_reql_cluster_interface_t::table_fdb_backends_map_t *
artificial_reql_cluster_interface_t::get_table_fdb_backends_map_mutable() {
    return &m_table_fdb_backends;
}

artificial_reql_cluster_interface_t::table_fdb_backends_map_t const &
artificial_reql_cluster_interface_t::get_table_fdb_backends_map() const {
    return m_table_fdb_backends;
}

bool artificial_reql_cluster_interface_t::next_or_error(admin_err_t *error_out) const {
    if (m_next == nullptr) {
        if (error_out != nullptr) {
            *error_out = admin_err_t{
                "Failed to find an interface.", query_state_t::FAILED};
        }
        return false;
    }
    return true;
}

artificial_reql_cluster_backends_t::artificial_reql_cluster_backends_t(
        artificial_reql_cluster_interface_t *artificial_reql_cluster_interface,
        real_reql_cluster_interface_t *real_reql_cluster_interface,
        std::shared_ptr<semilattice_readwrite_view_t<auth_semilattice_metadata_t>>
            auth_semilattice_view,
        std::shared_ptr<semilattice_readwrite_view_t<cluster_semilattice_metadata_t>>
            cluster_semilattice_view,
        clone_ptr_t<watchable_t<change_tracking_map_t<
            peer_id_t, cluster_directory_metadata_t>>> directory_view,
        watchable_map_t<peer_id_t, cluster_directory_metadata_t> *directory_map_view,
        table_meta_client_t *table_meta_client,
        server_config_client_t *server_config_client,
        mailbox_manager_t *mailbox_manager,
        rdb_context_t *rdb_context,
        lifetime_t<name_resolver_t const &> name_resolver) {
    for (int format = 0; format < 2; ++format) {
        permissions_backend[format].init(
            new auth::permissions_artificial_table_backend_t(
                name_resolver,
                auth_semilattice_view,
                cluster_semilattice_view,
                static_cast<admin_identifier_format_t>(format)));
    }
    permissions_sentry = backend_sentry_t(
        artificial_reql_cluster_interface->get_table_backends_map_mutable(),
        name_string_t::guarantee_valid("permissions"),
        std::make_pair(permissions_backend[0].get(), permissions_backend[1].get()));

    users_backend.init(
        new auth::users_artificial_table_backend_t(
            name_resolver,
            auth_semilattice_view,
            cluster_semilattice_view));
    users_sentry = backend_sentry_t(
        artificial_reql_cluster_interface->get_table_backends_map_mutable(),
        name_string_t::guarantee_valid("users"),
        std::make_pair(users_backend.get(), users_backend.get()));

    cluster_config_backend.init(
        new cluster_config_artificial_table_backend_t(name_resolver));
    cluster_config_sentry = backend_sentry_t(
        artificial_reql_cluster_interface->get_table_backends_map_mutable(),
        name_string_t::guarantee_valid("cluster_config"),
        std::make_pair(cluster_config_backend.get(), cluster_config_backend.get()));

    db_config_backend.init(
        new db_config_artificial_table_backend_t(
            rdb_context,
            name_resolver,
            metadata_field(
                &cluster_semilattice_metadata_t::databases,
                cluster_semilattice_view),
            real_reql_cluster_interface));
    db_config_sentry = backend_sentry_t(
        artificial_reql_cluster_interface->get_table_backends_map_mutable(),
        name_string_t::guarantee_valid("db_config"),
        std::make_pair(db_config_backend.get(), db_config_backend.get()));

    for (int format = 0; format < 2; ++format) {
        issues_backend[format].init(
            new issues_artificial_table_backend_t(
                name_resolver,
                mailbox_manager,
                cluster_semilattice_view,
                directory_map_view,
                server_config_client,
                table_meta_client,
                static_cast<admin_identifier_format_t>(format)));
    }
    issues_sentry = backend_sentry_t(
        artificial_reql_cluster_interface->get_table_backends_map_mutable(),
        name_string_t::guarantee_valid("current_issues"),
        std::make_pair(issues_backend[0].get(), issues_backend[1].get()));

    for (int format = 0; format < 2; ++format) {
        logs_backend[format].init(
            new logs_artificial_table_backend_t(
                name_resolver,
                mailbox_manager,
                directory_map_view,
                server_config_client,
                static_cast<admin_identifier_format_t>(format)));
    }
    logs_sentry = backend_sentry_t(
        artificial_reql_cluster_interface->get_table_backends_map_mutable(),
        name_string_t::guarantee_valid("logs"),
        std::make_pair(logs_backend[0].get(), logs_backend[1].get()));

    server_config_backend.init(
        new server_config_artificial_table_backend_t(
            name_resolver,
            directory_map_view,
            server_config_client));
    server_config_sentry = backend_sentry_t(
        artificial_reql_cluster_interface->get_table_backends_map_mutable(),
        name_string_t::guarantee_valid("server_config"),
        std::make_pair(server_config_backend.get(), server_config_backend.get()));

    for (int format = 0; format < 2; ++format) {
        server_status_backend[format].init(
            new server_status_artificial_table_backend_t(
                name_resolver,
                directory_map_view,
                server_config_client,
                static_cast<admin_identifier_format_t>(format)));
    }
    server_status_sentry = backend_sentry_t(
        artificial_reql_cluster_interface->get_table_backends_map_mutable(),
        name_string_t::guarantee_valid("server_status"),
        std::make_pair(server_status_backend[0].get(), server_status_backend[1].get()));

    for (int format = 0; format < 2; ++format) {
        stats_backend[format].init(
            new stats_artificial_table_backend_t(
                name_resolver,
                directory_view,
                cluster_semilattice_view,
                server_config_client,
                table_meta_client,
                mailbox_manager,
                static_cast<admin_identifier_format_t>(format)));
    }
    stats_sentry = backend_sentry_t(
        artificial_reql_cluster_interface->get_table_backends_map_mutable(),
        name_string_t::guarantee_valid("stats"),
        std::make_pair(stats_backend[0].get(), stats_backend[1].get()));

    for (int format = 0; format < 2; ++format) {
        table_config_backend[format].init(
            new table_config_artificial_table_backend_t(
                rdb_context,
                name_resolver,
                cluster_semilattice_view,
                static_cast<admin_identifier_format_t>(format),
                server_config_client,
                table_meta_client));
    }
    table_config_sentry = backend_sentry_t(
        artificial_reql_cluster_interface->get_table_backends_map_mutable(),
        name_string_t::guarantee_valid("table_config"),
        std::make_pair(table_config_backend[0].get(), table_config_backend[1].get()));

    for (int format = 0; format < 2; ++format) {
        table_status_backend[format].init(
            new table_status_artificial_table_backend_t(
                name_resolver,
                cluster_semilattice_view,
                server_config_client,
                table_meta_client,
                static_cast<admin_identifier_format_t>(format)));
    }
    table_status_sentry = backend_sentry_t(
        artificial_reql_cluster_interface->get_table_backends_map_mutable(),
        name_string_t::guarantee_valid("table_status"),
        std::make_pair(table_status_backend[0].get(), table_status_backend[1].get()));

    for (int format = 0; format < 2; ++format) {
        jobs_backend[format].init(
            new jobs_artificial_table_fdb_backend_t(
                static_cast<admin_identifier_format_t>(format)));
    }
    jobs_sentry = fdb_backend_sentry_t(
        artificial_reql_cluster_interface->get_table_fdb_backends_map_mutable(),
        name_string_t::guarantee_valid("jobs"),
        std::make_pair(jobs_backend[0].get(), jobs_backend[1].get()));

    debug_scratch_backend.init(
        new in_memory_artificial_table_backend_t(
            name_string_t::guarantee_valid("_debug_scratch"),
            name_resolver));
    debug_scratch_sentry = backend_sentry_t(
        artificial_reql_cluster_interface->get_table_backends_map_mutable(),
        name_string_t::guarantee_valid("_debug_scratch"),
        std::make_pair(debug_scratch_backend.get(), debug_scratch_backend.get()));

    debug_stats_backend.init(
        new debug_stats_artificial_table_backend_t(
            name_resolver,
            directory_map_view,
            server_config_client,
            mailbox_manager));
    debug_stats_sentry = backend_sentry_t(
        artificial_reql_cluster_interface->get_table_backends_map_mutable(),
        name_string_t::guarantee_valid("_debug_stats"),
        std::make_pair(debug_stats_backend.get(), debug_stats_backend.get()));

    debug_table_status_backend.init(
        new debug_table_status_artificial_table_backend_t(
            name_resolver,
            cluster_semilattice_view,
            table_meta_client));
    debug_table_status_sentry = backend_sentry_t(
        artificial_reql_cluster_interface->get_table_backends_map_mutable(),
        name_string_t::guarantee_valid("_debug_table_status"),
        std::make_pair(debug_table_status_backend.get(), debug_table_status_backend.get()));
}
