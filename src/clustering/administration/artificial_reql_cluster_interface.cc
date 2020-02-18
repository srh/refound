// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/administration/artificial_reql_cluster_interface.hpp"

#include "clustering/administration/admin_op_exc.hpp"
#include "clustering/administration/auth/permissions_artificial_table_backend.hpp"
#include "clustering/administration/auth/users_artificial_table_backend.hpp"
#include "clustering/administration/jobs/jobs_backend.hpp"
#include "clustering/administration/tables/db_config.hpp"
#include "clustering/administration/tables/table_config.hpp"
#include "rdb_protocol/artificial_table/artificial_table.hpp"
#include "rdb_protocol/artificial_table/in_memory.hpp"
#include "rdb_protocol/env.hpp"
#include "fdb/reql_fdb.hpp"


constexpr const char *SYSTEM_DB_NAME = "rethinkdb";

const name_string_t artificial_reql_cluster_interface_t::database_name =
    name_string_t::guarantee_valid(SYSTEM_DB_NAME);

const database_id_t artificial_reql_cluster_interface_t::database_id =
    database_id_t{uuid_u::from_hash(str_to_uuid("39a24924-14ec-4deb-99f1-742eda7aba5e"), SYSTEM_DB_NAME)};

artificial_reql_cluster_interface_t::artificial_reql_cluster_interface_t() :
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
    auto backend = get_table_backend_or_null(name, identifier_format);
    if (backend != nullptr) {
        table_out->reset(new artificial_table_fdb_t(backend));
        return true;
    } else {
        *error_out = table_not_found_error(
                      artificial_reql_cluster_interface_t::database_name, name);
        return false;
    }
}

bool artificial_reql_cluster_interface_t::table_config(
        auth::user_context_t const &user_context,
        counted_t<const ql::db_t> db,
        config_version_checker cv_checker,
        const namespace_id_t &table_id,
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
        user_context, db, cv_checker, table_id, name, bt, env, selection_out, error_out);
}

void artificial_reql_cluster_interface_t::set_next_reql_cluster_interface(
        reql_cluster_interface_t *next) {
    m_next = next;
}

artificial_table_fdb_backend_t *
artificial_reql_cluster_interface_t::get_table_backend_or_null(
        name_string_t const &table_name,
        admin_identifier_format_t admin_identifier_format) const {
    auto table_fdb_backend = m_table_fdb_backends.find(table_name);
    if (table_fdb_backend != m_table_fdb_backends.end()) {
        switch (admin_identifier_format) {
        case admin_identifier_format_t::name:
            return table_fdb_backend->second.first;
        case admin_identifier_format_t::uuid:
            return table_fdb_backend->second.second;
        default:
            unreachable();
        }
    }
    return nullptr;
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

artificial_reql_cluster_backends_t::~artificial_reql_cluster_backends_t() { }

artificial_reql_cluster_backends_t::artificial_reql_cluster_backends_t(
        artificial_reql_cluster_interface_t *artificial_reql_cluster_interface) {
    for (int format = 0; format < 2; ++format) {
        permissions_backend[format].init(
            new auth::permissions_artificial_table_fdb_backend_t(
                static_cast<admin_identifier_format_t>(format)));
    }
    permissions_sentry = fdb_backend_sentry_t(
        artificial_reql_cluster_interface->get_table_fdb_backends_map_mutable(),
        name_string_t::guarantee_valid("permissions"),
        std::make_pair(permissions_backend[0].get(), permissions_backend[1].get()));

    users_backend.init(new auth::users_artificial_table_fdb_backend_t());
    users_sentry = fdb_backend_sentry_t(
        artificial_reql_cluster_interface->get_table_fdb_backends_map_mutable(),
        name_string_t::guarantee_valid("users"),
        std::make_pair(users_backend.get(), users_backend.get()));

    db_config_backend.init(
        new db_config_artificial_table_fdb_backend_t());
    db_config_sentry = fdb_backend_sentry_t(
        artificial_reql_cluster_interface->get_table_fdb_backends_map_mutable(),
        name_string_t::guarantee_valid("db_config"),
        std::make_pair(db_config_backend.get(), db_config_backend.get()));

    for (int format = 0; format < 2; ++format) {
        table_config_backend[format].init(
            new table_config_artificial_table_fdb_backend_t(
                static_cast<admin_identifier_format_t>(format)));
    }
    table_config_sentry = fdb_backend_sentry_t(
        artificial_reql_cluster_interface->get_table_fdb_backends_map_mutable(),
        name_string_t::guarantee_valid("table_config"),
        std::make_pair(table_config_backend[0].get(), table_config_backend[1].get()));

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
        new in_memory_artificial_table_fdb_backend_t(
            name_string_t::guarantee_valid("_debug_scratch")));
    debug_scratch_sentry = fdb_backend_sentry_t(
        artificial_reql_cluster_interface->get_table_fdb_backends_map_mutable(),
        name_string_t::guarantee_valid("_debug_scratch"),
        std::make_pair(debug_scratch_backend.get(), debug_scratch_backend.get()));
}
