// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/artificial_reql_cluster_interface.hpp"

#include "clustering/admin_op_exc.hpp"
#include "clustering/auth/permissions_artificial_table_backend.hpp"
#include "clustering/auth/users_artificial_table_backend.hpp"
#include "clustering/jobs/jobs_backend.hpp"
#include "clustering/tables/db_config.hpp"
#include "clustering/tables/table_config.hpp"
#include "rdb_protocol/artificial_table/artificial_table.hpp"
#include "rdb_protocol/artificial_table/in_memory.hpp"
#include "rdb_protocol/env.hpp"
#include "fdb/reql_fdb.hpp"


constexpr const char *SYSTEM_DB_NAME = "rethinkdb";

const name_string_t artificial_reql_cluster_interface_t::database_name =
    name_string_t::guarantee_valid(SYSTEM_DB_NAME);

const database_id_t artificial_reql_cluster_interface_t::database_id =
    database_id_t{uuid_u::from_hash(str_to_uuid("39a24924-14ec-4deb-99f1-742eda7aba5e"), SYSTEM_DB_NAME)};

admin_err_t db_already_exists_error(const name_string_t &db_name) {
    return admin_err_t{
            strprintf("Database `%s` already exists.", db_name.c_str()),
            query_state_t::FAILED};
}

std::vector<name_string_t> artificial_reql_cluster_interface_t::table_list_sorted() {
    // Keep in sync with table_find.
    return {
        name_string_t::guarantee_valid("db_config"),
        name_string_t::guarantee_valid("jobs"),
        name_string_t::guarantee_valid("permissions"),
        name_string_t::guarantee_valid("table_config"),
        name_string_t::guarantee_valid("users"),
        // Nope -- we don't show '_'-prefixed system tables to the user unless they explicitly request it.
        // name_string_t::guarantee_valid("_debug_scratch"),
    };
}

bool artificial_reql_cluster_interface_t::table_find(
        const name_string_t &name,
        admin_identifier_format_t identifier_format,
        counted_t<const base_table_t> *table_out,
        admin_err_t *error_out) {
    auto backend = get_table_backend_or_null(name, identifier_format);
    if (backend != nullptr) {
        table_out->reset(new artificial_table_fdb_t(backend));
        return true;
    } else {
        *error_out = table_dne_error(
                      artificial_reql_cluster_interface_t::database_name, name);
        return false;
    }
}

artificial_table_fdb_backend_t *
artificial_reql_cluster_interface_t::get_table_backend_or_null(
        name_string_t const &table_name,
        admin_identifier_format_t admin_identifier_format) const {
    // Keep in sync with table_list_sorted.

    if (table_name.str() < "q") {    // Much performant.
        if (table_name.str() == "db_config") {
            return m_backends.db_config_backend.get();
        }
        if (table_name.str() == "jobs") {
            return m_backends.jobs_backend[static_cast<int>(admin_identifier_format)].get();
        }
        if (table_name.str() == "permissions") {
            return m_backends.permissions_backend[static_cast<int>(admin_identifier_format)].get();
        }
    } else {
        if (table_name.str() == "table_config") {
            return m_backends.table_config_backend[static_cast<int>(admin_identifier_format)].get();
        }
        if (table_name.str() == "users") {
            return m_backends.users_backend.get();
        }
        if (table_name.str() == "_debug_scratch") {
            return m_backends.debug_scratch_backend.get();
        }
    }
    return nullptr;
}

optional<namespace_id_t> artificial_reql_cluster_interface_t::get_table_id(
        const name_string_t &table_name) {
    // Keep in sync with table_list_sorted.

    bool found = false;
    if (table_name.str() < "q") {    // Much performant.
        if (table_name.str() == "db_config") {
            found = true;
        } else if (table_name.str() == "jobs") {
            found = true;
        } else if (table_name.str() == "permissions") {
            found = true;
        }
    } else {
        if (table_name.str() == "table_config") {
            found = true;
        } else if (table_name.str() == "users") {
            found = true;
        } else if (table_name.str() == "_debug_scratch") {
            found = true;
        }
    }

    optional<namespace_id_t> ret;
    if (found) {
        ret.set(artificial_table_fdb_backend_t::compute_artificial_table_id(table_name));
    }
    return ret;
}

artificial_reql_cluster_backends_t::~artificial_reql_cluster_backends_t() { }

artificial_reql_cluster_backends_t::artificial_reql_cluster_backends_t() {
    db_config_backend.init(
        new db_config_artificial_table_fdb_backend_t());

    for (int format = 0; format < 2; ++format) {
        jobs_backend[format].init(
            new jobs_artificial_table_fdb_backend_t(
                static_cast<admin_identifier_format_t>(format)));
    }

    for (int format = 0; format < 2; ++format) {
        permissions_backend[format].init(
            new auth::permissions_artificial_table_fdb_backend_t(
                static_cast<admin_identifier_format_t>(format)));
    }

    for (int format = 0; format < 2; ++format) {
        table_config_backend[format].init(
            new table_config_artificial_table_fdb_backend_t(
                static_cast<admin_identifier_format_t>(format)));
    }

    users_backend.init(new auth::users_artificial_table_fdb_backend_t());

    debug_scratch_backend.init(
        new in_memory_artificial_table_fdb_backend_t(
            name_string_t::guarantee_valid("_debug_scratch")));
}
