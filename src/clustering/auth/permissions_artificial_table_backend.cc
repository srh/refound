// iopyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/auth/permissions_artificial_table_backend.hpp"

#include "clustering/artificial_reql_cluster_interface.hpp"
#include "clustering/auth/user.hpp"
#include "clustering/auth/user_fut.hpp"
#include "clustering/auth/username.hpp"
#include "clustering/datum_adapter.hpp"
#include "clustering/tables/table_metadata.hpp"
#include "containers/lifetime.hpp"
#include "fdb/index.hpp"
#include "fdb/retry_loop.hpp"
#include "fdb/system_tables.hpp"

namespace auth {

permissions_artificial_table_fdb_backend_t::permissions_artificial_table_fdb_backend_t(
        admin_identifier_format_t identifier_format)
    : artificial_table_fdb_backend_t(
        name_string_t::guarantee_valid("permissions")),
      m_identifier_format(identifier_format) {
}

std::unordered_map<database_id_t, optional<name_string_t>>
make_db_name_cache() {
    std::unordered_map<database_id_t, optional<name_string_t>> ret;
    ret.emplace(artificial_reql_cluster_interface_t::database_id,
        make_optional(artificial_reql_cluster_interface_t::database_name));
    return ret;
}

struct lookup_db_state {
    database_id_t database_id;
    fdb_value_fut<name_string_t> fut;
    // Valid only if fut is empty.
    optional<name_string_t> cached_result;
};

lookup_db_state start_lookup_db(
        FDBTransaction *txn,
        std::unordered_map<database_id_t, optional<name_string_t>> *db_name_cache,
        const database_id_t &database_id) {
    lookup_db_state ret;
    ret.database_id = database_id;
    auto cached = db_name_cache->find(database_id);
    if (cached != db_name_cache->end()) {
        ret.cached_result = cached->second;
    } else {
        ret.fut = transaction_lookup_uq_index<db_config_by_id>(txn, database_id);
    }
    return ret;
}

optional<name_string_t> finish_lookup_db(
        const signal_t *interruptor,
        std::unordered_map<database_id_t, optional<name_string_t>> *db_name_cache,
        lookup_db_state &&state) {
    if (state.fut.has()) {
        optional<name_string_t> optname;
        name_string_t name;
        if (state.fut.block_and_deserialize(interruptor, &name)) {
            optname.set(std::move(name));
        }
        db_name_cache->emplace(state.database_id, optname);
        return optname;
    } else {
        return std::move(state.cached_result);
    }
}

optional<name_string_t> lookup_db(
        FDBTransaction *txn,
        const signal_t *interruptor,
        std::unordered_map<database_id_t, optional<name_string_t>> *db_name_cache,
        const database_id_t &database_id) {
    optional<name_string_t> database_name;
    auto cached = db_name_cache->find(database_id);
    if (cached != db_name_cache->end()) {
        database_name = cached->second;
    } else {
        fdb_value_fut<name_string_t> fut = transaction_lookup_uq_index<db_config_by_id>(txn, database_id);
        name_string_t name;
        if (fut.block_and_deserialize(interruptor, &name)) {
            database_name.set(std::move(name));
        }
        db_name_cache->emplace(database_id, database_name);
    }
    return database_name;
}


bool permissions_artificial_table_fdb_backend_t::read_all_rows_as_vector(
        FDBDatabase *fdb,
        auth::user_context_t const &user_context,
        const signal_t *interruptor,
        std::vector<ql::datum_t> *rows_out,
        UNUSED admin_err_t *error_out) {
    std::vector<ql::datum_t> rows_from_db;
    fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor, [&](FDBTransaction *txn) {
        auth::fdb_user_fut<auth::read_permission> user_fut = get_read_permission(txn, user_context);
        user_fut.block_and_check(interruptor);
        std::vector<ql::datum_t> rows;
        std::string prefix = users_by_username::prefix;
        transaction_read_whole_range_coro(txn, prefix, prefix_end(prefix), interruptor,
                [&](const FDBKeyValue &kv) {
            // Only used for admin_identifier_format_t::name, btw.
            std::unordered_map<database_id_t, optional<name_string_t>> db_name_cache = make_db_name_cache();

            key_view whole_key{void_as_uint8(kv.key), kv.key_length};
            key_view username_key = whole_key.guarantee_without_prefix(prefix);
            username_t username = users_by_username::parse_ukey(username_key);
            user_t user;
            deserialize_off_fdb(void_as_uint8(kv.value), kv.value_length, &user);
            {
                ql::datum_t row;
                if (global_to_datum(
                        username,
                        user.get_global_permissions(),
                        &row)) {
                    rows.push_back(std::move(row));
                }
            }

            for (const auto &db_perm : user.get_database_permissions()) {
                ql::datum_t row;
                if (database_to_datum(
                        txn,
                        interruptor,
                        &db_name_cache,
                        username,
                        db_perm.first,
                        db_perm.second,
                        &row)) {
                    rows.push_back(std::move(row));
                }
            }

            const std::map<namespace_id_t, permissions_t> &table_perm_map = user.get_table_permissions();
            std::vector<std::pair<namespace_id_t, permissions_t>> perm_vec(table_perm_map.begin(), table_perm_map.end());

            // TODO: We're doing a separate request per user -- we might re-retrieve the same table many times.  Does fdb client cache it?
            std::vector<fdb_value_fut<table_config_t>> config_futs;
            size_t count = table_perm_map.size();
            config_futs.reserve(count);
            for (auto const &table_perm : perm_vec) {
                config_futs.push_back(transaction_lookup_uq_index<table_config_by_id>(txn, table_perm.first));
            }

            std::vector<optional<table_config_t>> configs;
            for (size_t i = 0; i < count; ++i) {
                table_config_t config;
                if (!config_futs[i].block_and_deserialize(interruptor, &config)) {
                    configs.push_back(r_nullopt);
                    // Table not found... skip.
                    continue;
                }
                configs.emplace_back(std::move(config));
            }

            std::unordered_map<database_id_t, lookup_db_state> db_lookup_states;
            for (const auto& config : configs) {
                if (!config.has_value()) {
                    continue;
                }
                if (db_lookup_states.find(config->basic.database) != db_lookup_states.end()) {
                    continue;
                }
                db_lookup_states.emplace(config->basic.database, start_lookup_db(txn, &db_name_cache, config->basic.database));
            }

            std::unordered_map<database_id_t, optional<name_string_t>> db_lookup_results;
            for (auto& lookup_state : db_lookup_states) {
                optional<name_string_t> res = finish_lookup_db(interruptor, &db_name_cache, std::move(lookup_state.second));
                db_lookup_results.emplace(lookup_state.first, std::move(res));
            }


            for (size_t i = 0; i < count; ++i) {
                if (!configs[i].has_value()) {
                    continue;
                }
                ql::datum_t row;
                if (table_to_datum(
                        username,
                        configs[i]->basic.database,
                        db_lookup_results.at(configs[i]->basic.database),
                        perm_vec[i].first,
                        configs[i]->basic,
                        perm_vec[i].second,
                        &row)) {
                    rows.push_back(std::move(row));
                }
            }

            return true;
        });

        rows_from_db = std::move(rows);
    });
    if (set_fdb_error(loop_err, error_out, "Error reading `rethinkdb.permissions` table")) {
        return false;
    }

    // We build result with the admin user being the first result -- in the same order
    // as pre-fdb.
    std::vector<ql::datum_t> result;
    result.reserve(2 + rows_from_db.size());

    // The "admin" user is faked here
    {
        ql::datum_t row;
        if (global_to_datum(
                username_t("admin"),
                permissions_t(tribool::True, tribool::True, tribool::True, tribool::True),
                &row)) {
            result.push_back(std::move(row));
        }
    }

    {
        std::unordered_map<database_id_t, optional<name_string_t>> db_name_cache
            = make_db_name_cache();
        ql::datum_t row;
        if (database_to_datum(
                static_cast<FDBTransaction *>(nullptr),
                interruptor,
                &db_name_cache,
                username_t("admin"),
                artificial_reql_cluster_interface_t::database_id,
                permissions_t(tribool::True, tribool::True, tribool::True),
                &row)) {
            result.push_back(std::move(row));
        }
    }

    // TODO: Should we sort the rows?  "admin" isn't the smallest possible username.
    // We didn't pre-fdb.

    std::move(rows_from_db.begin(), rows_from_db.end(), std::back_inserter(result));

    *rows_out = std::move(result);
    return true;
}

bool permissions_artificial_table_fdb_backend_t::read_row(
        FDBTransaction *txn,
        UNUSED auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        const signal_t *interruptor,
        ql::datum_t *row_out,
        UNUSED admin_err_t *error_out) {
    *row_out = ql::datum_t();

    username_t username;
    database_id_t database_id;
    name_string_t database_name;
    namespace_id_t table_id;
    table_config_t table_config;
    uint8_t array_size = parse_primary_key(
        interruptor,
        txn,
        primary_key,
        &username,
        &database_id,
        &database_name,
        &table_id,
        &table_config);
    if (array_size == 0) {
        return true;
    }

    if (username.is_admin()) {
        switch (array_size) {
            case 1:
                global_to_datum(
                    username,
                    permissions_t(tribool::True, tribool::True, tribool::True, tribool::True),
                    row_out);
                break;
            case 2:
                if (database_id == artificial_reql_cluster_interface_t::database_id) {
                    std::unordered_map<database_id_t, optional<name_string_t>> db_name_cache
                        = make_db_name_cache();

                    database_to_datum(
                        static_cast<FDBTransaction *>(nullptr),  // it uses the cache.
                        interruptor,
                        &db_name_cache,
                        username,
                        database_id,
                        permissions_t(tribool::True, tribool::True, tribool::True),
                        row_out);
                }
                break;
        }
        return true;
    }

    // We could put this request in the first round-trip, with logic to handle the admin
    // user case... but who cares.
    fdb_value_fut<auth::user_t> user_fut = transaction_get_user(txn, username);
    user_t user;
    if (!user_fut.block_and_deserialize(interruptor, &user)) {
        return true;
    }

    // Note these functions will only set `row_out` on success.
    switch (array_size) {
        case 1:
            global_to_datum(
                username,
                user.get_global_permissions(),
                row_out);
            break;
        case 2: {
            std::unordered_map<database_id_t, optional<name_string_t>> db_name_cache
                = make_db_name_cache();
            database_to_datum(
                txn,
                interruptor,
                &db_name_cache,
                username,
                database_id,
                user.get_database_permissions(database_id),
                row_out);
        } break;
        case 3: {
            std::unordered_map<database_id_t, optional<name_string_t>> db_name_cache
                = make_db_name_cache();
            optional<name_string_t> db_name = lookup_db(txn, interruptor, &db_name_cache, database_id);
            table_to_datum(
                username,
                database_id,
                db_name,
                table_id,
                table_config.basic,
                user.get_table_permissions(table_id),
                row_out);
        } break;
    }

    return true;
}

bool permissions_artificial_table_fdb_backend_t::write_row(
        FDBTransaction *txn,
        UNUSED auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        bool pkey_was_autogenerated,
        ql::datum_t *new_value_inout,
        const signal_t *interruptor,
        admin_err_t *error_out) {
    if (pkey_was_autogenerated) {
        *error_out = admin_err_t{
            "You must specify a primary key.", query_state_t::FAILED};
        return false;
    }

    username_t username_primary;
    database_id_t database_id_primary;
    name_string_t database_name_primary;
    namespace_id_t table_id_primary;
    table_config_t table_config_primary;
    uint8_t array_size = parse_primary_key(
        interruptor,
        txn,
        primary_key,
        &username_primary,
        &database_id_primary,
        &database_name_primary,
        &table_id_primary,
        &table_config_primary,
        error_out);
    if (array_size == 0) {
        return false;
    }

    if (username_primary.is_admin()) {
        *error_out = admin_err_t{
            "The permissions of the user `" + username_primary.to_string() +
            "` can't be modified.",
            query_state_t::FAILED};
        return false;
    }

    fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);
    fdb_value_fut<auth::user_t> user_fut = transaction_get_user(txn, username_primary);
    user_t user;
    if (!user_fut.block_and_deserialize(interruptor, &user)) {
        *error_out = admin_err_t{
            "No user named `" + username_primary.to_string() + "`.",
            query_state_t::FAILED};
        return false;
    }

    // Used to check whether we really need to write and update the config_version.
    user_t old_user = user;

    if (new_value_inout->has()) {
        std::set<std::string> keys;
        for (size_t i = 0; i < new_value_inout->obj_size(); ++i) {
            keys.insert(new_value_inout->get_pair(i).first.to_std());
        }
        keys.erase("id");

        ql::datum_t username = new_value_inout->get_field("user", ql::NOTHROW);
        if (username.has()) {
            keys.erase("user");
            if (username_t(username.as_str().to_std()) != username_primary) {
                *error_out = admin_err_t{
                    "The key `user` does not match the primary key.",
                    query_state_t::FAILED};
                return false;
            }
        }

        if (array_size > 1) {
            ql::datum_t database = new_value_inout->get_field("database", ql::NOTHROW);
            if (database.has()) {
                keys.erase("database");

                switch (m_identifier_format) {
                    case admin_identifier_format_t::name:
                        {
                            name_string_t database_name;
                            if (!convert_name_from_datum(
                                    database,
                                    "database name",
                                    &database_name,
                                    error_out)) {
                                return false;
                            }

                            if (database_name_primary != database_name) {
                                *error_out = admin_err_t{
                                    "The key `database` does not match the primary key.",
                                    query_state_t::FAILED};
                                return false;
                            }
                        }
                        break;
                    case admin_identifier_format_t::uuid:
                        {
                            database_id_t database_id_secondary;
                            if (!convert_uuid_from_datum(
                                    database, &database_id_secondary.value, error_out)) {
                                return false;
                            }

                            if (database_id_primary != database_id_secondary) {
                                *error_out = admin_err_t{
                                    "The key `database` does not match the primary key.",
                                    query_state_t::FAILED};
                                return false;
                            }
                        }
                        break;
                }
            }
        }

        if (array_size > 2) {
            ql::datum_t table = new_value_inout->get_field("table", ql::NOTHROW);
            if (table.has()) {
                keys.erase("table");

                switch (m_identifier_format) {
                    case admin_identifier_format_t::name:
                        {
                            name_string_t table_name;
                            if (!convert_name_from_datum(
                                    table,
                                    "table name",
                                    &table_name,
                                    error_out)) {
                                return false;
                            }

                            if (table_config_primary.basic.name != table_name) {
                                *error_out = admin_err_t{
                                    "The key `table` does not match the primary key.",
                                    query_state_t::FAILED};
                                return false;
                            }
                        }
                        break;
                    case admin_identifier_format_t::uuid:
                        {
                            namespace_id_t table_id_secondary;
                            if (!convert_uuid_from_datum(
                                    table, &table_id_secondary.value, error_out)) {
                                return false;
                            }

                            if (table_id_primary != table_id_secondary) {
                                *error_out = admin_err_t{
                                    "The key `table` does not match the primary key.",
                                    query_state_t::FAILED};
                                return false;
                            }
                        }
                        break;
                }
            }
        }

        bool is_indeterminate = false;

        ql::datum_t permissions = new_value_inout->get_field("permissions", ql::NOTHROW);
        if (permissions.has()) {
            keys.erase("permissions");

            try {
                switch (array_size) {
                    case 1:
                        {
                            auto perm =
                                user.get_global_permissions();

                            perm.merge(permissions);
                            is_indeterminate = perm.is_indeterminate();
                            user.set_global_permissions(perm);
                        }
                        break;
                    case 2:
                        {
                            auto perm =
                                user.get_database_permissions(database_id_primary);
                            perm.merge(permissions);
                            is_indeterminate = perm.is_indeterminate();
                            user.set_database_permissions(database_id_primary, perm);
                        }
                        break;
                    case 3:
                        {
                            auto perm = user.get_table_permissions(table_id_primary);
                            perm.merge(permissions);
                            is_indeterminate = perm.is_indeterminate();
                            user.set_table_permissions(table_id_primary, perm);
                        }
                        break;
                }
            } catch (admin_op_exc_t const &admin_op_exc) {
                *error_out = admin_op_exc.to_admin_err();
                return false;
            }
        } else {
            *error_out = admin_err_t{
                "Expected a field `permissions`.", query_state_t::FAILED};
            return false;
        }

        if (!keys.empty()) {
            *error_out = admin_err_t{
                "Unexpected key(s) `" + string_join(keys, "`, `") + "`.",
                query_state_t::FAILED};
            return false;
        }

        // Updating the permissions to indeterminate is considered equal to a deletion
        if (is_indeterminate) {
            *new_value_inout = ql::datum_t();
        }
    } else {
        switch (array_size) {
        case 1:
            user.set_global_permissions(
                permissions_t(
                    tribool::Indeterminate,
                    tribool::Indeterminate,
                    tribool::Indeterminate,
                    tribool::Indeterminate));
            break;
        case 2:
            user.set_database_permissions(
                database_id_primary,
                permissions_t(
                    tribool::Indeterminate,
                    tribool::Indeterminate,
                    tribool::Indeterminate));
            break;
        case 3:
            user.set_table_permissions(
                table_id_primary,
                permissions_t(
                    tribool::Indeterminate,
                    tribool::Indeterminate,
                    tribool::Indeterminate));
            break;
        }
    }

    if (user != old_user) {
        transaction_modify_user(txn, username_primary, old_user, user);
        reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
        cv.value++;
        transaction_set_config_version(txn, cv);
    }

    return true;
}

uint8_t permissions_artificial_table_fdb_backend_t::parse_primary_key(
        const signal_t *interruptor,
        FDBTransaction *txn,
        ql::datum_t const &primary_key,
        username_t *username_out,
        database_id_t *database_id_out,
        name_string_t *database_name_out,
        namespace_id_t *table_id_out,
        table_config_t *table_config_out,
        admin_err_t *admin_err_out) {
    if (primary_key.get_type() != ql::datum_t::R_ARRAY ||
            primary_key.arr_size() < 1 ||
            primary_key.arr_size() > 3) {
        if (admin_err_out != nullptr) {
            *admin_err_out = admin_err_t{
                "Expected an array of one to three items in the primary key, got " +
                    primary_key.print() + ".",
                query_state_t::FAILED};
        }
        return 0;
    }

    ql::datum_t username = primary_key.get(0, ql::NOTHROW);
    if (username.get_type() != ql::datum_t::R_STR) {
        if (admin_err_out != nullptr) {
            *admin_err_out = admin_err_t{
                "Expected a string as the username, got " + username.print() + ".",
                query_state_t::FAILED};
        }
        return 0;
    }
    *username_out = username_t(username.as_str().to_std());

    // Left empty if the database is the system db.
    fdb_value_fut<name_string_t> database_name_fut;

    if (primary_key.arr_size() > 1) {
        ql::datum_t database = primary_key.get(1, ql::NOTHROW);
        if (database.get_type() != ql::datum_t::R_STR ||
                !str_to_uuid(database.as_str().to_std(), &database_id_out->value)) {
            if (admin_err_out != nullptr) {
                *admin_err_out = admin_err_t{
                    "Expected a UUID as the database, got " + database.print() + ".",
                    query_state_t::FAILED};
            }
            return 0;
        }

        if (*database_id_out != artificial_reql_cluster_interface_t::database_id) {
            database_name_fut = transaction_lookup_uq_index<db_config_by_id>(txn, *database_id_out);
        }
    }

    fdb_value_fut<table_config_t> table_config_fut;

    if (primary_key.arr_size() > 2) {
        ql::datum_t table = primary_key.get(2, ql::NOTHROW);
        if (table.get_type() != ql::datum_t::R_STR ||
                !str_to_uuid(table.as_str().to_std(), &table_id_out->value)) {
            if (admin_err_out != nullptr) {
                *admin_err_out = admin_err_t{
                    "Expected a UUID as the table, got " + table.print() + ".",
                    query_state_t::FAILED};
            }
            return 0;
        }

        if (!database_name_fut.empty()) {
            table_config_fut = transaction_lookup_uq_index<table_config_by_id>(txn, *table_id_out);
        }
    }

    if (primary_key.arr_size() > 1) {
        if (database_name_fut.empty()) {
            *database_name_out = artificial_reql_cluster_interface_t::database_name;
        } else {
            if (!database_name_fut.block_and_deserialize(interruptor, database_name_out)) {
                if (admin_err_out != nullptr) {
                    *admin_err_out = admin_err_t{
                        strprintf(
                            "No database with UUID `%s` exists.",
                            uuid_to_str(*database_id_out).c_str()),
                        query_state_t::FAILED};
                }
                return 0;
            }
        }
    }

    if (primary_key.arr_size() > 2) {
        if (database_name_fut.empty()) {
            bool exists = artificial_reql_cluster_interface_t::check_by_uuid_table_exists(*table_id_out);
            if (!exists) {
                if (admin_err_out != nullptr) {
                    *admin_err_out = admin_err_t{
                        strprintf(
                            "No table with UUID `%s` exists.",
                            uuid_to_str(*table_id_out).c_str()),
                        query_state_t::FAILED};
                }
                return 0;
            }
        } else {
            if (!table_config_fut.block_and_deserialize(interruptor, table_config_out)) {
                if (admin_err_out != nullptr) {
                    *admin_err_out = admin_err_t{
                        strprintf(
                            "No table with UUID `%s` exists.",
                            uuid_to_str(*table_id_out).c_str()),
                        query_state_t::FAILED};
                }
                return 0;
            }

            if (table_config_out->basic.database != *database_id_out) {
                // TODO: Just force admin_err_out to be non-null.
                if (admin_err_out != nullptr) {
                    // TODO: This should be a better error message, right?
                    *admin_err_out = admin_err_t{
                        strprintf(
                            "No table with UUID `%s` exists.",
                            uuid_to_str(*table_id_out).c_str()),
                        query_state_t::FAILED};
                }
                return 0;
            }
        }
    }

    return primary_key.arr_size();
}

bool permissions_artificial_table_fdb_backend_t::global_to_datum(
        username_t const &username,
        permissions_t const &permissions,
        ql::datum_t *datum_out) {
    ql::datum_t permissions_datum = permissions.to_datum();
    if (permissions_datum.get_type() != ql::datum_t::R_NULL) {
        ql::datum_object_builder_t builder;

        ql::datum_array_builder_t id_builder(ql::configured_limits_t::unlimited);
        id_builder.add(convert_string_to_datum(username.to_string()));

        builder.overwrite("id", std::move(id_builder).to_datum());
        builder.overwrite("permissions", std::move(permissions_datum));
        builder.overwrite("user", convert_string_to_datum(username.to_string()));

        *datum_out = std::move(builder).to_datum();
        return true;
    } else {
        *datum_out = ql::datum_t::null();
    }

    return false;
}

bool permissions_artificial_table_fdb_backend_t::database_to_datum(
        FDBTransaction *txn,
        const signal_t *interruptor,
        std::unordered_map<database_id_t, optional<name_string_t>> *db_name_cache,
        username_t const &username,
        database_id_t const &database_id,
        permissions_t const &permissions,
        ql::datum_t *datum_out) {
    ql::datum_t permissions_datum = permissions.to_datum();
    if (permissions_datum.get_type() != ql::datum_t::R_NULL) {
        ql::datum_object_builder_t builder;

        ql::datum_array_builder_t id_builder(ql::configured_limits_t::unlimited);
        id_builder.add(convert_string_to_datum(username.to_string()));
        id_builder.add(convert_uuid_to_datum(database_id.value));

        ql::datum_t database_name_or_uuid;
        switch (m_identifier_format) {
        case admin_identifier_format_t::name: {
            optional<name_string_t> database_name
                = lookup_db(txn, interruptor, db_name_cache, database_id);

            database_name_or_uuid = ql::datum_t(database_name.value_or(
                name_string_t::guarantee_valid("__deleted_database__")).str());
        } break;
        case admin_identifier_format_t::uuid:
            database_name_or_uuid = ql::datum_t(uuid_to_str(database_id));
            break;
        }

        builder.overwrite("database", std::move(database_name_or_uuid));
        builder.overwrite("id", std::move(id_builder).to_datum());
        builder.overwrite("permissions", std::move(permissions_datum));
        builder.overwrite("user", convert_string_to_datum(username.to_string()));

        *datum_out = std::move(builder).to_datum();
        return true;
    }

    return false;
}

bool permissions_artificial_table_fdb_backend_t::table_to_datum(
        username_t const &username,
        database_id_t const &database_id,
        optional<name_string_t> const &database_name,
        namespace_id_t const &table_id,
        table_basic_config_t const &table_basic_config,
        permissions_t const &permissions,
        ql::datum_t *datum_out) {
    // Here we bailed out early if the table didn't exist, according to the name
    // resolver.  Every user permission should check if the table exists -- or user
    // permissions by construction should not reference tables or databases that don't
    // exist.
    //
    // Note that right now, tables of deleted db's will sit around in user permissions,
    // until they're incrementally removed by the db drop job.  We filter them from this
    // system table.

    if (!database_name.has_value()) {
        // This is how we filter them.
        return false;
    }

    ql::datum_t permissions_datum = permissions.to_datum();
    if (permissions_datum.get_type() != ql::datum_t::R_NULL) {
        ql::datum_object_builder_t builder;

        ql::datum_array_builder_t id_builder(ql::configured_limits_t::unlimited);
        id_builder.add(convert_string_to_datum(username.to_string()));
        id_builder.add(convert_uuid_to_datum(database_id.value));
        id_builder.add(convert_uuid_to_datum(table_id.value));

        ql::datum_t database_name_or_uuid;
        ql::datum_t table_name_or_uuid;
        switch (m_identifier_format) {
            case admin_identifier_format_t::name: {
                guarantee(database_name.has_value());  // True because we bail out at the top of this function.
                database_name_or_uuid = ql::datum_t(database_name->str());
                table_name_or_uuid = ql::datum_t(table_basic_config.name.str());
            } break;
            case admin_identifier_format_t::uuid:
                database_name_or_uuid =
                    ql::datum_t(uuid_to_str(database_id));
                table_name_or_uuid = ql::datum_t(uuid_to_str(table_id));
                break;
        }

        builder.overwrite("database", std::move(database_name_or_uuid));
        builder.overwrite("id", std::move(id_builder).to_datum());
        builder.overwrite("permissions", std::move(permissions_datum));
        builder.overwrite("table", std::move(table_name_or_uuid));
        builder.overwrite("user", convert_string_to_datum(username.to_string()));

        *datum_out = std::move(builder).to_datum();
        return true;
    }

    return false;
}

}  // namespace auth
