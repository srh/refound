// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/tables/db_config.hpp"

#include "clustering/auth/user_context.hpp"
#include "clustering/auth/user_fut.hpp"
#include "clustering/datum_adapter.hpp"
#include "clustering/artificial_reql_cluster_interface.hpp"
#include "fdb/index.hpp"
#include "fdb/retry_loop.hpp"
#include "fdb/system_tables.hpp"
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"

ql::datum_t convert_db_or_table_config_and_name_to_datum(
        const name_string_t &db_name,
        uuid_u id) {
    ql::datum_object_builder_t builder;
    builder.overwrite("name", convert_name_to_datum(db_name));
    builder.overwrite("id", convert_uuid_to_datum(id));
    return std::move(builder).to_datum();
}

bool convert_db_config_and_name_from_datum(
        ql::datum_t datum,
        name_string_t *db_name_out,
        uuid_u *id_out,
        admin_err_t *error_out) {
    /* In practice, the input will always be an object and the `id` field will always
    be valid, because `artificial_table_t` will check those thing before passing the
    row to `db_config_artificial_table_backend_t`. But we check them anyway for
    consistency. */
    converter_from_datum_object_t converter;
    if (!converter.init(datum, error_out)) {
        return false;
    }

    ql::datum_t name_datum;
    if (!converter.get("name", &name_datum, error_out)) {
        return false;
    }
    if (!convert_name_from_datum(name_datum, "db name", db_name_out, error_out)) {
        error_out->msg = "In `name`: " + error_out->msg;
        return false;
    }

    ql::datum_t id_datum;
    if (!converter.get("id", &id_datum, error_out)) {
        return false;
    }
    if (!convert_uuid_from_datum(id_datum, id_out, error_out)) {
        error_out->msg = "In `id`: " + error_out->msg;
        return false;
    }

    if (!converter.check_no_extra_keys(error_out)) {
        return false;
    }

    return true;
}

db_config_artificial_table_fdb_backend_t::db_config_artificial_table_fdb_backend_t() :
    artificial_table_fdb_backend_t(
        name_string_t::guarantee_valid("db_config"))
    { }

db_config_artificial_table_fdb_backend_t::~db_config_artificial_table_fdb_backend_t() {
}

bool db_config_artificial_table_fdb_backend_t::read_all_rows_as_vector(
        FDBDatabase *fdb,
        auth::user_context_t const &user_context,
        const signal_t *interruptor,
        std::vector<ql::datum_t> *rows_out,
        UNUSED admin_err_t *error_out) {
    std::vector<std::pair<database_id_t, name_string_t>> result;
    fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor,
            [&](FDBTransaction *txn) {
        auth::fdb_user_fut<auth::read_permission> auth_fut = get_read_permission(txn, user_context);
        auto dbs = config_cache_db_list_sorted_by_id(txn, interruptor);
        auth_fut.block_and_check(interruptor);
        result = std::move(dbs);
    });
    guarantee_fdb_TODO(loop_err, "read_all_rows_as_vector retry loop");

    rows_out->clear();
    for (auto &pair : result) {
        rows_out->push_back(convert_db_or_table_config_and_name_to_datum(pair.second, pair.first.value));
    }
    return true;
}

bool db_config_artificial_table_fdb_backend_t::read_row(
        FDBTransaction *txn,
        UNUSED auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        const signal_t *interruptor,
        ql::datum_t *row_out,
        UNUSED admin_err_t *error_out) {
    database_id_t database_id;
    admin_err_t dummy_error;
    if (!convert_uuid_from_datum(primary_key, &database_id.value, &dummy_error)) {
        /* If the primary key was not a valid UUID, then it must refer to a nonexistent
        row. */
        // Successfully read that there is no such row.
        *row_out = ql::datum_t::null();
        return true;
    }

    // This table does _not_ include the system db.

    // TODO: Pre-fdb we include the system db in the databases table, right?
    fdb_value_fut<name_string_t> fut = transaction_lookup_uq_index<db_config_by_id>(txn, database_id);
    name_string_t db_name;
    if (!fut.block_and_deserialize(interruptor, &db_name)) {
        // Successfully read that there is no such row.
        *row_out = ql::datum_t::null();
        return true;
    }
    *row_out = convert_db_or_table_config_and_name_to_datum(db_name, database_id.value);
    return true;
}

// TODO: Probably should move a lot of write_row to reqlfdb_config_cache.cc.

bool db_config_artificial_table_fdb_backend_t::write_row(
        FDBTransaction *txn,
        auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        bool pkey_was_autogenerated,
        ql::datum_t *new_value_inout,
        const signal_t *interruptor,
        admin_err_t *error_out) {

    /* Look for an existing DB with the given UUID */
    database_id_t database_id;
    name_string_t old_db_name;
    bool existed_before;
    admin_err_t dummy_error;
    if (!convert_uuid_from_datum(primary_key, &database_id.value, &dummy_error)) {
        /* If the primary key was not a valid UUID, then it must refer to a nonexistent
        row. */
        database_id.value = nil_uuid();
        existed_before = false;
    } else {
        fdb_value_fut<name_string_t> value_fut = transaction_lookup_uq_index<db_config_by_id>(txn, database_id);
        existed_before = value_fut.block_and_deserialize(interruptor, &old_db_name);
    }

    // TODO: Load auth stuff in first round trip, make an indeterminately checking auth fut type.
    if (existed_before) {
        auth::fdb_user_fut<auth::db_config_permission> auth_fut
            = user_context.transaction_require_db_config_permission(txn, database_id);
        auth_fut.block_and_check(interruptor);
        // We'll do a redundant auth check (for all the tables, in
        // config_cache_db_drop_uuid) if we drop the db.
    } else {
        auth::fdb_user_fut<auth::config_permission> auth_fut
            = user_context.transaction_require_config_permission(txn);
        auth_fut.block_and_check(interruptor);
    }

    if (new_value_inout->has()) {
        /* We're updating an existing database (if `existed_before == true`) or creating
        a new one (if `existed_before == false`) */

        /* Parse the new value the user provided for the database */
        name_string_t new_db_name;
        database_id_t new_database_id;
        if (!convert_db_config_and_name_from_datum(*new_value_inout,
                &new_db_name, &new_database_id.value, error_out)) {
            error_out->msg = "The change you're trying to make to "
                "`rethinkdb.db_config` has the wrong format. " + error_out->msg;
            return false;
        }
        guarantee(new_database_id == database_id, "artificial_table_t should ensure "
            "that the primary key doesn't change.");

        if (existed_before) {
            guarantee(!pkey_was_autogenerated, "UUID collision happened");
        } else {
            if (!pkey_was_autogenerated) {
                *error_out = admin_err_t{
                    "If you want to create a new table by inserting into "
                    "`rethinkdb.db_config`, you must use an auto-generated primary key.",
                    query_state_t::FAILED};
                return false;
            }
        }

        if (!existed_before || new_db_name != old_db_name) {
            /* Reserve the `rethinkdb` database name */
            const name_string_t rethinkdb_db_name = artificial_reql_cluster_interface_t::database_name;
            if (new_db_name == rethinkdb_db_name) {
                if (!existed_before) {
                    *error_out = db_already_exists_error(rethinkdb_db_name);
                } else {
                    // TODO: Dedup error message (with code below).
                    *error_out = admin_err_t{
                        strprintf("Cannot rename database `%s` to `rethinkdb` "
                                  "because database `rethinkdb` already exists.",
                                  old_db_name.c_str()),
                        query_state_t::FAILED};
                }
                return false;
            }
        }

        fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);
        if (!existed_before) {
            fdb_future collision_fut = transaction_lookup_uq_index<db_config_by_name>(
                txn, new_db_name);
            fdb_value collision = future_block_on_value(collision_fut.fut, interruptor);
            if (collision.present) {
                *error_out = admin_err_t{
                    strprintf("Database `%s` already exists.",
                              new_db_name.c_str()),
                    query_state_t::FAILED};
                return false;
            }

            transaction_set_uq_index<db_config_by_id>(txn, new_database_id, new_db_name);
            transaction_set_uq_index<db_config_by_name>(txn, new_db_name, new_database_id);

            reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
            cv.value++;
            transaction_set_config_version(txn, cv);
            return true;
        } else if (new_db_name != old_db_name) {
            // Perform a rename.
            fdb_future collision_fut = transaction_lookup_uq_index<db_config_by_name>(
                txn, new_db_name);
            fdb_value collision = future_block_on_value(collision_fut.fut, interruptor);
            if (collision.present) {
                *error_out = admin_err_t{
                    strprintf("Cannot rename database `%s` to `%s` "
                              "because database `%s` already exists.",
                              old_db_name.c_str(),
                              new_db_name.c_str(),
                              new_db_name.c_str()),
                    query_state_t::FAILED};
                return false;
            }


            transaction_set_uq_index<db_config_by_id>(txn, new_database_id, new_db_name);
            transaction_erase_uq_index<db_config_by_name>(txn, old_db_name);
            transaction_set_uq_index<db_config_by_name>(txn, new_db_name, new_database_id);

            reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
            cv.value++;
            transaction_set_config_version(txn, cv);
            return true;
        } else {
            // no-op write, new_db_name == old_db_name; do nothing.
            return true;
        }
    } else {
        if (existed_before) {
            /* We're deleting a database. */
            // TODO: This message seems nonsensical, pkey_was_autogenerated should be false whenever new_value_inout->has() is false.  Right?
            guarantee(!pkey_was_autogenerated, "UUID collision happened");

            config_cache_db_drop_uuid(txn, user_context, database_id, old_db_name,
                interruptor);
        }
        return true;
    }
}
