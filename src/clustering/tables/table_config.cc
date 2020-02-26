// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/tables/table_config.hpp"

#include "clustering/auth/user_fut.hpp"
#include "clustering/datum_adapter.hpp"
#include "clustering/real_reql_cluster_interface.hpp"  // for table_already_exists_error
#include "clustering/tables/table_metadata.hpp"
#include "containers/archive/string_stream.hpp"
#include "containers/uuid.hpp"
#include "fdb/index.hpp"
#include "fdb/system_tables.hpp"
#include "fdb/typed.hpp"
#include "rdb_protocol/terms/write_hook.hpp"
#include "rdb_protocol/reqlfdb_config_cache_functions.hpp"

table_config_artificial_table_fdb_backend_t::table_config_artificial_table_fdb_backend_t(
        admin_identifier_format_t _identifier_format)
    : common_table_artificial_table_fdb_backend_t(
        name_string_t::guarantee_valid("table_config"),
        _identifier_format) {
}

table_config_artificial_table_fdb_backend_t::~table_config_artificial_table_fdb_backend_t() {
}

ql::datum_t convert_write_ack_config_to_datum(
        const write_ack_config_t &config) {
    switch (config) {
        case write_ack_config_t::SINGLE:
            return ql::datum_t("single");
        case write_ack_config_t::MAJORITY:
            return ql::datum_t("majority");
        default:
            unreachable();
    }
}

bool convert_write_ack_config_from_datum(
        const ql::datum_t &datum,
        write_ack_config_t *config_out,
        admin_err_t *error_out) {
    if (datum == ql::datum_t("single")) {
        *config_out = write_ack_config_t::SINGLE;
    } else if (datum == ql::datum_t("majority")) {
        *config_out = write_ack_config_t::MAJORITY;
    } else {
        *error_out = admin_err_t{
            "Expected \"single\" or \"majority\", got: " + datum.print(),
            query_state_t::FAILED};
        return false;
    }
    return true;
}

ql::datum_t convert_durability_to_datum(
        write_durability_t durability) {
    switch (durability) {
        case write_durability_t::SOFT:
            return ql::datum_t("soft");
        case write_durability_t::HARD:
            return ql::datum_t("hard");
        case write_durability_t::INVALID:
        default:
            unreachable();
    }
}

bool convert_durability_from_datum(
        const ql::datum_t &datum,
        write_durability_t *durability_out,
        admin_err_t *error_out) {
    if (datum == ql::datum_t("soft")) {
        *durability_out = write_durability_t::SOFT;
    } else if (datum == ql::datum_t("hard")) {
        *durability_out = write_durability_t::HARD;
    } else {
        *error_out = admin_err_t{
            "Expected \"soft\" or \"hard\", got: " + datum.print(),
            query_state_t::FAILED};
        return false;
    }
    return true;
}

ql::datum_t convert_sindexes_to_datum(
        const std::unordered_map<std::string, sindex_metaconfig_t> &sindexes) {
    ql::datum_array_builder_t sindexes_builder(ql::configured_limits_t::unlimited);
    for (const auto &sindex : sindexes) {
        sindexes_builder.add(ql::datum_t(sindex.first));
    }
    sindexes_builder.sort();
    return std::move(sindexes_builder).to_datum();
}

ql::datum_t convert_write_hook_to_datum(
    const optional<write_hook_config_t> &write_hook) {

    ql::datum_t res = ql::datum_t::null();
    if (write_hook.has_value()) {
        write_message_t wm;
        serialize<cluster_version_t::LATEST_DISK>(
            &wm, write_hook->func);
        string_stream_t stream;
        int write_res = send_write_message(&stream, &wm);

        rcheck_toplevel(write_res == 0,
                        ql::base_exc_t::LOGIC,
                        "Invalid write hook.");

        ql::datum_t binary = ql::datum_t::binary(
            datum_string_t(write_hook_blob_prefix + stream.str()));
        res =
            ql::datum_t{
                std::map<datum_string_t, ql::datum_t>{
                    std::pair<datum_string_t, ql::datum_t>(
                        datum_string_t("function"), binary),
                        std::pair<datum_string_t, ql::datum_t>(
                            datum_string_t("query"),
                            ql::datum_t(
                                datum_string_t(
                                    format_write_hook_query(write_hook.get()))))}};
    }
    return res;
}

bool convert_sindexes_from_datum(
        ql::datum_t datum,
        std::set<std::string> *indexes_out,
        admin_err_t *error_out) {
    if (!convert_set_from_datum<std::string>(
            &convert_string_from_datum, false, datum, indexes_out, error_out)) {
        error_out->msg = "In `indexes`: " + error_out->msg;
        return false;
    }

    return true;
}

/* This is separate from `format_row()` because it needs to be publicly exposed so it
   can be used to create the return value of `table.reconfigure()`. */
ql::datum_t convert_table_config_to_datum(
        namespace_id_t table_id,
        const ql::datum_t &db_name_or_uuid,
        const table_config_t &config,
        admin_identifier_format_t identifier_format) {
    ql::datum_object_builder_t builder;
    builder.overwrite("name", convert_name_to_datum(config.basic.name));
    builder.overwrite("db", db_name_or_uuid);
    builder.overwrite("id", convert_uuid_to_datum(table_id.value));
    builder.overwrite("indexes", convert_sindexes_to_datum(config.sindexes));
    builder.overwrite("write_hook", convert_write_hook_to_datum(config.write_hook));
    builder.overwrite("primary_key", convert_string_to_datum(config.basic.primary_key));
    // TODO: Could we remove this from the table config datum?
    builder.overwrite("write_acks",
        convert_write_ack_config_to_datum(write_ack_config_t::MAJORITY));
    builder.overwrite("durability",
        convert_durability_to_datum(write_durability_t::HARD));
    builder.overwrite("data", config.user_data.datum);
    return std::move(builder).to_datum();
}

ql::datum_t table_config_artificial_table_fdb_backend_t::format_row(
        const namespace_id_t &table_id,
        const table_config_t &config,
        const ql::datum_t &db_name_or_uuid) const {
    return convert_table_config_to_datum(table_id, db_name_or_uuid,
        config, identifier_format);
}

bool convert_table_config_and_name_from_datum(
        ql::datum_t datum,
        bool existed_before,
        admin_identifier_format_t identifier_format,
        const table_config_t &old_config,
        namespace_id_t *id_out,
        table_config_t *config_out,
        admin_err_t *error_out) {
    /* In practice, the input will always be an object and the `id` field will always
    be valid, because `artificial_table_t` will check those thing before passing the
    row to `table_config_artificial_table_fdb_backend_t`. But we check them anyway for
    consistency. */
    converter_from_datum_object_t converter;
    if (!converter.init(datum, error_out)) {
        return false;
    }

    ql::datum_t name_datum;
    if (!converter.get("name", &name_datum, error_out)) {
        return false;
    }
    if (!convert_name_from_datum(
            name_datum, "table name", &config_out->basic.name, error_out)) {
        error_out->msg = "In `name`: " + error_out->msg;
        return false;
    }

    ql::datum_t db_datum;
    if (!converter.get("db", &db_datum, error_out)) {
        return false;
    }
    // QQQ: Make use of identifier_format, and handle db_drop in-progress case.
    if (!convert_uuid_from_datum(
            db_datum, &config_out->basic.database.value,
            error_out)) {
        return false;
    }

    ql::datum_t id_datum;
    if (!converter.get("id", &id_datum, error_out)) {
        return false;
    }
    if (!convert_uuid_from_datum(id_datum, &id_out->value, error_out)) {
        error_out->msg = "In `id`: " + error_out->msg;
        return false;
    }

    /* As a special case, we allow the user to omit `indexes`, `primary_key`, `shards`,
    `write_acks`, `durability`, and/or `data` for newly-created tables. */

    if (converter.has("indexes")) {
        ql::datum_t indexes_datum;
        if (!converter.get("indexes", &indexes_datum, error_out)) {
            return false;
        }
        std::set<std::string> sindexes;
        if (!convert_sindexes_from_datum(indexes_datum, &sindexes, error_out)) {
            return false;
        }

        if (existed_before) {
            bool equal = sindexes.size() == old_config.sindexes.size();
            for (const auto &old_sindex : old_config.sindexes) {
                equal &= sindexes.count(old_sindex.first) == 1;
            }
            if (!equal) {
                error_out->msg = "The `indexes` field is read-only and can't be used to "
                                 "create or drop indexes.";
                return false;
            }
            config_out->sindexes = old_config.sindexes;
        } else if (!sindexes.empty()) {
            error_out->msg = "The `indexes` field is read-only and can't be used to "
                             "create indexes.";
            return false;
        }
    } else {
        if (existed_before) {
            error_out->msg = "Expected a field named `indexes`.";
            return false;
        }
    }

    if (existed_before || converter.has("primary_key")) {
        ql::datum_t primary_key_datum;
        if (!converter.get("primary_key", &primary_key_datum, error_out)) {
            return false;
        }
        if (!convert_string_from_datum(primary_key_datum,
                &config_out->basic.primary_key, error_out)) {
            error_out->msg = "In `primary_key`: " + error_out->msg;
            return false;
        }
    } else {
        config_out->basic.primary_key = "id";
    }

    if (existed_before || converter.has("write_acks")) {
        ql::datum_t write_acks_datum;
        if (!converter.get("write_acks", &write_acks_datum, error_out)) {
            return false;
        }
        write_ack_config_t write_ack_config;
        if (!convert_write_ack_config_from_datum(write_acks_datum,
                &write_ack_config, error_out)) {
            error_out->msg = "In `write_acks`: " + error_out->msg;
            return false;
        }
        // TODO: Should we error if write_acks wasn't MAJORITY?
    }

    if (existed_before || converter.has("durability")) {
        ql::datum_t durability_datum;
        if (!converter.get("durability", &durability_datum, error_out)) {
            return false;
        }
        write_durability_t durability;
        if (!convert_durability_from_datum(durability_datum, &durability,
                                           error_out)) {
            error_out->msg = "In `durability`: " + error_out->msg;
            return false;
        }
        // TODO: Should we error if durability wasn't HARD?
    }

    if (converter.has("write_hook")) {
        ql::datum_t write_hook_datum;
        if (!converter.get("write_hook", &write_hook_datum, error_out)) {
            return false;
        }
        if (write_hook_datum.has()) {
            if ((!old_config.write_hook.has_value() &&
                 write_hook_datum.get_type() != ql::datum_t::type_t::R_NULL ) ||
                write_hook_datum
                != convert_write_hook_to_datum(old_config.write_hook)) {
                error_out->msg = "The `write_hook` field is read-only and can't" \
                    " be used to create or drop a write hook function.";
                return false;
            }
        }
        config_out->write_hook = old_config.write_hook;
    } else {
        if (existed_before) {
            error_out->msg = "Expected a field named `write_hook`.";
            return false;
        }
    }

    if (existed_before || converter.has("data")) {
        ql::datum_t user_data_datum;
        if (!converter.get("data", &user_data_datum, error_out)) {
            return false;
        }
        config_out->user_data = {std::move(user_data_datum)};
    } else {
        config_out->user_data = default_user_data();
    }

    // TODO: In fdb-ization, we dropped some keys, like shards, surely.  Tolerate them for now.
    if (!converter.check_no_extra_keys(error_out)) {
        return false;
    }

    return true;
}

bool table_config_artificial_table_fdb_backend_t::write_row(
        FDBTransaction *txn,
        auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        bool pkey_was_autogenerated,
        ql::datum_t *new_value_inout,
        const signal_t *interruptor,
        admin_err_t *error_out) {
    /* Parse primary key */
    bool old_exists = false;
    name_string_t old_db_name;
    table_config_t old_config;
    namespace_id_t table_id;
    {
        admin_err_t dummy_error;
        if (!convert_uuid_from_datum(primary_key, &table_id.value, &dummy_error)) {
            /* If the primary key was not a valid UUID, then it must refer to a nonexistent
            row. */
            guarantee(!pkey_was_autogenerated, "auto-generated primary key should have "
                "been a valid UUID string.");
            // TODO: Just create an error right here instead of falling through to if (!pkey_was_autogenerated) branch later on.
            table_id.value = nil_uuid();
        } else {

            // Fetch the old config.
            fdb_value_fut<table_config_t> old_config_fut = transaction_lookup_uq_index<table_config_by_id>(txn, table_id);

            bool old_seems_to_exist = old_config_fut.block_and_deserialize(interruptor, &old_config);

            if (old_seems_to_exist) {
                fdb_value_fut<name_string_t> db_by_id_fut = transaction_lookup_uq_index<db_config_by_id>(txn, old_config.basic.database);
                old_exists = db_by_id_fut.block_and_deserialize(interruptor, &old_db_name);
            }
        }
    }

    if (old_exists) {
        auth::fdb_user_fut<auth::db_table_config_permission> auth_fut
            = user_context.transaction_require_db_and_table_config_permission(
                txn, old_config.basic.database, table_id);

        // TODO: Maybe parallelize a bit.
        auth_fut.block_and_check(interruptor);

        if (new_value_inout->has()) {
            // TODO: identifier_format freaks me out.
            namespace_id_t new_table_id;
            table_config_t new_config;
            if (!convert_table_config_and_name_from_datum(*new_value_inout, true,
                identifier_format, old_config, &new_table_id, &new_config,
                error_out)) {
                error_out->msg = "The change you're trying to make to "
                    "`rethinkdb.table_config` has the wrong format. "
                    + error_out->msg;
                return false;
            }
            guarantee(new_table_id == table_id, "artificial_table_t shouldn't have "
                "allowed the primary key to change");

            // At this point, we're now going to mutate the table config in-place.

            // 0. If there is no config change, don't write anything.  We did already
            // check permission.  Avoid spurious reqlfdb_config_version increments.
            if (new_config == old_config) {
                return true;
            }

            fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);

            // 1. Check the new db exists, if different from the old.

            const bool db_changed = new_config.basic.database != old_config.basic.database;

            name_string_t new_db_name;
            if (db_changed) {
                // TODO: Read the user_t _once_, not twice as with here and auth_fut.
                auth::fdb_user_fut<auth::db_config_permission> auth2_fut
                    = user_context.transaction_require_db_config_permission(txn, new_config.basic.database);
                fdb_value_fut<name_string_t> new_db_fut
                    = transaction_lookup_uq_index<db_config_by_id>(txn, new_config.basic.database);

                auth2_fut.block_and_check(interruptor);
                if (!new_db_fut.block_and_deserialize(interruptor, &new_db_name)) {
                    // TODO: Better message, saying the move destination x -> y, etc.
                    *error_out = admin_err_t{
                        strprintf("The database with id `%s` does not exist.",
                            uuid_to_str(new_config.basic.database).c_str()),
                        query_state_t::FAILED};
                    return false;
                }
            } else {
                new_db_name = old_db_name;
            }

            if (new_config.basic.primary_key != old_config.basic.primary_key) {
                *error_out = admin_err_t{"It's illegal to change a table's primary key",
                     query_state_t::FAILED};
                 return false;
            }

            if (db_changed || new_config.basic.name != old_config.basic.name) {
                // We'll have to update the index entry and check for name conflicts
                // (either with the new name in the same db, or the name in the new
                // db).
                std::pair<database_id_t, name_string_t> old_table_index_key{old_config.basic.database,
                    old_config.basic.name};
                std::pair<database_id_t, name_string_t> new_table_index_key{new_config.basic.database,
                    new_config.basic.name};
                fdb_value_fut<namespace_id_t> fut = transaction_lookup_uq_index<table_config_by_name>(txn,
                    new_table_index_key);

                fdb_value new_table_index_value = future_block_on_value(fut.fut, interruptor);
                if (new_table_index_value.present) {
                    *error_out = table_already_exists_error(new_db_name, new_config.basic.name);
                    return false;
                }

                transaction_erase_uq_index<table_config_by_name>(txn, old_table_index_key);
                transaction_set_uq_index<table_config_by_name>(txn, new_table_index_key,
                    new_table_id);
            }

            // users_by_ids index is unchanged.
            transaction_set_uq_index<table_config_by_id>(txn, table_id, new_config);

            reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
            cv.value++;
            transaction_set_config_version(txn, cv);
            return true;
        } else {
            fdb_value_fut<reqlfdb_config_version> cv_fut = transaction_get_config_version(txn);
            help_remove_table(txn, table_id, old_config, interruptor);
            reqlfdb_config_version cv = cv_fut.block_and_deserialize(interruptor);
            cv.value++;
            transaction_set_config_version(txn, cv);
            return true;
        }
    }

    if (!new_value_inout->has()) {
        /* The user is deleting a table that doesn't exist. Do nothing. */
        return true;
    }

    // Now we're creating a table.
    if (!pkey_was_autogenerated) {
        *error_out = admin_err_t{
            "There is no existing table with the given ID. To create a "
            "new table by inserting into `rethinkdb.table_config`, you must let "
            "the database generate the primary key automatically.",
            query_state_t::FAILED};
        return false;
    }

    namespace_id_t new_table_id;
    table_config_t new_config;
    if (!convert_table_config_and_name_from_datum(*new_value_inout, false,
        identifier_format, table_config_t(),
        &new_table_id, &new_config, error_out)) {
        error_out->msg = "The change you're trying to make to "
            "`rethinkdb.table_config` has the wrong format. " + error_out->msg;
        return false;
    }
    guarantee(new_table_id == table_id, "artificial_table_t shouldn't have "
        "allowed the primary key to change");

    /* `convert_table_config_and_name_from_datum()` might have filled in missing
    fields, so we need to write back the filled-in values to `new_value_inout`.
    */
    *new_value_inout = convert_table_config_to_datum(
        table_id, new_value_inout->get_field("db"), new_config,
        identifier_format);

    // At this point we need to create the table.

    // Consistency check:  Check if the new table config db exists.
    // TODO: When we start paying attention to identifier_format, we might need to check by name.
    fdb_value_fut<name_string_t> new_db_by_id_fut = transaction_lookup_uq_index<db_config_by_id>(txn, new_config.basic.database);
    name_string_t new_db_name;
    if (!new_db_by_id_fut.block_and_deserialize(interruptor, &new_db_name)) {
        // TODO: dedup general db does not exist error.  And a better error message for system db.
        *error_out = admin_err_t{
            strprintf("The database with id `%s` does not exist.",
                uuid_to_str(new_config.basic.database).c_str()),
            query_state_t::FAILED};
        return false;
    }

    // This updates the reqlfdb_config_version.
    bool success = config_cache_table_create(txn, config_version_checker::empty(),
        user_context, new_table_id, new_config, interruptor);
    if (!success) {
        *error_out = admin_err_t{
            strprintf("Table `%s.%s` already exists.",
                      new_db_name.c_str(), new_config.basic.name.c_str()),
            query_state_t::FAILED};
        return false;
    }

    return true;
}
