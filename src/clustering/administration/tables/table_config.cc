// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/administration/tables/table_config.hpp"

#include "clustering/administration/datum_adapter.hpp"
#include "clustering/administration/metadata.hpp"
#include "clustering/table_manager/table_meta_client.hpp"
#include "concurrency/cross_thread_signal.hpp"
#include "containers/archive/string_stream.hpp"
#include "containers/uuid.hpp"
#include "rdb_protocol/terms/write_hook.hpp"

table_config_artificial_table_backend_t::table_config_artificial_table_backend_t(
        rdb_context_t *_rdb_context,
        lifetime_t<name_resolver_t const &> name_resolver,
        std::shared_ptr< semilattice_readwrite_view_t<
            cluster_semilattice_metadata_t> > _semilattice_view,
        admin_identifier_format_t _identifier_format,
        table_meta_client_t *_table_meta_client)
    : common_table_artificial_table_backend_t(
        name_string_t::guarantee_valid("table_config"),
        name_resolver,
        _semilattice_view,
        _table_meta_client,
        _identifier_format),
      rdb_context(_rdb_context) {
}

table_config_artificial_table_backend_t::~table_config_artificial_table_backend_t() {
#if RDB_CF
    begin_changefeed_destruction();
#endif
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
        const std::map<std::string, sindex_config_t> &sindexes) {
    ql::datum_array_builder_t sindexes_builder(ql::configured_limits_t::unlimited);
    for (const auto &sindex : sindexes) {
        sindexes_builder.add(convert_string_to_datum(sindex.first));
    }
    return std::move(sindexes_builder).to_datum();
}

ql::datum_t convert_write_hook_to_datum(
    const optional<write_hook_config_t> &write_hook) {

    ql::datum_t res = ql::datum_t::null();
    if (write_hook) {
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

void table_config_artificial_table_backend_t::format_row(
        UNUSED auth::user_context_t const &user_context,
        const namespace_id_t &table_id,
        const table_config_and_shards_t &config,
        const ql::datum_t &db_name_or_uuid,
        UNUSED const signal_t *interruptor_on_home,
        ql::datum_t *row_out)
        THROWS_ONLY(interrupted_exc_t, no_such_table_exc_t, failed_table_op_exc_t) {
    assert_thread();
    *row_out = convert_table_config_to_datum(table_id, db_name_or_uuid,
        config.config, identifier_format);
}

bool convert_table_config_and_name_from_datum(
        ql::datum_t datum,
        bool existed_before,
        const cluster_semilattice_metadata_t &all_metadata,
        admin_identifier_format_t identifier_format,
        const table_config_and_shards_t &old_config,
        namespace_id_t *id_out,
        table_config_t *config_out,
        name_string_t *db_name_out,
        admin_err_t *error_out)
        THROWS_ONLY(interrupted_exc_t, admin_op_exc_t) {
    /* In practice, the input will always be an object and the `id` field will always
    be valid, because `artificial_table_t` will check those thing before passing the
    row to `table_config_artificial_table_backend_t`. But we check them anyway for
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
    if (!convert_database_id_from_datum(
            db_datum, identifier_format, all_metadata, &config_out->basic.database,
            db_name_out, error_out)) {
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
            bool equal = sindexes.size() == old_config.config.sindexes.size();
            for (const auto &old_sindex : old_config.config.sindexes) {
                equal &= sindexes.count(old_sindex.first) == 1;
            }
            if (!equal) {
                error_out->msg = "The `indexes` field is read-only and can't be used to "
                                 "create or drop indexes.";
                return false;
            }
            config_out->sindexes = old_config.config.sindexes;
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
            if ((!old_config.config.write_hook &&
                 write_hook_datum.get_type() != ql::datum_t::type_t::R_NULL ) ||
                write_hook_datum
                != convert_write_hook_to_datum(old_config.config.write_hook)) {
                error_out->msg = "The `write_hook` field is read-only and can't" \
                    " be used to create or drop a write hook function.";
                return false;
            }
        }
        config_out->write_hook = old_config.config.write_hook;
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

    if (!converter.check_no_extra_keys(error_out)) {
        return false;
    }

    return true;
}

void table_config_artificial_table_backend_t::do_modify(
        const namespace_id_t &table_id,
        table_config_and_shards_t &&old_config,
        table_config_t &&new_config_no_shards,
        const name_string_t &old_db_name,
        const name_string_t &new_db_name,
        const signal_t *interruptor)
        THROWS_ONLY(interrupted_exc_t, no_such_table_exc_t, failed_table_op_exc_t,
            maybe_failed_table_op_exc_t, admin_op_exc_t) {
    table_config_and_shards_t new_config;
    new_config.config = std::move(new_config_no_shards);

    if (new_config.config.basic.primary_key != old_config.config.basic.primary_key) {
        throw admin_op_exc_t("It's illegal to change a table's primary key",
                             query_state_t::FAILED);
    }

    if (new_config.config.basic.database != old_config.config.basic.database ||
            new_config.config.basic.name != old_config.config.basic.name) {
        if (table_meta_client->exists(
                new_config.config.basic.database, new_config.config.basic.name)) {
            throw admin_op_exc_t(
                strprintf(
                    "Can't rename table `%s.%s` to `%s.%s` because table `%s.%s` "
                    "already exists.",
                    old_db_name.c_str(), old_config.config.basic.name.c_str(),
                    new_db_name.c_str(), new_config.config.basic.name.c_str(),
                    new_db_name.c_str(), new_config.config.basic.name.c_str()),
                query_state_t::FAILED);
        }
    }

    table_config_and_shards_change_t table_config_and_shards_change(
        table_config_and_shards_change_t::set_table_config_and_shards_t{ new_config });
    table_meta_client->set_config(
        table_id, table_config_and_shards_change, interruptor);
}

void table_config_artificial_table_backend_t::do_create(
        const namespace_id_t &table_id,
        table_config_t &&new_config_no_shards,
        const name_string_t &new_db_name,
        const signal_t *interruptor)
        THROWS_ONLY(interrupted_exc_t, no_such_table_exc_t, failed_table_op_exc_t,
            maybe_failed_table_op_exc_t, admin_op_exc_t) {
    table_config_and_shards_t new_config;
    new_config.config = std::move(new_config_no_shards);

    if (table_meta_client->exists(
            new_config.config.basic.database, new_config.config.basic.name)) {
        throw admin_op_exc_t(
            strprintf("Table `%s.%s` already exists.",
                      new_db_name.c_str(), new_config.config.basic.name.c_str()),
            query_state_t::FAILED);
    }

    table_meta_client->create(table_id, new_config, interruptor);
}

bool table_config_artificial_table_backend_t::write_row(
        auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        bool pkey_was_autogenerated,
        ql::datum_t *new_value_inout,
        const signal_t *interruptor_on_caller,
        admin_err_t *error_out) {
    /* Parse primary key */
    namespace_id_t table_id;
    admin_err_t dummy_error;
    if (!convert_uuid_from_datum(primary_key, &table_id.value, &dummy_error)) {
        /* If the primary key was not a valid UUID, then it must refer to a nonexistent
        row. */
        guarantee(!pkey_was_autogenerated, "auto-generated primary key should have "
            "been a valid UUID string.");
        table_id.value = nil_uuid();
    }

    cross_thread_signal_t interruptor_on_home(interruptor_on_caller, home_thread());
    on_thread_t thread_switcher(home_thread());
    cluster_semilattice_metadata_t metadata = semilattice_view->get();

    try {
        try {
            /* Fetch the name of the table and its database for error messages */
            table_basic_config_t old_basic_config;
            table_meta_client->get_name(table_id, &old_basic_config);
            guarantee(!pkey_was_autogenerated, "UUID collision happened");

            user_context.require_config_permission(
                rdb_context, old_basic_config.database, table_id);

            name_string_t old_db_name;
            if (!convert_database_id_to_datum(old_basic_config.database,
                    identifier_format, metadata, nullptr, &old_db_name)) {
                old_db_name = name_string_t::guarantee_valid("__deleted_database__");
            }

            if (new_value_inout->has()) {
                table_config_and_shards_t old_config;
                try {
                    table_meta_client->get_config(
                        table_id, &interruptor_on_home, &old_config);
                } CATCH_OP_ERRORS(old_db_name, old_basic_config.name, error_out,
                    "Failed to retrieve the table's configuration, it was not changed.",
                    "Failed to retrieve the table's configuration, it was not changed.")

                table_config_t new_config;
                namespace_id_t new_table_id;
                name_string_t new_db_name;
                if (!convert_table_config_and_name_from_datum(*new_value_inout, true,
                        metadata, identifier_format,
                        old_config,
                        &new_table_id, &new_config, &new_db_name,
                        error_out)) {
                    error_out->msg = "The change you're trying to make to "
                        "`rethinkdb.table_config` has the wrong format. "
                        + error_out->msg;
                    return false;
                }
                guarantee(new_table_id == table_id, "artificial_table_t shouldn't have "
                    "allowed the primary key to change");

                // Verify the user is allowed to move the table to the new database.
                if (new_config.basic.database != old_basic_config.database) {
                    user_context.require_config_permission(
                        rdb_context, new_config.basic.database);
                }

                try {
                    do_modify(table_id, std::move(old_config), std::move(new_config),
                        old_db_name, new_db_name,
                        &interruptor_on_home);
                    return true;
                } CATCH_OP_ERRORS(old_db_name, old_basic_config.name, error_out,
                    "The table's configuration was not changed.",
                    "The table's configuration may or may not have been changed.")
            } else {
                try {
                    table_meta_client->drop(table_id, &interruptor_on_home);
                    return true;
                } CATCH_OP_ERRORS(old_db_name, old_basic_config.name, error_out,
                    "The table was not dropped.",
                    "The table may or may not have been dropped.")
            }
        } catch (const no_such_table_exc_t &) {
            /* Fall through */
        }
        if (new_value_inout->has()) {
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
            name_string_t new_db_name;
            if (!convert_table_config_and_name_from_datum(*new_value_inout, false,
                    metadata, identifier_format,
                    table_config_and_shards_t(),
                    &new_table_id, &new_config,
                    &new_db_name, error_out)) {
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

            try {
                do_create(table_id, std::move(new_config),
                    new_db_name, &interruptor_on_home);
                return true;
            } CATCH_OP_ERRORS(new_db_name, new_config.basic.name, error_out,
                "The table was not created.",
                "The table may or may not have been created.")
        } else {
            /* The user is deleting a table that doesn't exist. Do nothing. */
            return true;
        }
    } catch (const admin_op_exc_t &admin_op_exc) {
        *error_out = admin_op_exc.to_admin_err();
        return false;
    }
}
