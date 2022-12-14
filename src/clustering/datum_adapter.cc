// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/datum_adapter.hpp"

#include "clustering/tables/table_metadata.hpp"
#include "rdb_protocol/admin_identifier_format.hpp"
#include "rdb_protocol/pseudo_time.hpp"

ql::datum_t convert_string_to_datum(
        const std::string &value) {
    return ql::datum_t(datum_string_t(value));
}

bool convert_string_from_datum(
        const ql::datum_t &datum,
        std::string *value_out,
        admin_err_t *error_out) {
    if (datum.get_type() != ql::datum_t::R_STR) {
        *error_out = admin_err_t{
            "Expected a string; got " + datum.print(),
            query_state_t::FAILED};
        return false;
    }
    *value_out = datum.as_str().to_std();
    return true;
}

ql::datum_t convert_name_to_datum(
        const name_string_t &value) {
    return ql::datum_t(value.str());
}

bool convert_name_from_datum(
        ql::datum_t datum,
        const char *what,
        name_string_t *value_out,
        admin_err_t *error_out) {
    if (datum.get_type() != ql::datum_t::R_STR) {
        *error_out = admin_err_t{
            std::string("Expected a ") + what + "; got " + datum.print(),
            query_state_t::FAILED};
        return false;
    }
    if (!value_out->assign_value(datum.as_str())) {
        *error_out = admin_err_t{
            datum.print() + " is not a valid " + what + "; "
            + std::string(name_string_t::valid_char_msg),
            query_state_t::FAILED};
        return false;
    }
    return true;
}

ql::datum_t convert_uuid_to_datum(
        const uuid_u &value) {
    return ql::datum_t(datum_string_t(uuid_to_str(value)));
}

bool convert_uuid_from_datum(
        ql::datum_t datum,
        uuid_u *value_out,
        admin_err_t *error_out) {
    if (datum.get_type() != ql::datum_t::R_STR) {
        *error_out = admin_err_t{
            "Expected a UUID; got " + datum.print(),
            query_state_t::FAILED};
        return false;
    }
    if (!str_to_uuid(datum.as_str().to_std(), value_out)) {
        *error_out = admin_err_t{
            "Expected a UUID; got " + datum.print(),
            query_state_t::FAILED};
        return false;
    }
    return true;
}

ql::datum_t convert_name_or_uuid_to_datum(
        const name_string_t &name,
        const uuid_u &uuid,
        admin_identifier_format_t identifier_format) {
    if (identifier_format == admin_identifier_format_t::name) {
        return convert_name_to_datum(name);
    } else {
        return convert_uuid_to_datum(uuid);
    }
}

#if STATS_REQUEST_IN_FDB
bool search_db_metadata_by_name(
        const databases_semilattice_metadata_t &metadata,
        const name_string_t &name,
        database_id_t *id_out,
        admin_err_t *error_out) {
    size_t found = 0;
    for (const auto &pair : metadata.databases) {
        if (!pair.second.is_deleted() && pair.second.get_ref().name.get_ref() == name) {
            *id_out = pair.first;
            ++found;
        }
    }
    if (found == 0) {
        *error_out = db_not_found_error(name);
        return false;
    } else if (found >= 2) {
        *error_out = admin_err_t{
            strprintf("Database `%s` is ambiguous; there are multiple "
                      "databases with that name.", name.c_str()),
            query_state_t::FAILED};
        return false;
    } else {
        return true;
    }
}

bool convert_table_id_to_datums(
        const namespace_id_t &table_id,
        admin_identifier_format_t identifier_format,
        const cluster_semilattice_metadata_t &metadata,
        table_meta_client_t *table_meta_client,
        /* Any of these can be `nullptr` if they are not needed */
        ql::datum_t *table_name_or_uuid_out,
        name_string_t *table_name_out,
        ql::datum_t *db_name_or_uuid_out,
        name_string_t *db_name_out) {
    table_basic_config_t basic_config;
    try {
        table_meta_client->get_name(table_id, &basic_config);
    } catch (const no_such_table_exc_t &) {
        return false;
    }
    if (table_name_or_uuid_out != nullptr) {
        *table_name_or_uuid_out = convert_name_or_uuid_to_datum(
            basic_config.name, table_id.value, identifier_format);
    }
    if (table_name_out != nullptr) *table_name_out = basic_config.name;
    name_string_t db_name;
    auto jt = metadata.databases.databases.find(basic_config.database);
    if (jt == metadata.databases.databases.end() || jt->second.is_deleted()) {
        db_name = name_string_t::guarantee_valid("__deleted_database__");
    } else {
        db_name = jt->second.get_ref().name.get_ref();
    }
    if (db_name_or_uuid_out != nullptr) {
        *db_name_or_uuid_out = convert_name_or_uuid_to_datum(
            db_name, basic_config.database.value, identifier_format);
    }
    if (db_name_out != nullptr) *db_name_out = db_name;
    return true;
}

bool convert_database_id_to_datum(
        const database_id_t &db_id,
        admin_identifier_format_t identifier_format,
        const cluster_semilattice_metadata_t &metadata,
        ql::datum_t *db_name_or_uuid_out,
        name_string_t *db_name_out) {
    auto it = metadata.databases.databases.find(db_id);
    guarantee(it != metadata.databases.databases.end());
    if (it->second.is_deleted()) {
        return false;
    }
    name_string_t db_name = it->second.get_ref().name.get_ref();
    if (db_name_or_uuid_out != nullptr) {
        *db_name_or_uuid_out =
            convert_name_or_uuid_to_datum(db_name, db_id.value, identifier_format);
    }
    if (db_name_out != nullptr) *db_name_out = db_name;
    return true;
}

bool convert_database_id_from_datum(
        const ql::datum_t &db_name_or_uuid,
        admin_identifier_format_t identifier_format,
        const cluster_semilattice_metadata_t &metadata,
        database_id_t *db_id_out,
        name_string_t *db_name_out,
        admin_err_t *error_out) {
    if (identifier_format == admin_identifier_format_t::name) {
        name_string_t name;
        if (!convert_name_from_datum(db_name_or_uuid, "database name",
                                     &name, error_out)) {
            return false;
        }
        database_id_t id;
        if (!search_db_metadata_by_name(metadata.databases, name, &id, error_out)) {
            return false;
        }
        if (db_id_out != nullptr) *db_id_out = id;
        if (db_name_out != nullptr) *db_name_out = name;
        return true;
    } else {
        database_id_t db_id;
        if (!convert_uuid_from_datum(db_name_or_uuid, &db_id.value, error_out)) {
            return false;
        }
        auto it = metadata.databases.databases.find(db_id);
        if (it == metadata.databases.databases.end() || it->second.is_deleted()) {
            *error_out = admin_err_t{
                strprintf("There is no database with UUID `%s`.",
                          uuid_to_str(db_id).c_str()),
                query_state_t::FAILED};
            return false;
        }
        if (db_id_out != nullptr) {
            *db_id_out = db_id;
        }
        if (db_name_out != nullptr) {
            *db_name_out = it->second.get_ref().name.get_ref();
        }
        return true;
    }
}
#endif  // STATS_REQUEST_IN_FDB

ql::datum_t convert_port_to_datum(
        int value) {
    return ql::datum_t(static_cast<double>(value));
}

ql::datum_t convert_microtime_to_datum(
        microtime_t value) {
    return ql::pseudo::make_time(value / 1.0e6, "+00:00");
}

bool converter_from_datum_object_t::init(
        ql::datum_t _datum,
        admin_err_t *error_out) {
    if (_datum.get_type() != ql::datum_t::R_OBJECT) {
        *error_out = admin_err_t{
            "Expected an object; got " + _datum.print(),
            query_state_t::FAILED};
        return false;
    }
    datum = _datum;
    for (size_t i = 0; i < datum.obj_size(); ++i) {
        std::pair<datum_string_t, ql::datum_t> pair = datum.get_pair(i);
        extra_keys.insert(pair.first);
    }
    return true;
}

bool converter_from_datum_object_t::get(
        const char *key,
        ql::datum_t *value_out,
        admin_err_t *error_out) {
    extra_keys.erase(datum_string_t(key));
    *value_out = datum.get_field(key, ql::NOTHROW);
    if (!value_out->has()) {
        *error_out = admin_err_t{
            strprintf("Expected a field named `%s`.", key),
            query_state_t::FAILED};
        return false;
    }
    return true;
}

void converter_from_datum_object_t::get_optional(
        const char *key,
        ql::datum_t *value_out) {
    extra_keys.erase(datum_string_t(key));
    *value_out = datum.get_field(key, ql::NOTHROW);
}

bool converter_from_datum_object_t::has(const char *key) {
    return datum.get_field(key, ql::NOTHROW).has();
}

bool converter_from_datum_object_t::check_no_extra_keys(admin_err_t *error_out) {
    if (!extra_keys.empty()) {
        *error_out = admin_err_t{"Unexpected key(s):", query_state_t::FAILED};
        for (const datum_string_t &key : extra_keys) {
            error_out->msg += " " + key.to_std();
        }
        return false;
    }
    return true;
}

