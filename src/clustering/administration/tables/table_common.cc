// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/administration/tables/table_common.hpp"

#include "clustering/administration/auth/user_fut.hpp"
#include "clustering/administration/datum_adapter.hpp"
#include "clustering/administration/tables/table_metadata.hpp"
#include "fdb/retry_loop.hpp"
#include "fdb/system_tables.hpp"

common_table_artificial_table_fdb_backend_t::common_table_artificial_table_fdb_backend_t(
        name_string_t const &table_name,
        admin_identifier_format_t _identifier_format)
    : artificial_table_fdb_backend_t(table_name),
      identifier_format(_identifier_format) {
}

bool common_table_artificial_table_fdb_backend_t::read_all_rows_as_vector(
        FDBDatabase *fdb,
        auth::user_context_t const &user_context,
        const signal_t *interruptor,
        std::vector<ql::datum_t> *rows_out,
        UNUSED admin_err_t *error_out) {
    // QQQ: Did we need to use user_context_t here?

    // TODO: Break into parts...
    std::string prefix = table_config_by_id::prefix;
    std::string pend = prefix_end(prefix);
    std::vector<std::pair<namespace_id_t, table_config_t>> configs;
    fdb_error_t loop_err = txn_retry_loop_coro(fdb, interruptor,
            [&](FDBTransaction *txn) {
        // TODO: Might be worth making a typed version of this read_whole_range func -- similar code to config_cache_db_list_sorted_by_id.
        auth::fdb_user_fut<auth::read_permission> auth_fut = get_read_permission(txn, user_context);
        std::vector<std::pair<namespace_id_t, table_config_t>> builder;
        transaction_read_whole_range_coro(txn, prefix, pend, interruptor,
                [&](const FDBKeyValue &kv) {
            key_view whole_key{void_as_uint8(kv.key), kv.key_length};
            key_view key = whole_key.guarantee_without_prefix(prefix);
            namespace_id_t table_id = table_config_by_id::parse_ukey(key);
            table_config_by_id::value_type config;
            deserialize_off_fdb(void_as_uint8(kv.value), kv.value_length, &config);
            builder.emplace_back(table_id, std::move(config));
            return true;
        });
        auth_fut.block_and_check(interruptor);
        configs = std::move(builder);
    });
    guarantee_fdb_TODO(loop_err, "common_table_artificial_table_fdb_backend_t::"
        "read_all_rows_as_vector retry loop");

    std::vector<ql::datum_t> rows;
    rows.reserve(configs.size());
    for (auto &pair : configs) {
        // QQQ: Make use of identifier_format, and handle db_drop in-progress case.
        ql::datum_t db_name_or_uuid = convert_uuid_to_datum(pair.second.basic.database.value);
        rows.push_back(format_row(
            pair.first,
            pair.second,
            db_name_or_uuid));
    }
    *rows_out = std::move(rows);
    return true;
}

bool common_table_artificial_table_fdb_backend_t::read_row(
        FDBTransaction *txn,
        UNUSED auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        const signal_t *interruptor,
        ql::datum_t *row_out,
        UNUSED admin_err_t *error_out) {
    // TODO: Did we need to use user_context here?
    namespace_id_t table_id;
    admin_err_t dummy_error;
    if (!convert_uuid_from_datum(primary_key, &table_id.value, &dummy_error)) {
        /* If the primary key was not a valid UUID, then it must refer to a nonexistent
        row. */
        *row_out = ql::datum_t::null();
        return true;
    }

    // QQQ: This excludes the system tables, right?
    fdb_value_fut<table_config_t> config_fut
        = transaction_lookup_uq_index<table_config_by_id>(txn, table_id);

    table_config_t config;
    if (!config_fut.block_and_deserialize(interruptor, &config)) {
        *row_out = ql::datum_t::null();
        return true;
    }

    // QQQ: Make use of identifier_format, and handle db_drop in-progress case.
    ql::datum_t db_name_or_uuid = convert_uuid_to_datum(config.basic.database.value);
    *row_out = format_row(
        table_id,
        config,
        db_name_or_uuid);
    return true;
}

