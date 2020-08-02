// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_TABLES_TABLE_CONFIG_HPP_
#define CLUSTERING_ADMINISTRATION_TABLES_TABLE_CONFIG_HPP_

#include <memory>
#include <string>
#include <vector>

#include "containers/uuid.hpp"
#include "rdb_protocol/admin_identifier_format.hpp"
#include "rdb_protocol/artificial_table/backend.hpp"

class table_config_t;
class write_hook_config_t;

/* This is publicly exposed so that it can be used to create the return value of
`table.reconfigure()`. */
ql::datum_t convert_table_config_to_datum(
        namespace_id_t table_id,
        const ql::datum_t &db_name_or_uuid,
        const table_config_t &config);

ql::datum_t convert_write_hook_to_datum(
    const optional<write_hook_config_t> &write_hook);

class table_config_artificial_table_fdb_backend_t :
    public artificial_table_fdb_backend_t
{
public:
    explicit table_config_artificial_table_fdb_backend_t(
            admin_identifier_format_t _identifier_format);
    ~table_config_artificial_table_fdb_backend_t();

    bool read_all_rows_as_vector(
            FDBDatabase *fdb,
            auth::user_context_t const &user_context,
            const signal_t *interruptor_on_caller,
            std::vector<ql::datum_t> *rows_out,
            admin_err_t *error_out) override;

    bool read_row(
            FDBTransaction *txn,
            auth::user_context_t const &user_context,
            ql::datum_t primary_key,
            const signal_t *interruptor_on_caller,
            ql::datum_t *row_out,
            admin_err_t *error_out) override;

    bool write_row(
            FDBTransaction *txn,
            auth::user_context_t const &user_context,
            ql::datum_t primary_key,
            bool pkey_was_autogenerated,
            ql::datum_t *new_value_inout,
            const signal_t *interruptor_on_caller,
            admin_err_t *error_out) override;

private:
    ql::datum_t format_row(
            const namespace_id_t &table_id,
            const table_config_t &config,
            const ql::datum_t &db_name_or_uuid) const;

    const admin_identifier_format_t identifier_format;
};

#endif /* CLUSTERING_ADMINISTRATION_TABLES_TABLE_CONFIG_HPP_ */

