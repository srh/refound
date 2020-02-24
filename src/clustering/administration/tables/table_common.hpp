// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_TABLES_TABLE_COMMON_HPP_
#define CLUSTERING_ADMINISTRATION_TABLES_TABLE_COMMON_HPP_

#include <memory>
#include <string>
#include <vector>

#include "clustering/administration/admin_op_exc.hpp"
#include "rdb_protocol/admin_identifier_format.hpp"
#include "rdb_protocol/artificial_table/backend.hpp"

// TODO: There is only one subclass.  So just combine this all together.

class table_config_t;

/* This is a base class for the `rethinkdb.table_config` and `rethinkdb.table_status`
pseudo-tables. Subclasses should implement `format_row()` and `write_row()`. */

class common_table_artificial_table_fdb_backend_t :
    public artificial_table_fdb_backend_t
{
public:
    common_table_artificial_table_fdb_backend_t(
            name_string_t const &table_name,
            admin_identifier_format_t _identifier_format);

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

protected:
    virtual ql::datum_t format_row(
            const namespace_id_t &table_id,
            const table_config_t &config,
            /* In theory `db_name_or_uuid` can be computed from `config`, but the
            computation is non-trivial, so it's easier if `table_common` does it for all
            the subclasses. */
            const ql::datum_t &db_name_or_uuid) const = 0;

    const admin_identifier_format_t identifier_format;
};


#endif /* CLUSTERING_ADMINISTRATION_TABLES_TABLE_COMMON_HPP_ */

