// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_TABLES_SERVER_STATUS_HPP_
#define CLUSTERING_ADMINISTRATION_TABLES_SERVER_STATUS_HPP_

#include <memory>
#include <string>
#include <vector>

#include "rdb_protocol/admin_identifier_format.hpp"
#include "rdb_protocol/artificial_table/backend.hpp"

class server_status_artificial_table_fdb_backend_t :
    public artificial_table_fdb_backend_t
{
public:
    explicit server_status_artificial_table_fdb_backend_t(
            admin_identifier_format_t _identifier_format);
    ~server_status_artificial_table_fdb_backend_t();

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
    const admin_identifier_format_t identifier_format;
};

#endif /* CLUSTERING_ADMINISTRATION_TABLES_SERVER_STATUS_HPP_ */

