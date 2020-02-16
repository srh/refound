// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_AUTH_PERMISSIONS_ARTIFICIAL_TABLE_BACKEND_HPP
#define CLUSTERING_ADMINISTRATION_AUTH_PERMISSIONS_ARTIFICIAL_TABLE_BACKEND_HPP

#include "rdb_protocol/admin_identifier_format.hpp"
#include "rdb_protocol/artificial_table/backend.hpp"

namespace auth {
class username_t;
class permissions_t;

/* This backend is basically a join on the users and tables system tables. */
class permissions_artificial_table_fdb_backend_t :
    public artificial_table_fdb_backend_t
{
public:
    explicit permissions_artificial_table_fdb_backend_t(
        admin_identifier_format_t identifier_format);

    std::string get_primary_key_name() const override;

    bool read_all_rows_as_vector(
        FDBDatabase *fdb,
        auth::user_context_t const &user_context,
        const signal_t *interruptor,
        std::vector<ql::datum_t> *rows_out,
        admin_err_t *error_out) override;

    bool read_row(
        FDBTransaction *txn,
        auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        const signal_t *interruptor,
        ql::datum_t *row_out,
        admin_err_t *error_out) override;

    bool write_row(
        FDBTransaction *txn,
        auth::user_context_t const &user_context,
        ql::datum_t primary_key,
        bool pkey_was_autogenerated,
        ql::datum_t *new_value_inout,
        UNUSED const signal_t *interruptor,
        admin_err_t *error_out) override;

private:
    static uint8_t parse_primary_key(
        const signal_t *interruptor,
        FDBTransaction *txn,
        ql::datum_t const &primary_key,
        username_t *username_out,
        database_id_t *database_id_out,
        namespace_id_t *table_id_out,
        admin_err_t *admin_err_out = nullptr);

    static bool global_to_datum(
        username_t const &username,
        permissions_t const &permissions,
        ql::datum_t *datum_out);

    bool database_to_datum(
        username_t const &username,
        database_id_t const &database_id,
        permissions_t const &permissions,
        ql::datum_t *datum_out);

    bool table_to_datum(
        username_t const &username,
        database_id_t const &database_id,
        namespace_id_t const &table_id,
        permissions_t const &permissions,
        ql::datum_t *datum_out);

private:
    admin_identifier_format_t m_identifier_format;
};

}  // namespace auth

#endif  // CLUSTERING_ADMINISTRATION_AUTH_PERMISSIONS_ARTIFICIAL_TABLE_BACKEND_HPP
