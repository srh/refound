// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_LOGS_LOGS_BACKEND_HPP_
#define CLUSTERING_ADMINISTRATION_LOGS_LOGS_BACKEND_HPP_

#include <map>
#include <set>
#include <string>
#include <vector>

#include "rdb_protocol/artificial_table/caching_cfeed_backend.hpp"
#include "rdb_protocol/context.hpp"
#include "rpc/connectivity/peer_id.hpp"

class cluster_directory_metadata_t;
class log_message_t;
class log_server_business_card_t;
class server_config_client_t;
class server_id_t;
template <class T> class lifetime_t;
class name_resolver_t;

template <class key_t, class value_t> class watchable_map_t;

/* This backend assumes that the entries in the log file have timestamps that are unique
and monotonically increasing. These assumptions can be broken if the system clock runs
backwards while the server is turned off, or if the user manually edits the log file. If
these assumptions are broken, the system shouldn't crash, but the contents of
`rethinkdb.logs` are undefined. In particular, the following things will go wrong:
  * The `rethinkdb.logs` table might have multiple entries with the same primary key.
  * Log entries might appear in `r.db("rethinkdb").table("logs")` but not if the user
    runs `r.db("rethinkdb").table("logs").get(primary_key)`
  * Changefeeds on `rethinkdb.logs` might skip some changes.
*/

namespace logs_backend {
class cfeed_machinery_t;
}

class logs_artificial_table_backend_t :
#if RDB_CF
    public cfeed_artificial_table_backend_t
#else
    public artificial_table_backend_t
#endif
{
public:
    logs_artificial_table_backend_t(
            lifetime_t<name_resolver_t const &> name_resolver,
            mailbox_manager_t *_mailbox_manager,
            watchable_map_t<peer_id_t, cluster_directory_metadata_t> *_directory,
            server_config_client_t *_server_config_client,
            admin_identifier_format_t _identifier_format);
    ~logs_artificial_table_backend_t();

    std::string get_primary_key_name();

    bool read_all_rows_as_vector(
            auth::user_context_t const &user_context,
            const signal_t *interruptor,
            std::vector<ql::datum_t> *rows_out,
            admin_err_t *error_out);

    bool read_row(
            auth::user_context_t const &user_context,
            ql::datum_t primary_key,
            const signal_t *interruptor,
            ql::datum_t *row_out,
            admin_err_t *error_out);

    bool write_row(
            auth::user_context_t const &user_context,
            ql::datum_t primary_key,
            bool pkey_was_autogenerated,
            ql::datum_t *new_value_inout,
            const signal_t *interruptor,
            admin_err_t *error_out);

private:
    friend class logs_backend::cfeed_machinery_t;

    bool read_all_rows_raw(
        const std::function<void(
            const log_message_t &message,
            const peer_id_t &peer_id,
            const server_id_t &server_id,
            const ql::datum_t &server_name_datum)> &callback,
        const signal_t *interruptor,
        admin_err_t *error_out);

#if RDB_CF
    scoped_ptr_t<cfeed_artificial_table_backend_t::machinery_t>
        construct_changefeed_machinery(
            lifetime_t<name_resolver_t const &> name_resolver,
            auth::user_context_t const &user_context,
            const signal_t *interruptor);
#endif

    mailbox_manager_t *mailbox_manager;
    watchable_map_t<peer_id_t, cluster_directory_metadata_t> *directory;
    server_config_client_t *server_config_client;
    admin_identifier_format_t identifier_format;
};

#endif /* CLUSTERING_ADMINISTRATION_LOGS_LOGS_BACKEND_HPP_ */
