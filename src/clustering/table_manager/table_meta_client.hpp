// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_TABLE_MANAGER_TABLE_META_CLIENT_HPP_
#define CLUSTERING_TABLE_MANAGER_TABLE_META_CLIENT_HPP_

#include "btree/keys.hpp"
#include "concurrency/cross_thread_watchable.hpp"
#include "concurrency/watchable_map.hpp"
#include "containers/uuid.hpp"

enum class all_replicas_ready_mode_t;
class mailbox_manager_t;
class multi_table_manager_t;
class multi_table_manager_bcard_t;
class multi_table_manager_timestamp_t;
class multi_table_manager_timestamp_epoch_t;
class name_string_t;
class peer_id_t;
template <class edge_t, class value_t> class range_map_t;
class server_id_t;
class sindex_config_t;
class sindex_status_t;
class table_basic_config_t;
class table_config_and_shards_t;
class table_config_and_shards_change_t;
class table_raft_state_t;
class table_manager_bcard_t;
class table_shard_status_t;
class table_status_request_t;
class table_status_response_t;

/* These four exception classes are all thrown by `table_meta_client_t` to describe
different error conditions. There are several reasons why this is better than having
`table_meta_client_t` just throw an `admin_op_exc_t`:

 1. Sometimes we want to catch `no_such_table_exc_t` instead of reporting it to the user.
    For example, if we call `list_names()` and then run some other operation on each
    table in the list, we want to ignore any `no_such_table_exc_t`s thrown by that
    operation.

 2. `table_meta_client_t` usually doesn't have access to the name of the table. The
    caller will typically catch the exception at a point where the table name is known
    and then produce a user-readable error message that includes the table name.

 3. `table_meta_client_t` often doesn't have enough context to produce an optimal error
    message. The caller will typically add more context when it catches the exception and
    forwards it to the user. For example, instead of saying "the table's configuration
    was not modified" it can say "the secondary index was not renamed".

Note that the exception descriptions here are mostly just for documentation; unless there
is a bug, they will never be shown to the user. */

class no_such_table_exc_t : public std::runtime_error {
public:
    no_such_table_exc_t() :
        std::runtime_error("There is no table with the given name / UUID.") { }
};

class ambiguous_table_exc_t : public std::runtime_error {
public:
    ambiguous_table_exc_t() :
        std::runtime_error("There are multiple tables with the given name.") { }
};

class failed_table_op_exc_t : public std::runtime_error {
public:
    failed_table_op_exc_t() : std::runtime_error("The attempt to read or modify the "
        "table's configuration failed because none of the servers were accessible.  If "
        "it was an attempt to modify, the modification did not take place.") { }
};

class maybe_failed_table_op_exc_t : public std::runtime_error {
public:
    maybe_failed_table_op_exc_t() : std::runtime_error("The attempt to modify the "
        "table's configuration failed because we lost contact with the servers after "
        "initiating the modification, or the Raft leader lost contact with its "
        "followers, or we timed out while waiting for the changes to propagate.  The "
        "modification may or may not have taken place.") { }
};

class config_change_exc_t : public std::runtime_error {
public:
    config_change_exc_t() : std::runtime_error("The change could not be applied to the "
        "table's configuration.") { }
};

/* `table_meta_client_t` is responsible for submitting client requests over the network
to the `multi_table_manager_t`. It doesn't have any real state of its own; it's just a
convenient way of bundling together all of the objects that are necessary for submitting
a client request. */
class table_meta_client_t :
    public home_thread_mixin_t {
public:
    table_meta_client_t(
        mailbox_manager_t *_mailbox_manager,
        multi_table_manager_t *_multi_table_manager,
        watchable_map_t<peer_id_t, multi_table_manager_bcard_t>
            *_multi_table_manager_directory,
        watchable_map_t<std::pair<peer_id_t, namespace_id_t>, table_manager_bcard_t>
            *_table_manager_directory);
    ~table_meta_client_t();

    /* All of these functions can be called from any thread. */

    /* `find()` determines the ID of the table with the given name in the given database.
    */
    void find(
        const database_id_t &database, const name_string_t &name,
        namespace_id_t *table_id_out, std::string *primary_key_out = nullptr)
        THROWS_ONLY(no_such_table_exc_t, ambiguous_table_exc_t);

    /* `get_name()` determines the name, database, and primary key of the table with the
    given ID; it's the reverse of `find()`. It will not block. */
    void get_name(
        const namespace_id_t &table_id,
        table_basic_config_t *basic_config_out)
        THROWS_ONLY(no_such_table_exc_t);

    /* `list_configs()` fetches the configurations of every table at once. It may block.
    If it can't find a config for a certain table, then it puts the table's name and info
    into `disconnected_configs_out` instead. */
    void list_configs(
        const signal_t *interruptor,
        std::map<namespace_id_t, table_config_and_shards_t> *configs_out,
        std::map<namespace_id_t, table_basic_config_t> *disconnected_configs_out)
        THROWS_ONLY(interrupted_exc_t);

private:
    typedef std::pair<table_basic_config_t, multi_table_manager_timestamp_t>
        timestamped_basic_config_t;

    /* `get_status()` runs a status query. It can run for a specific table or every
    table. If `servers` is `EVERY_SERVER`, it runs against every server for the table(s);
    if `BEST_SERVER_ONLY`, it only runs against one server for each table, which will be
    the most up-to-date server that can be found. If it fails to contact at least one
    server for a given table, it calls `failure_callback()` on that table. If you specify
    a particular table and that table doesn't exist, it throws `no_such_table_exc_t`. */
    enum class server_selector_t { EVERY_SERVER, BEST_SERVER_ONLY };
    void get_status(
        const optional<namespace_id_t> &table,
        const table_status_request_t &request,
        server_selector_t servers,
        const signal_t *interruptor,
        const std::function<void(
            const server_id_t &server,
            const namespace_id_t &table,
            const table_status_response_t &response
            )> &callback,
        std::set<namespace_id_t> *failures_out)
        THROWS_ONLY(interrupted_exc_t);

    mailbox_manager_t *const mailbox_manager;
    multi_table_manager_t *const multi_table_manager;
    watchable_map_t<peer_id_t, multi_table_manager_bcard_t>
        *const multi_table_manager_directory;
    watchable_map_t<std::pair<peer_id_t, namespace_id_t>, table_manager_bcard_t>
        *const table_manager_directory;

    /* `table_basic_configs` distributes the `table_basic_config_t`s from the
    `multi_table_manager_t` to each thread, so that `find()`, `get_name()`, and
    `list_names()` can run without blocking. */
    all_thread_watchable_map_var_t<namespace_id_t, timestamped_basic_config_t>
        table_basic_configs;
};

#endif /* CLUSTERING_TABLE_MANAGER_TABLE_META_CLIENT_HPP_ */

