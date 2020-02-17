// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/table_manager/table_meta_client.hpp"

#include "clustering/generic/raft_core.tcc"
#include "clustering/table_manager/multi_table_manager.hpp"
#include "concurrency/cross_thread_signal.hpp"
#include "rpc/mailbox/disconnect_watcher.hpp"

table_meta_client_t::table_meta_client_t(
        mailbox_manager_t *_mailbox_manager,
        multi_table_manager_t *_multi_table_manager,
        watchable_map_t<peer_id_t, multi_table_manager_bcard_t>
            *_multi_table_manager_directory,
        watchable_map_t<std::pair<peer_id_t, namespace_id_t>, table_manager_bcard_t>
            *_table_manager_directory) :
    mailbox_manager(_mailbox_manager),
    multi_table_manager(_multi_table_manager),
    multi_table_manager_directory(_multi_table_manager_directory),
    table_manager_directory(_table_manager_directory),
    table_basic_configs(multi_table_manager->get_table_basic_configs())
    { }

table_meta_client_t::~table_meta_client_t() {}

void table_meta_client_t::find(
        const database_id_t &database,
        const name_string_t &name,
        namespace_id_t *table_id_out,
        std::string *primary_key_out)
        THROWS_ONLY(no_such_table_exc_t, ambiguous_table_exc_t) {
    size_t count = 0;
    table_basic_configs.get_watchable()->read_all(
        [&](const namespace_id_t &key, const timestamped_basic_config_t *value) {
            if (value->first.database == database && value->first.name == name) {
                ++count;
                *table_id_out = key;
                if (primary_key_out != nullptr) {
                    *primary_key_out = value->first.primary_key;
                }
            }
        });
    if (count == 0) {
        throw no_such_table_exc_t();
    } else if (count >= 2) {
        throw ambiguous_table_exc_t();
    }
}

void table_meta_client_t::get_name(
        const namespace_id_t &table_id,
        table_basic_config_t *basic_config_out)
        THROWS_ONLY(no_such_table_exc_t) {
    table_basic_configs.get_watchable()->read_key(table_id,
        [&](const timestamped_basic_config_t *value) {
            if (value == nullptr) {
                throw no_such_table_exc_t();
            }
            *basic_config_out = value->first;
        });
}

void table_meta_client_t::list_configs(
        const signal_t *interruptor_on_caller,
        std::map<namespace_id_t, table_config_and_shards_t> *configs_out,
        std::map<namespace_id_t, table_basic_config_t> *disconnected_configs_out)
        THROWS_ONLY(interrupted_exc_t) {
    cross_thread_signal_t interruptor(interruptor_on_caller, home_thread());
    on_thread_t thread_switcher(home_thread());
    configs_out->clear();
    table_status_request_t request;
    request.want_config = true;
    std::set<namespace_id_t> failures;
    get_status(
        r_nullopt,
        request,
        server_selector_t::BEST_SERVER_ONLY,
        &interruptor,
        [&](const server_id_t &, const namespace_id_t &table_id,
                const table_status_response_t &response) {
            configs_out->insert(std::make_pair(table_id, *response.config));
        },
        &failures);
    for (const namespace_id_t &table_id : failures) {
        multi_table_manager->get_table_basic_configs()->read_key(table_id,
        [&](const timestamped_basic_config_t *value) {
            if (value != nullptr) {
                disconnected_configs_out->insert(std::make_pair(table_id, value->first));
            }
        });
    }
}

/* `best_server_rank_t` is used for comparing servers when we're in `BEST_SERVER_ONLY`
mode. */
class best_server_rank_t {
public:
    /* Note that a default-constructed `best_server_rank_t` is superseded by any other
    `best_server_rank_t`. */
    best_server_rank_t() :
        is_leader(false), timestamp(multi_table_manager_timestamp_t::min()) { }
    best_server_rank_t(bool il, const multi_table_manager_timestamp_t ts) :
        is_leader(il), timestamp(ts) { }

    bool supersedes(const best_server_rank_t &other) const {
        /* Ordered comparison first on `timestamp.epoch`, then `is_leader`, then
        `timestamp.log_index`. This is slightly convoluted because `timestamp.epoch` is
        compared with `supersedes()` rather than `operator<()`. */
        return timestamp.epoch.supersedes(other.timestamp.epoch) ||
            (timestamp.epoch == other.timestamp.epoch &&
                std::tie(is_leader, timestamp.log_index) >
                    std::tie(other.is_leader, other.timestamp.log_index));
    }

    bool is_leader;
    multi_table_manager_timestamp_t timestamp;
};

void table_meta_client_t::get_status(
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
        THROWS_ONLY(interrupted_exc_t) {
    assert_thread();
    interruptor->assert_thread();

    /* Assemble a set of all the tables we need information for. As we get information
    for each table, we'll remove it from the set; we'll use this to track which tables
    we need to retry for. */
    std::set<namespace_id_t> tables_todo;
    if (static_cast<bool>(table)) {
        tables_todo.insert(*table);
    } else {
        table_basic_configs.get_watchable()->read_all(
            [&](const namespace_id_t &table_id, const timestamped_basic_config_t *) {
                tables_todo.insert(table_id);
            });
    }

    /* If we're in `BEST_SERVER_ONLY` mode, there's a risk that the table will move off
    the server we selected before we get a chance to run the query, which could cause
    spurious failures. So in that mode we try again if the first try fails. */
    int tries_left = servers == server_selector_t::BEST_SERVER_ONLY ? 2 : 1;

    while (!tables_todo.empty() && tries_left > 0) {
        --tries_left;

        /* Assemble a list of which servers we need to send messages to, and which tables
        we want from each server. */
        std::map<peer_id_t, std::set<namespace_id_t> > targets;
        switch (servers) {
            case server_selector_t::EVERY_SERVER: {
                table_manager_directory->read_all(
                [&](const std::pair<peer_id_t, namespace_id_t> &key,
                        const table_manager_bcard_t *) {
                    if (tables_todo.count(key.second) == 1) {
                        targets[key.first].insert(key.second);
                    }
                });
                break;
            }
            case server_selector_t::BEST_SERVER_ONLY: {
                std::map<namespace_id_t, peer_id_t> best_servers;
                std::map<namespace_id_t, best_server_rank_t> best_ranks;
                table_manager_directory->read_all(
                [&](const std::pair<peer_id_t, namespace_id_t> &key,
                        const table_manager_bcard_t *bcard) {
                    if (tables_todo.count(key.second) == 1) {
                        best_server_rank_t rank(
                            static_cast<bool>(bcard->leader), bcard->timestamp);
                        /* This relies on the fact that a default-constructed
                        `best_server_rank_t` is always superseded */
                        if (rank.supersedes(best_ranks[key.second])) {
                            best_ranks[key.second] = rank;
                            best_servers[key.second] = key.first;
                        }
                    }
                });
                for (const auto &pair : best_servers) {
                    targets[pair.second].insert(pair.first);
                }
                break;
            }
        }

        /* Dispatch all the messages in parallel. Probably this should really be a
        `throttled_pmap` instead. */
        pmap(targets.begin(), targets.end(),
        [&](const std::pair<peer_id_t, std::set<namespace_id_t> > &target) {
            optional<multi_table_manager_bcard_t> bcard =
                multi_table_manager_directory->get_key(target.first);
            if (!bcard.has_value()) {
                return;
            }
            disconnect_watcher_t dw(mailbox_manager,
                bcard->get_status_mailbox.get_peer());
            cond_t got_ack;
            mailbox_t<std::map<namespace_id_t, table_status_response_t>>
            ack_mailbox(mailbox_manager,
                [&](const signal_t *, const std::map<
                        namespace_id_t, table_status_response_t> &resp) {
                    for (const auto &pair : resp) {
                        callback(bcard->server_id, pair.first, pair.second);
                        tables_todo.erase(pair.first);
                    }
                    got_ack.pulse();
                });
            send(mailbox_manager, bcard->get_status_mailbox,
                 {target.second, request, ack_mailbox.get_address()});
            wait_any_t waiter(&dw, interruptor, &got_ack);
            waiter.wait_lazily_unordered();
        });

        if (interruptor->is_pulsed()) {
            throw interrupted_exc_t();
        }
    }

    *failures_out = std::move(tables_todo);
}

