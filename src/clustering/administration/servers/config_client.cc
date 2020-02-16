// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/administration/servers/config_client.hpp"

#include "clustering/administration/admin_op_exc.hpp"
#include "clustering/administration/metadata.hpp"
#include "rpc/mailbox/disconnect_watcher.hpp"

server_config_client_t::server_config_client_t(
        watchable_map_t<peer_id_t, cluster_directory_metadata_t> *_directory_view,
        watchable_map_t<std::pair<peer_id_t, server_id_t>, empty_value_t>
            *_peer_connections_map) :
    directory_view(_directory_view),
    peer_connections_map(_peer_connections_map),
    directory_subs(
        directory_view,
        std::bind(&server_config_client_t::on_directory_change, this, ph::_1, ph::_2),
        initial_call_t::YES),
    peer_connections_map_subs(
        peer_connections_map,
        std::bind(&server_config_client_t::on_peer_connections_map_change,
            this, ph::_1, ph::_2),
        initial_call_t::YES),
    server_connectivity_subscription(
        &connections_map,
        std::bind(&server_config_client_t::update_server_connectivity,
                  this,
                  ph::_1,
                  ph::_2),
        initial_call_t::YES)
    { }

void server_config_client_t::update_server_connectivity(
    const std::pair<server_id_t, server_id_t> &key,
    const empty_value_t *val) {
    if (val == nullptr) {
        // Entry removed
        if (--server_connectivity.all_servers[key.first] == 0) {
            server_connectivity.all_servers.erase(key.first);
        }
        if (--server_connectivity.all_servers[key.second] == 0) {
            server_connectivity.all_servers.erase(key.second);
        }
        server_connectivity.connected_to[key.first].erase(key.second);
        if (server_connectivity.connected_to[key.first].empty()) {
            server_connectivity.connected_to.erase(key.first);
        }
    } else {
        // Entry added
        ++server_connectivity.all_servers[key.first];
        ++server_connectivity.all_servers[key.second];
        server_connectivity.connected_to[key.first].insert(key.second);
    }
}

void server_config_client_t::install_server_metadata(
        const peer_id_t &peer_id,
        const cluster_directory_metadata_t &metadata) {
    const server_id_t &server_id = metadata.server_id;
    server_to_peer_map.set_key(server_id, peer_id);
    peer_connections_map->read_all(
        [&](const std::pair<peer_id_t, server_id_t> &pair, const empty_value_t *) {
            if (pair.first == peer_id) {
                connections_map.set_key(
                    std::make_pair(server_id, pair.second), empty_value_t());
            }
        });
}

void server_config_client_t::on_directory_change(
        const peer_id_t &peer_id,
        const cluster_directory_metadata_t *metadata) {
    if (metadata != nullptr) {
        // This is so we don't track servers inaccurately
        // if metadata updates after connections.
        peer_connections_map->read_all(
            [&] (const std::pair<peer_id_t, server_id_t> &connection,
                 const empty_value_t *value) {
                if (connection.first == peer_id) {
                    // This connection matches the directory view change
                    if (value != nullptr) {
                    connections_map.set_key(
                        std::make_pair(metadata->server_id, connection.second),
                        empty_value_t());
                    } else {
                        connections_map.delete_key(
                            std::make_pair(metadata->server_id, connection.second));
                    }
                }
            });

        if (metadata->peer_type != SERVER_PEER) {
            return;
        }
        const server_id_t &server_id = metadata->server_id;
        if (!peer_to_server_map.get_key(peer_id).has_value()) {
            all_server_to_peer_map.insert(std::make_pair(server_id, peer_id));
            peer_to_server_map.set_key(peer_id, server_id);
            install_server_metadata(peer_id, *metadata);
        }

    } else {
        optional<server_id_t> server_id = peer_to_server_map.get_key(peer_id);
        if (!server_id.has_value()) {
            return;
        }
        for (auto it = all_server_to_peer_map.lower_bound(*server_id); ; ++it) {
            guarantee(it != all_server_to_peer_map.end());
            if (it->second == peer_id) {
                all_server_to_peer_map.erase(it);
                break;
            }
        }
        peer_to_server_map.delete_key(peer_id);
        server_to_peer_map.delete_key(*server_id);
        std::vector<std::pair<server_id_t, server_id_t> > connection_pairs_to_delete;
        connections_map.read_all(
        [&](const std::pair<server_id_t, server_id_t> &pair, const empty_value_t *) {
            if (pair.first == *server_id) {
                connection_pairs_to_delete.push_back(pair);
            }
        });
        for (const auto &pair : connection_pairs_to_delete) {
            connections_map.delete_key(pair);
        }

        /* If there is another connected peer with the same server ID, reinstall its
        values. */
        auto jt = all_server_to_peer_map.find(*server_id);
        if (jt != all_server_to_peer_map.end()) {
            directory_view->read_key(jt->second,
                [&](const cluster_directory_metadata_t *other_metadata) {
                    guarantee(other_metadata != nullptr);
                    guarantee(other_metadata->server_id == *server_id);
                    install_server_metadata(jt->second, *other_metadata);
                });
        }
    }
}

void server_config_client_t::on_peer_connections_map_change(
        const std::pair<peer_id_t, server_id_t> &key,
        const empty_value_t *value) {
    directory_view->read_key(key.first,
    [&](const cluster_directory_metadata_t *metadata) {
        if (metadata != nullptr) {
            if (value != nullptr) {
                connections_map.set_key(
                    std::make_pair(metadata->server_id, key.second), empty_value_t());
            } else {
                connections_map.delete_key(
                    std::make_pair(metadata->server_id, key.second));
            }
        }
    });
}


