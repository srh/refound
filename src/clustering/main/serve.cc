// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/main/serve.hpp"

#include <stdio.h>

#include "arch/io/network.hpp"
#include "arch/os_signal.hpp"
#include "clustering/artificial_reql_cluster_interface.hpp"
#include "clustering/http/server.hpp"
#include "clustering/logs/log_writer.hpp"
#include "clustering/main/ports.hpp"
#include "clustering/main/memory_checker.hpp"
#include "clustering/perfmon_collection_repo.hpp"
#include "clustering/real_reql_cluster_interface.hpp"
#include "containers/incremental_lenses.hpp"
#include "containers/lifetime.hpp"
#include "containers/optional.hpp"
#include "extproc/extproc_pool.hpp"
#include "fdb/node_holder.hpp"
#include "rdb_protocol/query_server.hpp"

peer_address_set_t look_up_peers_addresses(const std::vector<host_and_port_t> &names) {
    peer_address_set_t peers;
    for (size_t i = 0; i < names.size(); ++i) {
        peer_address_t peer(std::set<host_and_port_t>{names[i]});
        if (peers.find(peer) != peers.end()) {
            logWRN("Duplicate peer in --join parameters, ignoring: '%s:%d'",
                   names[i].host().c_str(), names[i].port().value());
        } else {
            peers.insert(peer);
        }
    }
    return peers;
}

std::string service_address_ports_t::get_addresses_string(
    std::set<ip_address_t> actual_addresses) {

    bool first = true;
    std::string result;

    // Get the actual list for printing if we're listening on all addresses.
    if (is_bind_all(actual_addresses)) {
        actual_addresses = get_local_ips(std::set<ip_address_t>(),
                                         local_ip_filter_t::ALL);
    }

    for (std::set<ip_address_t>::const_iterator i = actual_addresses.begin(); i != actual_addresses.end(); ++i) {
        result += (first ? "" : ", " ) + i->to_string();
        first = false;
    }

    return result;
}

bool service_address_ports_t::is_bind_all(const std::set<ip_address_t> &addresses) {
    // If the set is empty, it means we're listening on all addresses.
    return addresses.empty();
}

#ifdef _WIN32
std::string windows_version_string();
#else
// Defined in command_line.cc; not in any header, because it is not
// safe to run in general.
std::string run_uname(const std::string &flags);
#endif

proc_metadata_info make_proc_metadata(const serve_info_t &serve_info) {
    return proc_metadata_info{
        RETHINKDB_VERSION_STR,
        current_microtime(),
        getpid(),
        str_gethostname(),
        /* Note we'll update `reql_port` and `http_port` later, once final values
           are available */

        // NNN: We need to do this, with the value of rdb_query_server_t::get_port() and
        // administrative_http_server_manager_t::get_port() once they get constructed
        // later.  Maybe we should open their sockets early, then pass those sockets into
        // the query server classes, so we can one-time construct this serve_info port
        // value (avoiding logic to update it later).


        // TODO: Why is the serve_info value an int, and not a uint16_t?
        static_cast<uint16_t>(serve_info.ports.reql_port),
        serve_info.ports.http_admin_is_disabled
            ? optional<uint16_t>()
            : optional<uint16_t>(static_cast<uint16_t>(serve_info.ports.http_port)),
        serve_info.argv,
    };
}

bool serve(FDBDatabase *fdb,
        const serve_info_t &serve_info,
        os_signal_cond_t *stop_cond) {
    // Vestigial proxy code exists.
    const bool i_am_a_server = true;
    /* This coroutine is responsible for creating and destroying most of the important
    components of the server. */

    // Do this here so we don't block on popen while pretending to serve.
#ifdef _WIN32
    std::string uname = windows_version_string();
#else
    std::string uname = run_uname("ms");
#endif
    try {
        /* `extproc_pool` spawns several subprocesses that can be used to run tasks that
        we don't want to run in the main RethinkDB process, such as Javascript
        evaluations. */
        // TODO: Consider bringing v8 JS exec in-process.
        extproc_pool_t extproc_pool(get_num_threads());

        // TODO: Strip out stuff unnecessary with fdb.

        /* `thread_pool_log_writer_t` automatically registers itself. While it exists,
        log messages will be written using the event loop instead of blocking. */
        thread_pool_log_writer_t log_writer;

        proc_metadata_info proc_metadata = make_proc_metadata(serve_info);
        fdb_node_holder node_holder{fdb, stop_cond, proc_metadata,
            serve_info.node_id, serve_info.expected_cluster_id};

        // The node id could be non-ephemeral (we could reuse it after a node goes down
        // and back up) for what it's worth.
        logNTC("Node ID is %s",
            uuid_to_str(node_holder.get_node_id().value).c_str());

        perfmon_collection_repo_t perfmon_collection_repo(
            &get_global_perfmon_collection());

        artificial_reql_cluster_interface_t artificial_reql_cluster_interface;

        /* We thread the `rdb_context_t` through every function that evaluates ReQL
        terms. It contains pointers to all the things that the ReQL term evaluation code
        needs. */
        rdb_context_t rdb_ctx(fdb,
                              &node_holder,
                              &extproc_pool,
                              &artificial_reql_cluster_interface,
                              &get_global_perfmon_collection(),
                              serve_info.reql_http_proxy);
        {

            /* Kick off a coroutine to log any outdated indexes. */
            // TODO: Do something like this at startup -- but make only one node do it, after there's a centralized logging infrastructure?
#if 0
            outdated_index_issue_tracker_t::log_outdated_indexes(
                multi_table_manager.get(),
                semilattice_manager_cluster.get_root_view()->get(),
                stop_cond);
#endif  // 0

            /* `memory_checker` periodically checks to see if we are using swap
                    memory, and will log a warning. */
            scoped_ptr_t<memory_checker_t> memory_checker;
            if (i_am_a_server) {
                memory_checker.init(new memory_checker_t());
            }

            {
                /* The `rdb_query_server_t` listens for client requests and processes the
                queries it receives. */
                rdb_query_server_t rdb_query_server(
                    node_holder.get_node_id(),
                    serve_info.ports.local_addresses_driver,
                    serve_info.ports.reql_port,
                    &rdb_ctx,
                    serve_info.tls_configs.driver.get());
                logNTC("Listening for client driver connections on port %d\n",
                       rdb_query_server.get_port());
                node_holder.update_proc_metadata([&](proc_metadata_info *info) -> bool {
                    auto old = info->reql_port;
                    info->reql_port = rdb_query_server.get_port();
                    return old != info->reql_port;
                });

                {
                    /* The `administrative_http_server_manager_t` serves the web UI. */
                    scoped_ptr_t<administrative_http_server_manager_t> admin_server_ptr;
                    if (serve_info.ports.http_admin_is_disabled) {
                        logNTC("Administrative HTTP connections are disabled.\n");
                    } else {
                        // TODO: Pardon me what, but is this how we fail here?
                        guarantee(serve_info.ports.http_port < 65536);
                        admin_server_ptr.init(
                            new administrative_http_server_manager_t(
                                serve_info.ports.local_addresses_http,
                                serve_info.ports.http_port,
                                rdb_query_server.get_http_app(),
                                serve_info.web_assets,
                                serve_info.tls_configs.web.get()));
                        logNTC("Listening for administrative HTTP connections on port %d\n",
                               admin_server_ptr->get_port());
                        node_holder.update_proc_metadata([&](proc_metadata_info *info) -> bool {
                            auto old = info->http_admin_port;
                            info->http_admin_port = optional<uint16_t>(admin_server_ptr->get_port());
                            return old != info->http_admin_port;
                        });
                    }

#if 0
                    // We don't listen on "cluster" addresses -- for peer node connections
                    // -- in ReFound.  At least, not yet (maybe dynamic query eval could
                    // be distributed over nodes that register themselves in
                    // FoundationDB), so this code is still left commented out.
                    std::string addresses_string =
                        serve_info.ports.get_addresses_string(
                            serve_info.ports.local_addresses_cluster);
                    logNTC("Listening on cluster address%s: %s\n",
                           serve_info.ports.local_addresses_cluster.size() == 1 ? "" : "es",
                           addresses_string.c_str());
#endif

                    std::string addresses_string =
                        serve_info.ports.get_addresses_string(
                            serve_info.ports.local_addresses_driver);
                    logNTC("Listening on driver address%s: %s\n",
                           serve_info.ports.local_addresses_driver.size() == 1 ? "" : "es",
                           addresses_string.c_str());

                    addresses_string =
                        serve_info.ports.get_addresses_string(
                            serve_info.ports.local_addresses_http);
                    logNTC("Listening on http address%s: %s\n",
                           serve_info.ports.local_addresses_http.size() == 1 ? "" : "es",
                           addresses_string.c_str());

                    if (!service_address_ports_t::is_bind_all(serve_info.ports.local_addresses)) {
                        if (serve_info.config_file.has_value()) {
                            logNTC("To fully expose RethinkDB on the network, bind to "
                                   "all addresses by adding `bind=all' to the config "
                                   "file (%s).", (*serve_info.config_file).c_str());
                        } else {
                            logNTC("To fully expose RethinkDB on the network, bind to "
                                   "all addresses by running rethinkdb with the `--bind "
                                   "all` command line option.");
                        }
                    }

                    if (i_am_a_server) {
                        logNTC("Server ready, ephemeral node id %s\n",
                               uuid_to_str(node_holder.get_node_id().value).c_str());
                    } else {
                        logNTC("Proxy ready.\n");
                    }

                    /* This is the end of the startup process. `stop_cond` will be pulsed
                    when it's time for the server to shut down. */
                    stop_cond->wait_lazily_unordered();
                    logNTC("Server got %s; shutting down...", stop_cond->format().c_str());
                }

                logNTC("Shutting down client connections...\n");
            }
            logNTC("All client connections closed.\n");
            // TODO: Review this message.
            logNTC("Shutting down storage engine... (This may take a while if you had a lot of unflushed data in the writeback cache.)\n");
        }
        // TODO: Review this message.  There is no storage engine.
        logNTC("Storage engine shut down.\n");

        cond_t non_interruptor;  // TODO?
        node_holder.shutdown(&non_interruptor);
    } catch (const address_in_use_exc_t &ex) {
        logERR("%s.\n", ex.what());
        return false;
    } catch (const tcp_socket_exc_t &ex) {
        logERR("%s.\n", ex.what());
        return false;
    } catch (const interrupted_exc_t &ex) {
        // fdb_node_holder stop_cond param
        return false;
    } catch (const startup_failed_exc_t &ex) {
        return false;
    }

    return true;
}
