// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/administration/main/serve.hpp"

#include <stdio.h>

#include "arch/io/network.hpp"
#include "arch/os_signal.hpp"
#include "clustering/administration/artificial_reql_cluster_interface.hpp"
#include "clustering/administration/http/server.hpp"
#include "clustering/administration/logs/log_writer.hpp"
#include "clustering/administration/main/initial_join.hpp"
#include "clustering/administration/main/ports.hpp"
#include "clustering/administration/main/memory_checker.hpp"
#include "clustering/administration/main/watchable_fields.hpp"
#include "clustering/administration/metadata.hpp"
#include "clustering/administration/perfmon_collection_repo.hpp"
#include "clustering/administration/persist/file_keys.hpp"
#include "clustering/administration/real_reql_cluster_interface.hpp"
#include "containers/incremental_lenses.hpp"
#include "containers/lifetime.hpp"
#include "containers/optional.hpp"
#include "extproc/extproc_pool.hpp"
#include "fdb/node_holder.hpp"
#include "rdb_protocol/query_server.hpp"
#include "rpc/connectivity/cluster.hpp"
#include "rpc/semilattice/semilattice_manager.hpp"
#include "rpc/semilattice/view/field.hpp"

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

bool do_serve(FDBDatabase *fdb,
              metadata_file_t *metadata_file,
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

        auth_semilattice_metadata_t auth_metadata;
        server_id_t server_id = server_id_t();
        if (true) {
            cond_t non_interruptor;
            metadata_file_t::read_txn_t txn(metadata_file, &non_interruptor);
            auth_metadata = txn.read(mdkey_auth_semilattices());
        }

#ifndef NDEBUG
        logNTC("Our server ID is %s", server_id.print().c_str());
#endif

        fdb_node_holder node_holder{fdb, stop_cond};

        /* The `connectivity_cluster_t` maintains TCP connections to other servers in the
        cluster. */
        connectivity_cluster_t connectivity_cluster;

        // TODO: Hopefully, remove mailbox_manager_t.
        /* The `mailbox_manager_t` maintains a local index of mailboxes that exist on
        this server, and routes mailbox messages received from other servers. */
        mailbox_manager_t mailbox_manager(&connectivity_cluster, 'M');

        /* `connectivity_cluster_run` is the other half of the `connectivity_cluster_t`.
        Before it's created, the `connectivity_cluster_t` won't process any connections
        or messages. So it's only safe to create now that we've set up all of our message
        handlers. */
        scoped_ptr_t<connectivity_cluster_t::run_t> connectivity_cluster_run(
            new connectivity_cluster_t::run_t(
                &connectivity_cluster,
                server_id));

        perfmon_collection_repo_t perfmon_collection_repo(
            &get_global_perfmon_collection());

        /* We thread the `rdb_context_t` through every function that evaluates ReQL
        terms. It contains pointers to all the things that the ReQL term evaluation code
        needs. */
        rdb_context_t rdb_ctx(fdb,
                              &extproc_pool,
                              nullptr,   /* we'll fill this in later */
                              &get_global_perfmon_collection(),
                              serve_info.reql_http_proxy);
        {
            artificial_reql_cluster_interface_t artificial_reql_cluster_interface;

            /* The `real_reql_cluster_interface_t` is the interface that the ReQL logic
            uses to create, destroy, and reconfigure databases and tables. */
            real_reql_cluster_interface_t real_reql_cluster_interface;

            artificial_reql_cluster_interface.set_next_reql_cluster_interface(
                &real_reql_cluster_interface);

            artificial_reql_cluster_backends_t artificial_reql_cluster_backends(
                &artificial_reql_cluster_interface);

            /* Kick off a coroutine to log any outdated indexes. */
            // TODO: Do something like this at startup -- but make only one node do it, after there's a centralized logging infrastructure?
#if 0
            outdated_index_issue_tracker_t::log_outdated_indexes(
                multi_table_manager.get(),
                semilattice_manager_cluster.get_root_view()->get(),
                stop_cond);
#endif  // 0

            /* `real_reql_cluster_interface_t` needs access to the admin tables so that
            it can return rows from the `table_status` and `table_config` artificial
            tables when the user calls the corresponding porcelains. But
            `admin_artificial_tables_t` needs access to the
            `real_reql_cluster_interface_t` because `table_config` needs to be able to
            run distribution queries. The simplest solution is for them to have
            references to each other. This is the place where we "close the loop". */
            real_reql_cluster_interface.artificial_reql_cluster_interface =
                &artificial_reql_cluster_interface;

            /* `rdb_context_t` needs access to the `reql_cluster_interface_t` so that it
            can find tables and run meta-queries, but the `real_reql_cluster_interface_t`
            needs access to the `rdb_context_t` so that it can construct instances of
            `cluster_namespace_interface_t`. Again, we solve this problem by having a
            circular reference. Note that the cluster interface is a chain of command,
            the `artificial_reql_cluster_interface` proxies to the
            `real_reql_cluster_interface`. */
            rdb_ctx.cluster_interface = &artificial_reql_cluster_interface;

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
                    serve_info.ports.local_addresses_driver,
                    serve_info.ports.reql_port,
                    &rdb_ctx,
                    serve_info.tls_configs.driver.get());
                logNTC("Listening for client driver connections on port %d\n",
                       rdb_query_server.get_port());

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
                    }

                    std::string addresses_string =
                        serve_info.ports.get_addresses_string(
                            serve_info.ports.local_addresses_cluster);
                    logNTC("Listening on cluster address%s: %s\n",
                           serve_info.ports.local_addresses_cluster.size() == 1 ? "" : "es",
                           addresses_string.c_str());

                    addresses_string =
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
                        if(serve_info.config_file) {
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
                        // TODO: "theserver" in user output.
                        logNTC("Server ready, \"%s\" %s\n",
                               "theserver",
                               server_id.print().c_str());
                    } else {
                        logNTC("Proxy ready, %s", server_id.print().c_str());
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
    }

    return true;
}

bool serve(FDBDatabase *fdb,
           metadata_file_t *metadata_file,
           const serve_info_t &serve_info,
           os_signal_cond_t *stop_cond) {
    return do_serve(fdb,
                    metadata_file,
                    serve_info,
                    stop_cond);
}
