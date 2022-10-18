// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CLUSTERING_MAIN_SERVE_HPP_
#define CLUSTERING_MAIN_SERVE_HPP_

#include <set>
#include <string>
#include <utility>
#include <vector>
#include <memory>

#include "arch/address.hpp"
#include "arch/io/openssl.hpp"
#include "containers/optional.hpp"
#include "fdb/fdb.hpp"
#include "fdb/id_types.hpp"
#include "utils.hpp"

class os_signal_cond_t;

// Used to throw failure in startup operations, handled by serve.cc, with errors _already_
// logged, silent exit.
class startup_failed_exc_t : public std::exception {
public:
    const char *what() const noexcept {
        return "startup failed";
    }
};

class invalid_port_exc_t : public std::exception {
public:
    invalid_port_exc_t(const std::string& name, int port, int port_offset) {
        if (port_offset == 0) {
            info = strprintf("%s has a value (%d) above the maximum allowed port (%d).",
                             name.c_str(), port, MAX_PORT);
        } else {
            info = strprintf("%s has a value (%d) above the maximum allowed port (%d)."
                             " Note port_offset is set to %d which may cause this error.",
                             name.c_str(), port, MAX_PORT, port_offset);
        }
    }
    ~invalid_port_exc_t() throw () { }
    const char *what() const throw () {
        return info.c_str();
    }
private:
    std::string info;
};

inline void sanitize_port(int port, const char *name, int port_offset) {
    if (port >= MAX_PORT) {
        throw invalid_port_exc_t(name, port, port_offset);
    }
}

struct service_address_ports_t {
    service_address_ports_t() :
        // port(0),
        // client_port(0),
        http_port(0),
        reql_port(0),
        port_offset(0) { }

    service_address_ports_t(const std::set<ip_address_t> &_local_addresses,
        // We don't have this in ReFound... yet.
        // const std::set<ip_address_t> &_local_addresses_cluster,
                            const std::set<ip_address_t> &_local_addresses_driver,
                            const std::set<ip_address_t> &_local_addresses_http,
        // const peer_address_t &_canonical_addresses,
        // int _port,
        // int _client_port,
                            bool _http_admin_is_disabled,
                            int _http_port,
                            int _reql_port,
                            int _port_offset) :
        local_addresses(_local_addresses),
        // local_addresses_cluster(_local_addresses_cluster),
        local_addresses_driver(_local_addresses_driver),
        local_addresses_http(_local_addresses_http),
        // canonical_addresses(_canonical_addresses),
        // `port` is the intra-cluster port
        // port(_port),
        // client_port(_client_port),
        http_admin_is_disabled(_http_admin_is_disabled),
        http_port(_http_port),
        reql_port(_reql_port),
        port_offset(_port_offset)
    {
            // sanitize_port(port, "port", port_offset);
            // sanitize_port(client_port, "client_port", port_offset);
            sanitize_port(http_port, "http_port", port_offset);
            sanitize_port(reql_port, "reql_port", port_offset);
    }

    static std::string get_addresses_string(std::set<ip_address_t> actual_addresses);

    static bool is_bind_all(const std::set<ip_address_t> &addresses);

    std::set<ip_address_t> local_addresses;
    // We don't have this in ReFound... yet.
    // std::set<ip_address_t> local_addresses_cluster;
    std::set<ip_address_t> local_addresses_driver;
    std::set<ip_address_t> local_addresses_http;

    // We're keeping around canonical addresses as a concept -- at some point they might
    // be registered in the FoundationDB cluster if nodes need to talk to one another.
    // peer_address_t canonical_addresses;
    // int port;
    // int client_port;
    bool http_admin_is_disabled;
    int http_port;
    int reql_port;
    int port_offset;
};

typedef std::shared_ptr<tls_ctx_t> shared_ssl_ctx_t;

class tls_configs_t {
public:
    shared_ssl_ctx_t web;
    shared_ssl_ctx_t driver;
    shared_ssl_ctx_t cluster;
};

peer_address_set_t look_up_peers_addresses(const std::vector<host_and_port_t> &names);

class serve_info_t {
public:
    serve_info_t(const fdb_node_id &_node_id,
                 std::string &&_reql_http_proxy,
                 std::string &&_web_assets,
                 service_address_ports_t _ports,
                 optional<std::string> _config_file,
                 std::vector<std::string> &&_argv,
                 const int _node_reconnect_timeout_secs,
                 tls_configs_t _tls_configs) :
        node_id(_node_id),
        reql_http_proxy(std::move(_reql_http_proxy)),
        web_assets(std::move(_web_assets)),
        ports(_ports),
        config_file(_config_file),
        argv(std::move(_argv)),
        node_reconnect_timeout_secs(_node_reconnect_timeout_secs)
    {
        tls_configs = _tls_configs;
    }

    fdb_node_id node_id;
    std::string reql_http_proxy;
    std::string web_assets;
    service_address_ports_t ports;
    optional<std::string> config_file;
    /* The original arguments, so we can display them in `server_status`. All the
    argument parsing has already been completed at this point. */
    std::vector<std::string> argv;
    int node_reconnect_timeout_secs;
    tls_configs_t tls_configs;
};

/* This has been factored out from `command_line.hpp` because it takes a very
long time to compile. */

bool serve(FDBDatabase *db,
           const serve_info_t &serve_info,
           os_signal_cond_t *stop_cond);

#endif /* CLUSTERING_MAIN_SERVE_HPP_ */
