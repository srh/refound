// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_QUERY_ROUTING_PRIMARY_QUERY_SERVER_HPP_
#define CLUSTERING_QUERY_ROUTING_PRIMARY_QUERY_SERVER_HPP_

#include <map>
#include <set>
#include <string>
#include <utility>

#include "clustering/generic/multi_client_server.hpp"
#include "clustering/query_routing/metadata.hpp"

struct admin_err_t;

/* Each shard has a `primary_query_server_t` on its primary replica server. The
`primary_query_server_t` is responsible for receiving queries from the servers that the
clients connect to and forwarding those queries to the `query_callback_t`. Specifically,
the class `primary_query_client_t`, which is instantiated by `table_query_client_t`,
sends the queries to the `primary_query_server_t`.

`primary_query_server_t` internally contains a `multi_client_server_t`, which is
responsible for managing clients from the different `primary_query_client_t`s.
We use it in combination with `primary_query_server_t::client_t` to ensure the
ordering of requests that originate from a given client. */

// NNN: Remove this file.

#endif /* CLUSTERING_QUERY_ROUTING_PRIMARY_QUERY_SERVER_HPP_ */
