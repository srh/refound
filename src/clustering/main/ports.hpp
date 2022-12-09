// Copyright 2010-2012 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_MAIN_PORTS_HPP_
#define CLUSTERING_ADMINISTRATION_MAIN_PORTS_HPP_

#include "arch/address.hpp"

namespace port_defaults {

const port_t peer_port{29015};
const port_t client_port{0};
const port_t http_port{8080};

/* We currently spin up clusters with port offsets differing by 1, so
   these ports should be reasonably far apart. */
const port_t reql_port{28015};

const int port_offset = 0;

}  // namespace port_defaults

#endif  // CLUSTERING_ADMINISTRATION_MAIN_PORTS_HPP_
