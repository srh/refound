// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CLUSTERING_GENERIC_RAFT_NETWORK_HPP_
#define CLUSTERING_GENERIC_RAFT_NETWORK_HPP_

#include "clustering/generic/raft_core.hpp"
#include "concurrency/watchable_transform.hpp"
#include "rpc/mailbox/typed.hpp"

/* This file is for running the Raft protocol using RethinkDB's clustering primitives.
The core logic for the Raft protocol is in `raft_core.hpp`, not here. This just adds a
networking layer over `raft_core.hpp`. */

template<class state_t>
class raft_business_card_t {
public:
    typedef mailbox_t<raft_rpc_request_t<state_t>, mailbox_addr_t<raft_rpc_reply_t>> rpc_mailbox_t;

    typename rpc_mailbox_t::address_t rpc;

    optional<raft_term_t> virtual_heartbeats;

    RDB_MAKE_ME_SERIALIZABLE_2(raft_business_card_t, rpc, virtual_heartbeats);
    RDB_MAKE_ME_EQUALITY_COMPARABLE_2(raft_business_card_t, rpc, virtual_heartbeats);
};

#endif   /* CLUSTERING_GENERIC_RAFT_NETWORK_HPP_ */

