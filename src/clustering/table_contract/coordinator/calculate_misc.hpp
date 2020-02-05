// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_TABLE_CONTRACT_COORDINATOR_CALCULATE_MISC_HPP_
#define CLUSTERING_TABLE_CONTRACT_COORDINATOR_CALCULATE_MISC_HPP_

#include "clustering/table_contract/contract_metadata.hpp"

void calculate_member_ids_and_raft_config(
        const raft_member_t<table_raft_state_t>::state_and_config_t &sc,
        std::set<server_id_t> *remove_member_ids_out,
        std::map<server_id_t, raft_member_id_t> *add_member_ids_out,
        raft_config_t *new_config_out);

#endif /* CLUSTERING_TABLE_CONTRACT_COORDINATOR_CALCULATE_MISC_HPP_ */

