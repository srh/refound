// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "clustering/table_contract/coordinator/check_ready.hpp"

bool check_all_replicas_ready(
        const table_raft_state_t &table_state,
        watchable_map_t<std::pair<server_id_t, contract_id_t>, contract_ack_t> *acks) {
    for (const auto &pair : table_state.contracts) {

        /* Find the config shard corresponding to this contract */
        const table_config_t::shard_t *shard = &table_state.config.config.the_shard;

        /* Check if the config shard matches the contract */
        const contract_t &contract = pair.second;
        if (contract.the_replica != shard->primary_replica ||
                contract.the_voter != shard->primary_replica ||
                static_cast<bool>(contract.temp_voters) ||
                !static_cast<bool>(contract.primary) ||
                contract.primary->server != shard->primary_replica ||
                static_cast<bool>(contract.primary->hand_over) ||
                contract.after_emergency_repair) {
            return false;
        }

        /* Check if all the replicas have acked the contract */
        {
            server_id_t server = contract.the_replica;
            bool ok;
            acks->read_key(std::make_pair(server, pair.first),
            [&](const contract_ack_t *ack) {
                ok = (ack != nullptr) && (
                    ack->state == contract_ack_t::state_t::primary_ready ||
                        ack->state == contract_ack_t::state_t::secondary_streaming);
            });
            if (!ok) {
                return false;
            }
        }
    }
    return true;
}
