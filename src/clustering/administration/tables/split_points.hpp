// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_TABLES_SPLIT_POINTS_HPP_
#define CLUSTERING_ADMINISTRATION_TABLES_SPLIT_POINTS_HPP_

#include <map>
#include <string>
#include <vector>

#include "btree/keys.hpp"
#include "clustering/table_manager/table_meta_client.hpp"
#include "containers/uuid.hpp"

class real_reql_cluster_interface_t;
class signal_t;

/* `fetch_distribution` fetches the distribution information from the database. */
void fetch_distribution(
        const namespace_id_t &table_id,
        real_reql_cluster_interface_t *reql_cluster_interface,
        signal_t *interruptor,
        std::map<store_key_t, int64_t> *counts_out)
        THROWS_ONLY(interrupted_exc_t, failed_table_op_exc_t, no_such_table_exc_t);

#endif /* CLUSTERING_ADMINISTRATION_TABLES_SPLIT_POINTS_HPP_ */

