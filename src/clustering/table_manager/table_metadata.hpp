// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef CLUSTERING_TABLE_MANAGER_TABLE_METADATA_HPP_
#define CLUSTERING_TABLE_MANAGER_TABLE_METADATA_HPP_

#include "clustering/generic/minidir.hpp"
#include "clustering/generic/raft_core.hpp"
#include "clustering/table_contract/contract_metadata.hpp"
#include "clustering/table_contract/executor/exec.hpp"
#include "containers/optional.hpp"
#include "rpc/mailbox/typed.hpp"

class store_ptr_t;

namespace metadata {
class read_txn_t;
class write_txn_t;
}

namespace rockstore { class store; }

class multi_table_manager_timestamp_epoch_t {
    using epoch_t = multi_table_manager_timestamp_epoch_t;
public:
    static epoch_t min();
    static epoch_t deletion();
    static epoch_t migrate(time_t ts);
    static epoch_t make(const epoch_t &prev);

    ql::datum_t to_datum() const;

    bool is_unset() const;
    bool is_deletion() const;
    bool operator==(const epoch_t &other) const;
    bool operator!=(const epoch_t &other) const;
    bool supersedes(const epoch_t &other) const;
private:
    // Workaround for issue #4668 - invalid timestamps that were migrated
    // These timestamps should never supercede any other timestamps
    static const microtime_t special_timestamp;

    /* Every table's lifetime is divided into "epochs". Each epoch corresponds to
    one Raft instance. Normally tables only have one epoch; a new epoch is created
    only when the user manually overrides the Raft state, which requires creating a
    new Raft instance.

    `timestamp` is the wall-clock time when the epoch began. `id` is a unique ID
    created for the epoch. An epoch with a later `timestamp` supersedes an epoch
    with an earlier `timestamp`. `id` breaks ties. Ties are possible because the
    user may manually override the Raft state on both sides of a netsplit, for
    example. */
    microtime_t timestamp;
    uuid_u id;

    // Keep the class keyword here to satisfy VC++
    RDB_DECLARE_ME_SERIALIZABLE(class multi_table_manager_timestamp_epoch_t);
};

/* Every message to the `action_mailbox` has an `multi_table_manager_timestamp_t`
attached. This is used to filter out outdated instructions. */
class multi_table_manager_timestamp_t {
public:
    using epoch_t = multi_table_manager_timestamp_epoch_t;

    static multi_table_manager_timestamp_t min() {
        multi_table_manager_timestamp_t ts;
        ts.epoch = epoch_t::min();
        ts.log_index = 0;
        return ts;
    }

    static multi_table_manager_timestamp_t deletion() {
        multi_table_manager_timestamp_t ts;
        ts.epoch = epoch_t::deletion();
        ts.log_index = std::numeric_limits<raft_log_index_t>::max();
        return ts;
    }

    bool is_deletion() const {
        return epoch.is_deletion();
    }

    bool operator==(const multi_table_manager_timestamp_t &other) const {
        return epoch == other.epoch && log_index == other.log_index;
    }
    bool operator!=(const multi_table_manager_timestamp_t &other) const {
        return !(*this == other);
    }

    bool supersedes(const multi_table_manager_timestamp_t &other) const {
        if (epoch.supersedes(other.epoch)) {
            return true;
        } else if (other.epoch.supersedes(epoch)) {
            return false;
        }
        return log_index > other.log_index;
    }

    // TODO: make the data members private and strictly control how they may be changed
    epoch_t epoch;

    /* Within each epoch, Raft log indices provide a monotonically increasing clock. */
    raft_log_index_t log_index;
};
RDB_DECLARE_SERIALIZABLE(multi_table_manager_timestamp_t);

/* In VERIFIED mode, the all replicas ready check makes sure that the leader
still has a quorum and can perform Raft transactions. This is relatively expensive
and causes disk writes and network overhead.
OUTDATED_OK skips that step, but might temporarily return an incorrect result. */
enum class all_replicas_ready_mode_t { INCLUDE_RAFT_TEST = 0, EXCLUDE_RAFT_TEST };
ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(all_replicas_ready_mode_t,
                                      int8_t,
                                      all_replicas_ready_mode_t::INCLUDE_RAFT_TEST,
                                      all_replicas_ready_mode_t::EXCLUDE_RAFT_TEST);

class table_status_request_t {
public:
    table_status_request_t() :
        want_config(false), want_sindexes(false), want_raft_state(false),
        want_contract_acks(false), want_shard_status(false),
        want_all_replicas_ready(false),
        all_replicas_ready_mode(all_replicas_ready_mode_t::INCLUDE_RAFT_TEST) { }

    bool want_config;
    bool want_sindexes;
    bool want_raft_state;
    bool want_contract_acks;
    bool want_shard_status;
    bool want_all_replicas_ready;
    all_replicas_ready_mode_t all_replicas_ready_mode;
};
RDB_DECLARE_SERIALIZABLE(table_status_request_t);

class table_status_response_t {
public:
    /* The booleans in `table_status_request_t` control whether each field of
    `table_status_response_t` will be included or not. This is to avoid making an
    expensive computation if the result will not be used. If a field is not requested,
    its value is undefined (but typically empty or default-constructed). */

    /* We must default-initialize boolean fields or they might cause out of range
    errors during serialization and/or deserializtion. */
    table_status_response_t() : all_replicas_ready(false) { }

    /* `config` is controlled by `want_config`. */
    optional<table_config_and_shards_t> config;

    /* `sindexes` is controlled by `want_sindexes`. */
    std::map<std::string, std::pair<sindex_config_t, sindex_status_t> > sindexes;

    /* `raft_state` and `raft_state_timestamp` are controlled by `want_raft_state` */
    optional<table_raft_state_t> raft_state;
    optional<multi_table_manager_timestamp_t> raft_state_timestamp;

    /* `contract_acks` is controlled by `want_contract_acks` */
    std::map<contract_id_t, contract_ack_t> contract_acks;

    /* `shard_status` is controlled by `want_shard_status`. */
    range_map_t<key_range_t::right_bound_t, table_shard_status_t> shard_status;

    /* `all_replicas_ready` is controlled by `want_all_replicas_ready`. It will be set to
    `true` if the responding server is leader and can confirm that all backfills are
    completed, the status matches the config, etc. Otherwise it will be set to `false`.
    */
    bool all_replicas_ready;
};
RDB_DECLARE_SERIALIZABLE(table_status_response_t);


#endif /* CLUSTERING_TABLE_MANAGER_TABLE_METADATA_HPP_ */

