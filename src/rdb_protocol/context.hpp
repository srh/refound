// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_CONTEXT_HPP_
#define RDB_PROTOCOL_CONTEXT_HPP_

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "rdb_protocol/secondary_operations.hpp"
#include "concurrency/one_per_thread.hpp"
#include "containers/counted.hpp"
#include "containers/name_string.hpp"
#include "containers/scoped.hpp"
#include "containers/uuid.hpp"
#include "fdb/fdb.hpp"
#include "fdb/id_types.hpp"
#include "perfmon/perfmon.hpp"
#include "protocol_api.hpp"
#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/geo/distances.hpp"
#include "rdb_protocol/geo/lon_lat_types.hpp"
#include "rdb_protocol/shards.hpp"
#include "rdb_protocol/wire_func.hpp"

namespace auth {
class user_context_t;
}  // namespace auth

class artificial_reql_cluster_interface_t;
class reqlfdb_config_cache;

namespace ql {
class env_t;
class query_cache_t;

#if RDB_CF
namespace changefeed {
class streamspec_t;
class client_t;
}
#endif
}  // namespace ql

struct admin_err_t;

class extproc_pool_t;

class sindex_config_t {
public:
    sindex_config_t(const ql::deterministic_func &_func, reql_version_t _func_version,
            sindex_multi_bool_t _multi, sindex_geo_bool_t _geo) :
        func(_func), func_version(_func_version), multi(_multi), geo(_geo) { }

    bool operator==(const sindex_config_t &o) const;
    bool operator!=(const sindex_config_t &o) const {
        return !(*this == o);
    }

    ql::deterministic_func func;
    reql_version_t func_version;
    sindex_multi_bool_t multi;
    sindex_geo_bool_t geo;

    sindex_config_t() = default;
    sindex_config_t(sindex_config_t &&) = default;
    sindex_config_t &operator=(sindex_config_t &&) = default;
    sindex_config_t(const sindex_config_t &) = default;
    sindex_config_t &operator=(const sindex_config_t &) = default;
};
RDB_DECLARE_SERIALIZABLE(sindex_config_t);

bool is_acceptable_outdated(const sindex_config_t &sindex_config);

class write_hook_config_t {
public:
    write_hook_config_t() { }
    write_hook_config_t(const ql::deterministic_func &_func, reql_version_t _func_version) :
        func(_func), func_version(_func_version) { }

    bool operator==(const write_hook_config_t &o) const;
    bool operator!=(const write_hook_config_t &o) const {
        return !(*this == o);
    }

    ql::deterministic_func func;
    reql_version_t func_version;
};
RDB_DECLARE_SERIALIZABLE(write_hook_config_t);

class sindex_status_t {
public:
    sindex_status_t() :
        progress_numerator(0),
        progress_denominator(0),
        ready(true),
        outdated(false),
        start_time(-1) { }
    void accum(const sindex_status_t &other);
    double progress_numerator;
    double progress_denominator;
    bool ready;
    bool outdated;
    /* Note that `start_time` is only valid when `ready` is false, and while we
    serialize it it's relative to the local clock. If this becomes a problem in the
    future you can apply the same solution as in
        `void serialize(write_message_t *wm, const batchspec_t &batchspec)`,
    but that's relatively expensive. */
    microtime_t start_time;
};
RDB_DECLARE_SERIALIZABLE(sindex_status_t);

namespace ql {
class reader_t;
}

class rdb_context_t {
public:
    // Used by unit tests.
    rdb_context_t();
    // Also used by unit tests.
    rdb_context_t(FDBDatabase *_fdb,
                  extproc_pool_t *_extproc_pool);

    // The "real" constructor used outside of unit tests.
    rdb_context_t(
        FDBDatabase *_fdb,
        extproc_pool_t *_extproc_pool,
        artificial_reql_cluster_interface_t *_cluster_interface,
        perfmon_collection_t *global_stats,
        const std::string &_reql_http_proxy);

    ~rdb_context_t();

    FDBDatabase *fdb = nullptr;
    one_per_thread_t<reqlfdb_config_cache> config_caches;
    extproc_pool_t *extproc_pool;
    // OOO: Just replace artificial_interface_or_null with the backends field it contains.
    // This is null in unit tests.
    artificial_reql_cluster_interface_t *artificial_interface_or_null = nullptr;

    const std::string reql_http_proxy;

    class stats_t {
    public:
        explicit stats_t(perfmon_collection_t *global_stats);

        perfmon_collection_t qe_stats_collection;
        perfmon_membership_t qe_stats_membership;
        perfmon_counter_t client_connections;
        perfmon_membership_t client_connections_membership;
        perfmon_counter_t clients_active;
        perfmon_membership_t clients_active_membership;
        perfmon_rate_monitor_t queries_per_sec;
        perfmon_membership_t queries_per_sec_membership;
        perfmon_counter_t queries_total;
        perfmon_membership_t queries_total_membership;
    private:
        DISABLE_COPYING(stats_t);
    } stats;

    std::set<ql::query_cache_t *> *get_query_caches_for_this_thread();

private:
    one_per_thread_t<std::set<ql::query_cache_t *> > query_caches;

    DISABLE_COPYING(rdb_context_t);
};

#endif /* RDB_PROTOCOL_CONTEXT_HPP_ */

