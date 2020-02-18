// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "rdb_protocol/context.hpp"

#include "clustering/administration/artificial_reql_cluster_interface.hpp"
#include "containers/archive/vector_stream.hpp"
#include "rdb_protocol/query_cache.hpp"
#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/reqlfdb_config_cache.hpp"
#include "time.hpp"

bool sindex_config_t::operator==(const sindex_config_t &o) const {
    if (func_version != o.func_version || multi != o.multi || geo != o.geo) {
        return false;
    }
    /* This is kind of a hack--we compare the functions by serializing them and comparing
    the serialized values. */
    return serialize_for_cluster_to_vector(func) == serialize_for_cluster_to_vector(o.func);
}

RDB_IMPL_SERIALIZABLE_4_SINCE_v2_1(sindex_config_t,
    func, func_version, multi, geo);

bool is_acceptable_outdated(const sindex_config_t &sindex_config) {
    if (sindex_config.func.det_func.is_simple_selector()) {
        switch(sindex_config.func_version) {
            // A version should return false if it breaks compatibility for even simple sindexes.
        case reql_version_t::v2_4_is_latest:
        default:
            return true;
        }
    }
    return false;
}

bool write_hook_config_t::operator==(const write_hook_config_t &o) const {
    if (func_version != o.func_version) {
        return false;
    }
    /* This is kind of a hack--we compare the functions by serializing them and comparing
    the serialized values. */
    return serialize_for_cluster_to_vector(func) == serialize_for_cluster_to_vector(o.func);
}

RDB_IMPL_SERIALIZABLE_2_SINCE_v2_4(write_hook_config_t,
    func, func_version);

void sindex_status_t::accum(const sindex_status_t &other) {
    progress_numerator += other.progress_numerator;
    progress_denominator += other.progress_denominator;
    ready &= other.ready;
    start_time = std::min(start_time, other.start_time);
    rassert(outdated == other.outdated);
}

RDB_IMPL_SERIALIZABLE_5_FOR_CLUSTER(sindex_status_t,
    progress_numerator, progress_denominator, ready, outdated, start_time);

const char *rql_perfmon_name = "query_engine";

rdb_context_t::stats_t::stats_t(perfmon_collection_t *global_stats)
    : qe_stats_membership(global_stats, &qe_stats_collection, rql_perfmon_name),
      client_connections(get_num_threads()),
      client_connections_membership(&qe_stats_collection,
                                    &client_connections, "client_connections"),
      clients_active(get_num_threads()),
      clients_active_membership(&qe_stats_collection,
                                &clients_active, "clients_active"),
      queries_per_sec(secs_to_ticks(1), get_num_threads()),
      queries_per_sec_membership(&qe_stats_collection,
                                 &queries_per_sec, "queries_per_sec"),
      queries_total(get_num_threads()),
      queries_total_membership(&qe_stats_collection,
                               &queries_total, "queries_total") { }

rdb_context_t::rdb_context_t()
    : fdb(nullptr),
      extproc_pool(nullptr),
      artificial_interface_or_null(nullptr),
      reql_http_proxy(),
      stats(&get_global_perfmon_collection()) { }

rdb_context_t::rdb_context_t(
        FDBDatabase *_fdb,
        extproc_pool_t *_extproc_pool)
    : fdb(_fdb),
      extproc_pool(_extproc_pool),
      artificial_interface_or_null(nullptr),
      reql_http_proxy(),
      stats(&get_global_perfmon_collection()) {
}

rdb_context_t::rdb_context_t(
        FDBDatabase *_fdb,
        extproc_pool_t *_extproc_pool,
        artificial_reql_cluster_interface_t *_cluster_interface,
        perfmon_collection_t *global_stats,
        const std::string &_reql_http_proxy)
    : fdb(_fdb),
      extproc_pool(_extproc_pool),
      artificial_interface_or_null(_cluster_interface),
      reql_http_proxy(_reql_http_proxy),
      stats(global_stats) {
}

rdb_context_t::~rdb_context_t() { }

std::set<ql::query_cache_t *> *rdb_context_t::get_query_caches_for_this_thread() {
    return query_caches.get();
}

