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
#include "concurrency/promise.hpp"
#include "concurrency/watchable.hpp"
#include "containers/clone_ptr.hpp"
#include "containers/counted.hpp"
#include "containers/name_string.hpp"
#include "containers/optional.hpp"
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
class permissions_t;

}  // namespace auth

class artificial_reql_cluster_interface_t;
class base_table_t;
class reqlfdb_config_cache;
enum class return_changes_t;

namespace ql {
class configured_limits_t;
class datumspec_t;
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

class auth_semilattice_metadata_t;
class ellipsoid_spec_t;
class extproc_pool_t;
class name_string_t;
class namespace_interface_t;
template <class> class cross_thread_watchable_variable_t;
template <class> class semilattice_read_view_t;

class sindex_config_t {
public:
    sindex_config_t() { }
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
};
RDB_DECLARE_SERIALIZABLE(sindex_config_t);

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
class db_t : public single_threaded_countable_t<db_t> {
public:
    db_t(database_id_t _id, const name_string_t &_name, config_version_checker _cv)
        : id(_id), name(_name), cv(_cv) { }
    const database_id_t id;
    const name_string_t name;
    // The config version behind the db id lookup, if there is one.
    // QQQ: Force everybody accessing database_id_t to use this value.
    config_version_checker cv;
};
} // namespace ql

// TODO: Remove this?
class table_generate_config_params_t {
public:
    static table_generate_config_params_t make_default() {
        table_generate_config_params_t p;
        p.primary_replica_tag = name_string_t::guarantee_valid("default");
        p.num_replicas[p.primary_replica_tag] = 1;
        return p;
    }
    std::map<name_string_t, size_t> num_replicas;
    std::set<name_string_t> nonvoting_replica_tags;
    name_string_t primary_replica_tag;
};

namespace ql {
class reader_t;
}

class reql_cluster_interface_t {
public:
    /* All of these methods return `true` on success and `false` on failure; if they
    fail, they will set `*error_out` to a description of the problem. They can all throw
    `interrupted_exc_t`.

    These methods are safe to call from any thread, and the calls can overlap
    concurrently in arbitrary ways. By the time a method returns, any changes it makes
    must be visible on every thread. */

    /* From the user's point of view, many of these are methods on the table object. The
    reason they're internally defined on `reql_cluster_interface_t` rather than
    `base_table_t` is because their implementations fits better with the implementations
    of the other methods of `reql_cluster_interface_t` than `base_table_t`. */

    // The remaining methods (besides write hook methods, getters, and those called only
    // for artificial_reql_cluster_interface_t behavior) all return single selectinos.

    virtual bool db_config(
            auth::user_context_t const &user_context,
            const counted_t<const ql::db_t> &db,
            ql::backtrace_id_t bt,
            ql::env_t *env,
            scoped_ptr_t<ql::val_t> *selection_out,
            admin_err_t *error_out) = 0;

    virtual bool table_config(
            auth::user_context_t const &user_context,
            counted_t<const ql::db_t> db,
            const name_string_t &name,
            ql::backtrace_id_t bt,
            ql::env_t *env,
            scoped_ptr_t<ql::val_t> *selection_out,
            admin_err_t *error_out) = 0;
    virtual bool table_status(
            counted_t<const ql::db_t> db,
            const name_string_t &name,
            ql::backtrace_id_t bt,
            ql::env_t *env,
            scoped_ptr_t<ql::val_t> *selection_out,
            admin_err_t *error_out) = 0;

#if RDB_CF
    virtual ql::changefeed::client_t *get_changefeed_client() = 0;
#endif

protected:
    virtual ~reql_cluster_interface_t() { }   // silence compiler warnings
};

class mailbox_manager_t;

class rdb_context_t {
public:
    // Used by unit tests.
    rdb_context_t();
    // Also used by unit tests.
    rdb_context_t(FDBDatabase *_fdb,
                  extproc_pool_t *_extproc_pool,
                  reql_cluster_interface_t *_cluster_interface,
                  std::shared_ptr<semilattice_read_view_t<auth_semilattice_metadata_t>>
                      auth_semilattice_view);

    // The "real" constructor used outside of unit tests.
    rdb_context_t(
        FDBDatabase *_fdb,
        extproc_pool_t *_extproc_pool,
        mailbox_manager_t *_mailbox_manager,
        artificial_reql_cluster_interface_t *_cluster_interface,
        std::shared_ptr<semilattice_read_view_t<auth_semilattice_metadata_t>>
            auth_semilattice_view,
        perfmon_collection_t *global_stats,
        const std::string &_reql_http_proxy);

    ~rdb_context_t();

    FDBDatabase *fdb = nullptr;
    one_per_thread_t<reqlfdb_config_cache> config_caches;
    extproc_pool_t *extproc_pool;
    // TODO: Clean this stuff up someday.
    reql_cluster_interface_t *cluster_interface;
    // This is null in unit tests.
    artificial_reql_cluster_interface_t *artificial_interface_or_null = nullptr;

    mailbox_manager_t *manager;

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

    clone_ptr_t<watchable_t<auth_semilattice_metadata_t>> get_auth_watchable() const;

private:
    void init_auth_watchables(
        std::shared_ptr<semilattice_read_view_t<auth_semilattice_metadata_t>>
            auth_semilattice_view);

    std::vector<std::unique_ptr<cross_thread_watchable_variable_t<
        auth_semilattice_metadata_t>>> m_cross_thread_auth_watchables;

    one_per_thread_t<std::set<ql::query_cache_t *> > query_caches;

    DISABLE_COPYING(rdb_context_t);
};

#endif /* RDB_PROTOCOL_CONTEXT_HPP_ */

