// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_PROTOCOL_HPP_
#define RDB_PROTOCOL_PROTOCOL_HPP_

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "errors.hpp"
#include <boost/variant.hpp>

#include "btree/keys.hpp"
#include "btree/key_edges.hpp"
#include "rdb_protocol/secondary_operations.hpp"
#include "clustering/auth/user_context.hpp"
#include "concurrency/auto_drainer.hpp"
#include "concurrency/cond_var.hpp"
#include "containers/optional.hpp"
#include "perfmon/perfmon.hpp"
#include "protocol_api.hpp"
#include "rdb_protocol/changefeed.hpp"
#include "rdb_protocol/configured_limits.hpp"
#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/datumspec.hpp"
#include "rdb_protocol/geo/ellipsoid.hpp"
#include "rdb_protocol/geo/lon_lat_types.hpp"
#include "rdb_protocol/optargs.hpp"
#include "rdb_protocol/return_changes.hpp"
#include "rdb_protocol/shards.hpp"

class store_t;
template <class> class clone_ptr_t;
template <class> class watchable_t;

enum class profile_bool_t {
    PROFILE,
    DONT_PROFILE
};
ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(
        profile_bool_t, int8_t,
        profile_bool_t::PROFILE, profile_bool_t::DONT_PROFILE);

enum class point_write_result_t {
    STORED,
    DUPLICATE
};
ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(
        point_write_result_t, int8_t,
        point_write_result_t::STORED, point_write_result_t::DUPLICATE);

enum class point_delete_result_t {
    DELETED,
    MISSING
};
ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(
        point_delete_result_t, int8_t,
        point_delete_result_t::DELETED, point_delete_result_t::MISSING);

class key_le_t {
public:
    explicit key_le_t(sorting_t _sorting) : sorting(_sorting) { }
    bool is_le(const store_key_t &key1, const store_key_t &key2) const {
        return (!reversed(sorting) && key1 <= key2)
            || (reversed(sorting) && key2 <= key1);
    }
private:
    sorting_t sorting;
};

namespace ql {
class datum_t;
class primary_readgen_t;
class readgen_t;
class sindex_readgen_t;
class intersecting_readgen_t;
} // namespace ql

namespace rdb_protocol {

void resume_construct_sindex(
        const uuid_u &sindex_to_construct,
        const key_range_t &construct_range,
        store_t *store,
        auto_drainer_t::lock_t store_keepalive)
    THROWS_NOTHING;

} // namespace rdb_protocol

struct point_read_response_t {
    ql::datum_t data;
    point_read_response_t() { }
    explicit point_read_response_t(ql::datum_t _data)
        : data(_data) { }
};

struct rget_read_response_t {
    ql::result_t result;

    rget_read_response_t() { }
    explicit rget_read_response_t(const ql::exc_t &ex)
        : result(ex) { }
};

struct nearest_geo_read_response_t {
    typedef std::pair<double, ql::datum_t> dist_pair_t;
    typedef std::vector<dist_pair_t> result_t;
    boost::variant<result_t, ql::exc_t> results_or_error;

    nearest_geo_read_response_t() { }
    explicit nearest_geo_read_response_t(result_t &&_results) {
        // Implement "move" on _results through std::vector<...>::swap to avoid
        // problems with boost::variant not supporting move assignment.
        results_or_error = result_t();
        boost::get<result_t>(&results_or_error)->swap(_results);
    }
    explicit nearest_geo_read_response_t(const ql::exc_t &_error)
        : results_or_error(_error) { }
};

struct count_read_response_t {
    uint64_t value;
};

struct dummy_read_response_t {
    // dummy read always succeeds
};

struct serializable_env_t {
    // The global optargs values passed to .run(...) in the Python, Ruby, and JS
    // drivers.
    ql::global_optargs_t global_optargs;

    // The user that's evaluating this query
    auth::user_context_t user_context;

    // The time that the most recent request started processing, set in fill_response
    // in query_cache.cc
    // Used to evaluate r.now in a deterministic way.
    ql::datum_t deterministic_time;
};


struct read_response_t {
    typedef boost::variant<point_read_response_t,
                           rget_read_response_t,
                           nearest_geo_read_response_t,
                           count_read_response_t,
                           dummy_read_response_t> variant_t;
    variant_t response;
    profile::event_log_t event_log;

    read_response_t() { }
    explicit read_response_t(const variant_t &r)
        : response(r) { }
};

class point_read_t {
public:
    point_read_t() { }
    explicit point_read_t(const store_key_t& _key) : key(_key) { }

    store_key_t key;
};

// Reads the table's solitary `count` field.
class count_read_t { };

// `dummy_read_t` can be used to poll for table readiness - it will go through all
// the clustering layers, but is a no-op in the protocol layer.
class dummy_read_t {
public:
    dummy_read_t() { }
};

struct sindex_rangespec_t {
    sindex_rangespec_t() { }
    sindex_rangespec_t(const std::string &_id,
                       // This is the region in the sindex keyspace.  It's
                       // sometimes smaller than the datum range below when
                       // dealing with truncated keys.
                       optional<region_t> _region,
                       ql::datumspec_t _datumspec,
                       require_sindexes_t _require_sindex_val = require_sindexes_t::NO)
        : id(_id),
          region(std::move(_region)),
          datumspec(std::move(_datumspec)),
          require_sindex_val(_require_sindex_val){ }
    std::string id; // What sindex we're using.
    // What keyspace we're currently operating on.  If empty, assume the
    // original range and create the readgen on the shards.
    optional<region_t> region;
    // For dealing with truncation and `get_all`.
    ql::datumspec_t datumspec;
    // For forcing sindex values to be returned with sorting::UNORDERED, used in eq_join.
    require_sindexes_t require_sindex_val;
};


class rget_read_t {
public:
    rget_read_t() : batchspec(ql::batchspec_t::empty()) { }

    rget_read_t(
                region_t _region,
                optional<std::map<store_key_t, uint64_t> > _primary_keys,
                serializable_env_t s_env,
                std::string _table_name,
                ql::batchspec_t _batchspec,
                std::vector<ql::transform_variant_t> _transforms,
                optional<ql::terminal_variant_t> &&_terminal,
                optional<sindex_rangespec_t> &&_sindex,
                sorting_t _sorting)
    : region(std::move(_region)),
      primary_keys(std::move(_primary_keys)),
      serializable_env(std::move(s_env)),
      table_name(std::move(_table_name)),
      batchspec(std::move(_batchspec)),
      transforms(std::move(_transforms)),
      terminal(std::move(_terminal)),
      sindex(std::move(_sindex)),
      sorting(std::move(_sorting)) { }

    region_t region; // We need this even for sindex reads due to sharding.

    // The `uint64_t`s here are counts.  This map is used to make `get_all` more
    // efficient, and it's legal to pass duplicate keys to `get_all`.
    optional<std::map<store_key_t, uint64_t> > primary_keys;

    serializable_env_t serializable_env;
    std::string table_name;
    ql::batchspec_t batchspec; // used to size batches

    // We use these two for lazy maps, reductions, etc.
    std::vector<ql::transform_variant_t> transforms;
    optional<ql::terminal_variant_t> terminal;

    // This is non-empty if we're doing an sindex read.
    // TODO: `read_t` should maybe be multiple types.  Determining the type
    // of read by branching on whether an optional is full sucks.
    optional<sindex_rangespec_t> sindex;

    sorting_t sorting; // Optional sorting info (UNORDERED means no sorting).
};

class intersecting_geo_read_t {
public:
    intersecting_geo_read_t() : batchspec(ql::batchspec_t::empty()) { }

    intersecting_geo_read_t(
        serializable_env_t s_env,
        std::string _table_name,
        ql::batchspec_t _batchspec,
        std::vector<ql::transform_variant_t> _transforms,
        optional<ql::terminal_variant_t> &&_terminal,
        sindex_rangespec_t &&_sindex,
        ql::datum_t _query_geometry)
        : serializable_env(s_env),
          table_name(std::move(_table_name)),
          batchspec(std::move(_batchspec)),
          transforms(std::move(_transforms)),
          terminal(std::move(_terminal)),
          sindex(std::move(_sindex)),
          query_geometry(std::move(_query_geometry)) { }

    serializable_env_t serializable_env;
    std::string table_name;
    ql::batchspec_t batchspec; // used to size batches

    // We use these two for lazy maps, reductions, etc.
    std::vector<ql::transform_variant_t> transforms;
    optional<ql::terminal_variant_t> terminal;

    sindex_rangespec_t sindex;

    ql::datum_t query_geometry; // Tested for intersection
};

class nearest_geo_read_t {
public:
    nearest_geo_read_t() { }

    nearest_geo_read_t(
            lon_lat_point_t _center,
            double _max_dist,
            uint64_t _max_results,
            const ellipsoid_spec_t &_geo_system,
            const std::string &_table_name,
            const std::string &_sindex_id,
            serializable_env_t s_env)
        : serializable_env(std::move(s_env)),
          center(_center),
          max_dist(_max_dist),
          max_results(_max_results),
          geo_system(_geo_system),
          table_name(_table_name),
          sindex_id(_sindex_id) { }

    serializable_env_t serializable_env;

    lon_lat_point_t center;
    double max_dist;
    uint64_t max_results;
    ellipsoid_spec_t geo_system;

    std::string table_name;

    std::string sindex_id;
};

struct read_t {
    typedef boost::variant<point_read_t,
                           rget_read_t,
                           intersecting_geo_read_t,
                           nearest_geo_read_t,
                           count_read_t,
                           dummy_read_t> variant_t;

    variant_t read;
    profile_bool_t profile;
    read_mode_t read_mode;

    read_t() : profile(profile_bool_t::DONT_PROFILE), read_mode(read_mode_t::SINGLE) { }
    template<class T>
    read_t(T &&_read, profile_bool_t _profile, read_mode_t _read_mode)
        : read(std::forward<T>(_read)), profile(_profile), read_mode(_read_mode) { }
};

struct point_write_response_t {
    point_write_result_t result;

    point_write_response_t() { }
    explicit point_write_response_t(point_write_result_t _result)
        : result(_result)
    { }
};

struct point_delete_response_t {
    point_delete_result_t result;
    point_delete_response_t() {}
    explicit point_delete_response_t(point_delete_result_t _result)
        : result(_result)
    { }
};

struct sync_response_t {
    // sync always succeeds
};

struct dummy_write_response_t {
    // dummy write always succeeds
};


typedef ql::datum_t batched_replace_response_t;

struct write_response_t {
    boost::variant<batched_replace_response_t,
                   // batched_replace_response_t is also for batched_insert
                   point_write_response_t,
                   point_delete_response_t,
                   sync_response_t,
                   dummy_write_response_t> response;

    profile::event_log_t event_log;

    write_response_t() { }
    template<class T>
    explicit write_response_t(const T &t) : response(t) { }
};

struct batched_replace_t {
    batched_replace_t() { }
    batched_replace_t(
            std::vector<store_key_t> &&_keys,
            const std::string &_pkey,
            const ql::deterministic_func &func,
            ignore_write_hook_t _ignore_write_hook,
            serializable_env_t s_env,
            return_changes_t _return_changes)
        : keys(std::move(_keys)),
          pkey(_pkey),
          f(func),
          ignore_write_hook(_ignore_write_hook),
          serializable_env(std::move(s_env)),
          return_changes(_return_changes) {
        r_sanity_check(keys.size() != 0);
    }
    std::vector<store_key_t> keys;
    std::string pkey;
    ql::deterministic_func f;
    ignore_write_hook_t ignore_write_hook;
    serializable_env_t serializable_env;
    return_changes_t return_changes;
};

struct batched_insert_t {
    batched_insert_t() { }
    batched_insert_t(
        std::vector<ql::datum_t> &&_inserts,
        const std::string &_pkey,
        ignore_write_hook_t _ignore_write_hook,
        conflict_behavior_t _conflict_behavior,
        const optional<ql::deterministic_func> &_conflict_func,
        const ql::configured_limits_t &_limits,
        serializable_env_t s_env,
        return_changes_t _return_changes);

    std::vector<ql::datum_t> inserts;
    std::string pkey;
    // TODO: Right now we just use this to decide whether to do config auth.  At some point we'll remove the write_hook field because we know that a priori in the table config.
    ignore_write_hook_t ignore_write_hook;
    conflict_behavior_t conflict_behavior;
    optional<ql::deterministic_func> conflict_func;
    ql::configured_limits_t limits;
    serializable_env_t serializable_env;
    return_changes_t return_changes;
};

class point_write_t {
public:
    point_write_t() { }
    point_write_t(const store_key_t& _key,
                  ql::datum_t _data)
        : key(_key), data(_data), overwrite(true) { }

    store_key_t key;
    ql::datum_t data;
    bool overwrite;
};

class point_delete_t {
public:
    point_delete_t() { }
    explicit point_delete_t(const store_key_t& _key)
        : key(_key) { }

    store_key_t key;
};

class sync_t {
public:
    sync_t()
        : region(region_t::universe())
    { }

    region_t region;
};

// `dummy_write_t` can be used to poll for table readiness - it will go through all
// the clustering layers, but is a no-op in the protocol layer.
class dummy_write_t {
public:
    dummy_write_t() : region(region_t::universe()) { }
    region_t region;
};

struct write_t {
    typedef boost::variant<batched_replace_t,
                           batched_insert_t,
                           point_write_t,
                           point_delete_t,
                           sync_t,
                           dummy_write_t> variant_t;
    variant_t write;

    durability_requirement_t durability_requirement;
    profile_bool_t profile;
    ql::configured_limits_t limits;

    // This is currently used to improve the cache's write transaction throttling.
    int expected_document_changes() const;

    durability_requirement_t durability() const { return durability_requirement; }

    /* The clustering layer calls this. */
    static write_t make_sync(const region_t &region, profile_bool_t profile) {
        sync_t sync;
        sync.region = region;
        return write_t(
            sync,
            durability_requirement_t::HARD,
            profile,
            ql::configured_limits_t());
    }

    write_t() :
        durability_requirement(durability_requirement_t::DEFAULT),
        profile(profile_bool_t::DONT_PROFILE),
        limits() {}
    /*  Note that for durability != durability_requirement_t::HARD, sync might
     *  not have the desired effect (of writing unsaved data to disk).
     *  However there are cases where we use sync internally (such as when
     *  splitting up batched replaces/inserts) and want it to only have an
     *  effect if durability_requirement_t::DEFAULT resolves to hard
     *  durability. */
    template<class T>
    write_t(T &&t,
            durability_requirement_t _durability,
            profile_bool_t _profile,
            const ql::configured_limits_t &_limits)
        : write(std::forward<T>(t)),
          durability_requirement(_durability), profile(_profile),
          limits(_limits) { }
    template<class T>
    write_t(T &&t, profile_bool_t _profile,
            const ql::configured_limits_t &_limits)
        : write(std::forward<T>(t)),
          durability_requirement(durability_requirement_t::DEFAULT),
          profile(_profile),
          limits(_limits) { }
};

class store_t;

namespace rdb_protocol {
const size_t MAX_PRIMARY_KEY_SIZE = 128;

// Construct a region containing only the specified key
region_t monokey_region(const store_key_t &k);

// Constructs a region which will query an sindex for matches to a specific key
// TODO consider relocating this
key_range_t sindex_key_range(const store_key_t &start,
                             const store_key_t &end,
                             key_range_t::bound_t end_type);
}  // namespace rdb_protocol

#endif  // RDB_PROTOCOL_PROTOCOL_HPP_
