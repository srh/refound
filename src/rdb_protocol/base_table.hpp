#ifndef RETHINKDB_RDB_PROTOCOL_BASE_TABLE_HPP_
#define RETHINKDB_RDB_PROTOCOL_BASE_TABLE_HPP_

#include <string>

#include "btree/keys.hpp"
#include "containers/counted.hpp"
#include "containers/optional.hpp"
#include "containers/uuid.hpp"
#include "fdb/id_types.hpp"
#include "protocol_api.hpp"
#include "rdb_protocol/error.hpp"

enum class return_changes_t;

namespace ql {
class configured_limits_t;
class datumspec_t;
class datum_t;
class datum_stream_t;
class deterministic_func;
class env_t;
class reader_t;

#if RDB_CF
namespace changefeed {
class streamspec_t;
}
#endif
}  // namespace ql

enum class dist_unit_t;
class ellipsoid_spec_t;
struct lon_lat_point_t;

// TODO: This type's usage in counted_t could be constified.  Do so after fully FDBized.
// TODO: slow_atomic_countable_t.  KMN.
class base_table_t : public slow_atomic_countable_t<base_table_t> {
public:
    explicit base_table_t(config_version_checker _cv) : cv(_cv) {}
    virtual namespace_id_t get_id() const = 0;
    virtual const std::string &get_pkey() const = 0;

    virtual scoped_ptr_t<ql::reader_t> read_all_with_sindexes(
        ql::env_t *,
        const std::string &,
        ql::backtrace_id_t,
        const std::string &,
        const ql::datumspec_t &,
        sorting_t,
        read_mode_t) {
        r_sanity_fail();
    }

    virtual ql::datum_t read_row(ql::env_t *env,
        ql::datum_t pval, read_mode_t read_mode) = 0;
    virtual scoped<ql::datum_stream_t> read_all(
        ql::env_t *env,
        const std::string &sindex,
        ql::backtrace_id_t bt,
        const std::string &table_name,   /* the table's own name, for display purposes */
        const ql::datumspec_t &datumspec,
        sorting_t sorting,
        read_mode_t read_mode) = 0;
    virtual scoped<ql::datum_stream_t> read_intersecting(
        ql::env_t *env,
        const std::string &sindex,
        ql::backtrace_id_t bt,
        const std::string &table_name,
        read_mode_t read_mode,
        const ql::datum_t &query_geometry) = 0;
    virtual ql::datum_t read_nearest(
        ql::env_t *env,
        const std::string &sindex,
        const std::string &table_name,
        read_mode_t read_mode,
        lon_lat_point_t center,
        double max_dist,
        uint64_t max_results,
        const ellipsoid_spec_t &geo_system,
        dist_unit_t dist_unit,
        const ql::configured_limits_t &limits) = 0;

    virtual ql::datum_t write_batched_replace(
        ql::env_t *env,
        const std::vector<ql::datum_t> &keys,
        const ql::deterministic_func &func,
        return_changes_t _return_changes,
        durability_requirement_t durability,
        ignore_write_hook_t ignore_write_hook) = 0;
    virtual ql::datum_t write_batched_insert(
        ql::env_t *env,
        std::vector<ql::datum_t> &&inserts,
        std::vector<bool> &&pkey_was_autogenerated,
        conflict_behavior_t conflict_behavior,
        optional<ql::deterministic_func> conflict_func,
        return_changes_t return_changes,
        durability_requirement_t durability,
        ignore_write_hook_t ignore_write_hook) = 0;

    /* This must be public */
    virtual ~base_table_t() { }

    // QQQ: Force everybody accessing the uuid to use this value.
    // TODO: This is a bit gross -- we just call assert_nonempty() on it -- we statically know when we have a real_table_t and an artificial_table_t by checking artificial_reql_cluster_interface_t::database_name, except for sync_term_t.
    config_version_checker cv;

    DISABLE_COPYING(base_table_t);
};


#endif  // RETHINKDB_RDB_PROTOCOL_BASE_TABLE_HPP_
