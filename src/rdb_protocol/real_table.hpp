// Copyright 2010-2014 RethinkDB, all rights reserved
#ifndef RDB_PROTOCOL_REAL_TABLE_HPP_
#define RDB_PROTOCOL_REAL_TABLE_HPP_

#include <map>
#include <set>
#include <string>
#include <vector>

#include "rdb_protocol/configured_limits.hpp"
#include "rdb_protocol/context.hpp"
#include "rdb_protocol/protocol.hpp"

namespace ql {
class datum_range_t;
namespace changefeed {
class client_t;
}
}
class table_meta_client_t;

/* `real_table_t` is a concrete subclass of `base_table_t` that routes its queries across
the network via the clustering logic to a B-tree. The administration logic is responsible
for constructing and returning them from `reql_cluster_interface_t::table_find()`. */

namespace ql {
class reader_t;
}

class real_table_t final : public base_table_t {
public:
    /* This doesn't automatically wait for readiness. */
    real_table_t(
            namespace_id_t _uuid,
            namespace_interface_access_t _namespace_access,
            const std::string &_pkey,
#if RDB_CF
            ql::changefeed::client_t *_changefeed_client,
#endif
            table_meta_client_t *table_meta_client) :
        uuid(_uuid),
        namespace_access(_namespace_access),
        pkey(_pkey),
#if RDB_CF
        changefeed_client(_changefeed_client),
#endif
        m_table_meta_client(table_meta_client) { }

    namespace_id_t get_id() const;
    const std::string &get_pkey() const;

    ql::datum_t read_row(ql::env_t *env, ql::datum_t pval, read_mode_t read_mode);
    counted_t<ql::datum_stream_t> read_all(
        ql::env_t *env,
        const std::string &sindex,
        ql::backtrace_id_t bt,
        const std::string &table_name, // The table's own name, for display purposes.
        const ql::datumspec_t &datumspec,
        sorting_t sorting,
        read_mode_t read_mode);
#if RDB_CF
    counted_t<ql::datum_stream_t> read_changes(
        ql::env_t *env,
        const ql::changefeed::streamspec_t &ss,
        ql::backtrace_id_t bt);
#endif
    counted_t<ql::datum_stream_t> read_intersecting(
        ql::env_t *env,
        const std::string &sindex,
        ql::backtrace_id_t bt,
        const std::string &table_name,
        read_mode_t read_mode,
        const ql::datum_t &query_geometry);
    ql::datum_t read_nearest(
        ql::env_t *env,
        const std::string &sindex,
        const std::string &table_name,
        read_mode_t read_mode,
        lon_lat_point_t center,
        double max_dist,
        uint64_t max_results,
        const ellipsoid_spec_t &geo_system,
        dist_unit_t dist_unit,
        const ql::configured_limits_t &limits);

    ql::datum_t write_batched_replace(
        ql::env_t *env,
        const std::vector<ql::datum_t> &keys,
        const ql::deterministic_func &func,
        return_changes_t _return_changes,
        durability_requirement_t durability,
        ignore_write_hook_t ignore_write_hook);
    ql::datum_t write_batched_insert(
        ql::env_t *env,
        std::vector<ql::datum_t> &&inserts,
        std::vector<bool> &&pkey_is_autogenerated,
        conflict_behavior_t conflict_behavior,
        optional<ql::deterministic_func> conflict_func,
        return_changes_t return_changes,
        durability_requirement_t durability,
        ignore_write_hook_t ignore_write_hook);

    scoped_ptr_t<ql::reader_t> read_all_with_sindexes(
        ql::env_t *env,
        const std::string &sindex,
        ql::backtrace_id_t bt,
        const std::string &table_name,
        const ql::datumspec_t &datumspec,
        sorting_t sorting,
        read_mode_t read_mode) final;

    /* These are not part of the `base_table_t` interface. They wrap the `read()`,
    and `write()` methods of the underlying `namespace_interface_t` to add profiling
    information. Specifically, they:
      * Set the explain field in the read_t/write_t object so that the shards know
        whether or not to do profiling
      * Construct a splitter_t
      * Call the corresponding method on the `namespace_if`
      * splitter_t::give_splits with the event logs from the shards
    These are public because some of the stuff in `datum_stream.hpp` needs to be
    able to access them. */
    void read_with_profile(ql::env_t *env, const read_t &, read_response_t *response);
    void write_with_profile(ql::env_t *env, write_t *, write_response_t *response);

private:
    optional<ql::deterministic_func> get_write_hook(
        ql::env_t *env,
        ignore_write_hook_t ignore_write_hook);

    namespace_id_t uuid;
    namespace_interface_access_t namespace_access;
    std::string pkey;
#if RDB_CF
    ql::changefeed::client_t *changefeed_client;
#endif
    table_meta_client_t *m_table_meta_client;
};

#endif /* RDB_PROTOCOL_REAL_TABLE_HPP_ */

