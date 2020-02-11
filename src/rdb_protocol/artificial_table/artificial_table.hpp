// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_ARTIFICIAL_TABLE_ARTIFICIAL_TABLE_HPP_
#define RDB_PROTOCOL_ARTIFICIAL_TABLE_ARTIFICIAL_TABLE_HPP_

#include <map>
#include <set>
#include <string>
#include <vector>

#include "containers/name_string.hpp"
#include "containers/uuid.hpp"
#include "rdb_protocol/context.hpp"

/* `artificial_table_t` is the subclass of `base_table_t` that represents a table in the
special `rethinkdb` database. Each of the tables in the `rethinkdb` database represents
a different type of underlying object, but it would be inefficient to duplicate the code
for handling each type of RethinkDB query across all of the different tables. Instead,
that logic lives in `artificial_table_t`, which translates the queries into a much
simpler format and then forwards them to an `artificial_table_backend_t`. */

class artificial_table_backend_t;

class artificial_table_t : public base_table_t {
public:
    artificial_table_t(
        rdb_context_t *rdb_context,
        database_id_t const &database_id,
        artificial_table_backend_t *backend);

    namespace_id_t get_id() const;
    const std::string &get_pkey() const;

    ql::datum_t read_row(ql::env_t *env,
        ql::datum_t pval, read_mode_t read_mode);
    counted_t<ql::datum_stream_t> read_all(
        ql::env_t *env,
        const std::string &get_all_sindex_id,
        ql::backtrace_id_t bt,
        const std::string &table_name,   /* the table's own name, for display purposes */
        const ql::datumspec_t &datumspec,
        sorting_t sorting,
        read_mode_t read_mode);
#if RDB_CF
    counted_t<ql::datum_stream_t> read_changes(
        ql::env_t *env,
        const ql::changefeed::streamspec_t &ss,
        ql::backtrace_id_t bt);
#endif  // RDB_CF
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
        std::vector<bool> &&pkey_was_autogenerated,
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

    static const uuid_u base_table_id;

private:
    /* `do_single_update()` can throw `interrupted_exc_t`, but it shouldn't throw query
    language exceptions; if `function()` throws a query language exception, then it will
    catch the exception and store it in `stats_inout`. */
    void do_single_update(
        ql::env_t *env,
        ql::datum_t pval,
        bool pkey_was_autogenerated,
        const std::function<ql::datum_t(ql::datum_t)>
            &function,
        return_changes_t return_changes,
        const signal_t *interruptor,
        ql::datum_t *stats_inout,
        std::set<std::string> *conditions_inout);

    rdb_context_t *m_rdb_context;
    database_id_t m_database_id;
    artificial_table_backend_t *m_backend;
    std::string m_primary_key_name;
};

#endif /* RDB_PROTOCOL_ARTIFICIAL_TABLE_ARTIFICIAL_TABLE_HPP_ */

