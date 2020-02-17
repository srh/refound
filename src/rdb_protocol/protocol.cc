// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/protocol.hpp"

#include <algorithm>
#include <functional>

#include "stl_utils.hpp"

#include "containers/archive/boost_types.hpp"
#include "containers/archive/optional.hpp"
#include "rdb_protocol/btree.hpp"
#include "rdb_protocol/changefeed.hpp"
#include "rdb_protocol/context.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/ql2proto.hpp"
#include "rdb_protocol/serialize_datum.hpp"

#include "debug.hpp"

namespace rdb_protocol {
// Construct a region containing only the specified key
region_t monokey_region(const store_key_t &k) {
    return key_range_t(key_range_t::closed, k, key_range_t::closed, k);
}

key_range_t sindex_key_range(const store_key_t &start,
                             const store_key_t &end,
                             key_range_t::bound_t end_type) {

    const size_t max_trunc_size = ql::datum_t::max_trunc_size();

    // If `end` is not truncated and right bound is open, we don't increment the right
    // bound.
    guarantee(static_cast<size_t>(end.size()) <= max_trunc_size);
    key_or_max end_key;
    const bool end_is_truncated = static_cast<size_t>(end.size()) == max_trunc_size;
    // The key range we generate must be open on the right end because the keys in the
    // btree have extra data appended to the secondary key part.
    if (end_is_truncated) {
        // Since the key is already truncated, we must make it larger without making it
        // longer.
        std::string end_key_str(key_to_unescaped_str(end));
        while (end_key_str.length() > 0 &&
               end_key_str[end_key_str.length() - 1] == static_cast<char>(255)) {
            end_key_str.erase(end_key_str.length() - 1);
        }

        if (end_key_str.length() == 0) {
            end_key = key_or_max::infinity();
        } else {
            ++end_key_str[end_key_str.length() - 1];
            end_key = key_or_max(store_key_t(end_key_str));
        }
    } else if (end_type == key_range_t::bound_t::closed) {
        // `end` is not truncated, but the range is closed. We know that `end` is
        // currently terminated by a null byte. We can replace that by a '\1' to ensure
        // that any key in the btree with that exact secondary index value will be
        // included in the range.
        end_key = key_or_max(end);
        guarantee(end_key.key.size() > 0);
        guarantee(end_key.key.str().back() == 0);
        end_key.key.str().back() = 1;
    } else {
        end_key = key_or_max(end);
    }
    return half_open_key_range(start, std::move(end_key));
}

}  // namespace rdb_protocol

/* read_t::get_region implementation */
struct rdb_r_get_region_visitor : public boost::static_visitor<region_t> {
    region_t operator()(const point_read_t &pr) const {
        return rdb_protocol::monokey_region(pr.key);
    }

    region_t operator()(const rget_read_t &rg) const {
        return rg.region;
    }

    region_t operator()(const intersecting_geo_read_t &) const {
        return region_t::universe();
    }

    region_t operator()(const nearest_geo_read_t &) const {
        return region_t::universe();
    }

    region_t operator()(const dummy_read_t &d) const {
        return d.region;
    }
};

region_t read_t::get_region() const THROWS_NOTHING {
    return boost::apply_visitor(rdb_r_get_region_visitor(), read);
}

/* write_t::get_region() implementation */

region_t region_from_keys(const std::vector<store_key_t> &keys) {
    // It shouldn't be empty, but we let the places that would break use a
    // guarantee.
    rassert(!keys.empty());
    if (keys.empty()) {
        return region_t();
    }

    size_t min_key = 0;
    size_t max_key = 0;
    
    for (size_t i = 1, e = keys.size(); i < e; ++i) {
        const store_key_t &key = keys[i];
        if (key < keys[min_key]) {
            min_key = i;
        }
        if (key > keys[max_key]) {
            max_key = i;
        }
    }

    return key_range_t(
        key_range_t::closed, keys[min_key],
        key_range_t::closed, keys[max_key]);
}

struct rdb_w_get_region_visitor : public boost::static_visitor<region_t> {
    region_t operator()(const batched_replace_t &br) const {
        return region_from_keys(br.keys);
    }
    region_t operator()(const batched_insert_t &bi) const {
        std::vector<store_key_t> keys;
        keys.reserve(bi.inserts.size());
        for (auto it = bi.inserts.begin(); it != bi.inserts.end(); ++it) {
            keys.emplace_back((*it).get_field(datum_string_t(bi.pkey)).print_primary());
        }
        return region_from_keys(keys);
    }

    region_t operator()(const point_write_t &pw) const {
        return rdb_protocol::monokey_region(pw.key);
    }

    region_t operator()(const point_delete_t &pd) const {
        return rdb_protocol::monokey_region(pd.key);
    }

    region_t operator()(const sync_t &s) const {
        return s.region;
    }

    region_t operator()(const dummy_write_t &d) const {
        return d.region;
    }
};

#ifndef NDEBUG
// This is slow, and should only be used in debug mode for assertions.
region_t write_t::get_region() const THROWS_NOTHING {
    return boost::apply_visitor(rdb_w_get_region_visitor(), write);
}
#endif // NDEBUG

/* write_t::shard implementation */

batched_insert_t::batched_insert_t(
        std::vector<ql::datum_t> &&_inserts,
        const std::string &_pkey,
        ignore_write_hook_t _ignore_write_hook,
        const optional<ql::deterministic_func> &_write_hook,
        conflict_behavior_t _conflict_behavior,
        const optional<ql::deterministic_func> &_conflict_func,
        const ql::configured_limits_t &_limits,
        serializable_env_t s_env,
        return_changes_t _return_changes)
        : inserts(std::move(_inserts)),
          pkey(_pkey),
          ignore_write_hook(_ignore_write_hook),
          write_hook(_write_hook),
          conflict_behavior(_conflict_behavior),
          conflict_func(_conflict_func),
          limits(_limits),
          serializable_env(std::move(s_env)),
          return_changes(_return_changes) {
    r_sanity_check(inserts.size() != 0);

#ifndef NDEBUG
    // These checks are done above us, but in debug mode we do them
    // again.  (They're slow.)  We do them above us because the code in
    // val.cc knows enough to report the write errors correctly while
    // still doing the other writes.
    for (auto it = inserts.begin(); it != inserts.end(); ++it) {
        ql::datum_t keyval =
            it->get_field(datum_string_t(pkey), ql::NOTHROW);
        r_sanity_check(keyval.has());
        try {
            keyval.print_primary(); // ERROR CHECKING
            continue;
        } catch (const ql::base_exc_t &e) {
        }
        r_sanity_check(false); // throws, so can't do this in exception handler
    }
#endif // NDEBUG
}

struct rdb_w_expected_document_changes_visitor_t : public boost::static_visitor<int> {
    rdb_w_expected_document_changes_visitor_t() { }
    int operator()(const batched_replace_t &w) const {
        return w.keys.size();
    }
    int operator()(const batched_insert_t &w) const {
        return w.inserts.size();
    }
    int operator()(const point_write_t &) const { return 1; }
    int operator()(const point_delete_t &) const { return 1; }
    int operator()(const sync_t &) const { return 0; }
    int operator()(const dummy_write_t &) const { return 0; }
};

int write_t::expected_document_changes() const {
    const rdb_w_expected_document_changes_visitor_t visitor;
    return boost::apply_visitor(visitor, write);
}

RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(point_read_response_t, data);
RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(
    rget_read_response_t, result);
RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(nearest_geo_read_response_t, results_or_error);

RDB_IMPL_SERIALIZABLE_2_FOR_CLUSTER(read_response_t, response, event_log);
RDB_IMPL_SERIALIZABLE_0_FOR_CLUSTER(dummy_read_response_t);

RDB_IMPL_SERIALIZABLE_3_FOR_CLUSTER(
    serializable_env_t, global_optargs, user_context, deterministic_time);

RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(point_read_t, key);
RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(dummy_read_t, region);
RDB_IMPL_SERIALIZABLE_4_FOR_CLUSTER(sindex_rangespec_t,
                                    id,
                                    region,
                                    datumspec,
                                    require_sindex_val);

ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(
        sorting_t, int8_t,
        sorting_t::UNORDERED, sorting_t::DESCENDING);
RDB_IMPL_SERIALIZABLE_9_FOR_CLUSTER(
    rget_read_t,
    region,
    primary_keys,
    serializable_env,
    table_name,
    batchspec,
    transforms,
    terminal,
    sindex,
    sorting);
RDB_IMPL_SERIALIZABLE_7_FOR_CLUSTER(
    intersecting_geo_read_t,
    serializable_env,
    table_name,
    batchspec,
    transforms,
    terminal,
    sindex,
    query_geometry);
RDB_IMPL_SERIALIZABLE_7_FOR_CLUSTER(
    nearest_geo_read_t,
    serializable_env,
    center,
    max_dist,
    max_results,
    geo_system,
    table_name,
    sindex_id);

RDB_IMPL_SERIALIZABLE_3_FOR_CLUSTER(read_t, read, profile, read_mode);

RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(point_write_response_t, result);
RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(point_delete_response_t, result);
RDB_IMPL_SERIALIZABLE_0_FOR_CLUSTER(sync_response_t);
RDB_IMPL_SERIALIZABLE_0_FOR_CLUSTER(dummy_write_response_t);

RDB_IMPL_SERIALIZABLE_2_FOR_CLUSTER(write_response_t, response, event_log);

RDB_IMPL_SERIALIZABLE_7_FOR_CLUSTER(
        batched_replace_t,
        keys,
        pkey,
        f,
        ignore_write_hook,
        write_hook,
        serializable_env,
        return_changes);
RDB_IMPL_SERIALIZABLE_9_FOR_CLUSTER(
        batched_insert_t,
        inserts,
        pkey,
        ignore_write_hook,
        write_hook,
        conflict_behavior,
        conflict_func,
        limits,
        serializable_env,
        return_changes);

RDB_IMPL_SERIALIZABLE_3_FOR_CLUSTER(point_write_t, key, data, overwrite);
RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(point_delete_t, key);
RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(sync_t, region);
RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(dummy_write_t, region);

RDB_IMPL_SERIALIZABLE_4_FOR_CLUSTER(
    write_t, write, durability_requirement, profile, limits);


