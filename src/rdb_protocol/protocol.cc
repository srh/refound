// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/protocol.hpp"

#include <algorithm>
#include <functional>

#include "stl_utils.hpp"

#include "btree/operations.hpp"
#include "btree/reql_specific.hpp"
#include "concurrency/cross_thread_signal.hpp"
#include "concurrency/cross_thread_watchable.hpp"
#include "containers/archive/boost_types.hpp"
#include "containers/archive/optional.hpp"
#include "containers/disk_backed_queue.hpp"
#include "rdb_protocol/btree.hpp"
#include "rdb_protocol/changefeed.hpp"
#include "rdb_protocol/context.hpp"
#include "rdb_protocol/distribution_progress.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/ql2proto.hpp"
#include "rdb_protocol/serialize_datum.hpp"
#include "rdb_protocol/store.hpp"

#include "debug.hpp"

namespace rdb_protocol {

void post_construct_and_drain_queue(
        auto_drainer_t::lock_t lock,
        uuid_u sindex_id_to_bring_up_to_date,
        key_range_t *construction_range_inout,
        int64_t max_pairs_to_construct,
        store_t *store,
        scoped_ptr_t<disk_backed_queue_wrapper_t<rdb_modification_report_t> >
            &&mod_queue)
    THROWS_NOTHING;

/* Creates a queue of operations for the sindex, runs a post construction for
 * the data already in the btree and finally drains the queue. */
void resume_construct_sindex(
        const uuid_u &sindex_to_construct,
        const key_range_t &construct_range,
        store_t *store,
        auto_drainer_t::lock_t store_keepalive) THROWS_NOTHING {
    with_priority_t p(CORO_PRIORITY_SINDEX_CONSTRUCTION);
    // TODO: Implement rockstore reading below.

    // Used by the `jobs` table and `indexStatus` to track the progress of the
    // construction.
    distribution_progress_estimator_t progress_estimator(
        store, store_keepalive.get_drain_signal());
    double current_progress =
        progress_estimator.estimate_progress(construct_range.left);
    map_insertion_sentry_t<
        store_t::sindex_context_map_t::key_type,
        store_t::sindex_context_map_t::mapped_type> sindex_context_sentry(
            store->get_sindex_context_map(),
            sindex_to_construct,
            std::make_pair(current_microtime(), &current_progress));

    /* We start by clearing out any residual data in the index left behind by a previous
    post construction process (if the server got terminated in the middle). */
    try {
        store->clear_sindex_data(
            sindex_to_construct,
            construct_range,
            store_keepalive.get_drain_signal());
    } catch (const interrupted_exc_t &) {
        return;
    }

    // TODO: Implement rockstore writing below.
    uuid_u post_construct_id = generate_uuid();

    /* Secondary indexes are constructed in multiple passes, moving through the primary
    key range from the smallest key to the largest one. In each pass, we handle a
    certain number of primary keys and put the corresponding entries into the secondary
    index. While this happens, we use a queue to keep track of any writes to the range
    we're constructing. We then drain the queue and atomically delete it, before we
    start the next pass. */
    const int64_t PAIRS_TO_CONSTRUCT_PER_PASS = 512;
    key_range_t remaining_range = construct_range;
    while (!remaining_range.is_empty()) {
        scoped_ptr_t<disk_backed_queue_wrapper_t<rdb_modification_report_t> > mod_queue;
        {
            /* Start a transaction and acquire the sindex_block */
            write_token_t token;
            store->new_write_token(&token);
            scoped_ptr_t<txn_t> txn;
            scoped_ptr_t<real_superblock_lock> superblock;
            try {
                store->acquire_superblock_for_write(1,
                                                    write_durability_t::SOFT,
                                                    &token,
                                                    &txn,
                                                    &superblock,
                                                    store_keepalive.get_drain_signal());
            } catch (const interrupted_exc_t &) {
                return;
            }
            superblock->sindex_block_write_signal()->wait_lazily_ordered();

            /* We register our modification queue here.
             * We must register it before calling post_construct_and_drain_queue to
             * make sure that every changes which we don't learn about in
             * the concurrent traversal that's started there, we do learn about from the
             * mod queue. Changes that happen between the mod queue registration and
             * the parallel traversal will be accounted for twice. That is ok though,
             * since every modification can be applied repeatedly without causing any
             * damage (if that should ever not true for any of the modifications, that
             * modification must be fixed or this code would have to be changed to
             * account for that). */
            const int64_t MAX_MOD_QUEUE_MEMORY_BYTES = 8 * MEGABYTE;
            mod_queue.init(
                    new disk_backed_queue_wrapper_t<rdb_modification_report_t>(
                        store->base_path_.path() + PATH_SEPARATOR +
                            "post_construction_" + uuid_to_str(post_construct_id),
                        &store->perfmon_collection,
                        MAX_MOD_QUEUE_MEMORY_BYTES));

            secondary_index_t sindex;
            bool found_index =
                get_secondary_index(store->rocksh(), superblock.get(), sindex_to_construct, &sindex);
            if (!found_index || sindex.being_deleted) {
                // The index was deleted. Abort construction.
                txn->commit(store->rocks, std::move(superblock));
                return;
            }

            new_mutex_in_line_t acq =
                store->get_in_line_for_sindex_queue(superblock.get());
            store->register_sindex_queue(mod_queue.get(), remaining_range, &acq);

            txn->commit(store->rocks, std::move(superblock));
        }

        // This updates `remaining_range`.
        post_construct_and_drain_queue(
            store_keepalive,
            sindex_to_construct,
            &remaining_range,
            PAIRS_TO_CONSTRUCT_PER_PASS,
            store,
            std::move(mod_queue));

        // Update the progress value
        current_progress = progress_estimator.estimate_progress(remaining_range.left);
    }
}

/* This function is used by resume_construct_sindex. It traverses the primary btree
and creates entries in the given secondary index. It then applies outstanding changes
from the mod_queue and deregisters it. */
void post_construct_and_drain_queue(
        auto_drainer_t::lock_t lock,
        uuid_u sindex_id_to_bring_up_to_date,
        key_range_t *construction_range_inout,
        int64_t max_pairs_to_construct,
        store_t *store,
        scoped_ptr_t<disk_backed_queue_wrapper_t<rdb_modification_report_t> >
            &&mod_queue)
    THROWS_NOTHING {

    // `post_construct_secondary_index_range` can post-construct multiple secondary
    // indexes at the same time. We don't currently make use of that functionality
    // though. (it doesn't make sense to remove it, since it doesn't add anything to
    // its complexity).
    std::set<uuid_u> sindexes_to_bring_up_to_date;
    sindexes_to_bring_up_to_date.insert(sindex_id_to_bring_up_to_date);

    try {
        const size_t MOD_QUEUE_SIZE_LIMIT = 16;
        // This constructs a part of the index and updates `construction_range_inout`
        // to the range that's still remaining.
        post_construct_secondary_index_range(
            store,
            sindexes_to_bring_up_to_date,
            construction_range_inout,
            // Abort if the mod_queue gets larger than the `MOD_QUEUE_SIZE_LIMIT`, or
            // we've constructed `max_pairs_to_construct` pairs.
            [&](int64_t pairs_constructed) {
                return pairs_constructed >= max_pairs_to_construct
                    || mod_queue->size() > MOD_QUEUE_SIZE_LIMIT;
            },
            lock.get_drain_signal());

        // Drain the queue.
        {
            write_token_t token;
            store->new_write_token(&token);

            scoped_ptr_t<txn_t> queue_txn;
            scoped_ptr_t<real_superblock_lock> queue_superblock;

            // We use HARD durability because we want post construction
            // to be throttled if we insert data faster than it can
            // be written to disk. Otherwise we might exhaust the cache's
            // dirty page limit and bring down the whole table.
            // Other than that, the hard durability guarantee is not actually
            // needed here.
            store->acquire_superblock_for_write(
                2 + mod_queue->size(),
                write_durability_t::HARD,
                &token,
                &queue_txn,
                &queue_superblock,
                lock.get_drain_signal());

            queue_superblock->sindex_block_write_signal()->wait();

            store_t::sindex_access_vector_t sindexes;
            store->acquire_sindex_superblocks_for_write(
                    make_optional(sindexes_to_bring_up_to_date),
                    queue_superblock.get(),
                    &sindexes);

            // Pretend that the indexes in `sindexes` have been post-constructed up to
            // the new range. This is important to make the call to
            // `rdb_update_sindexes()` below actually update the indexes.
            // We use the same trick in the `post_construct_traversal_helper_t`.
            // TODO: Avoid this hackery (here and in `post_construct_traversal_helper_t`)
            for (auto &&access : sindexes) {
                access->sindex.needs_post_construction_range = *construction_range_inout;
            }

            if (sindexes.empty()) {
                // We still need to deregister the queues. This is done further below
                // after the exception handler.
                // We throw here because that's consistent with how
                // `post_construct_secondary_index_range` signals the fact that all
                // indexes have been deleted.
                queue_txn->commit(store->rocksh().rocks, std::move(queue_superblock));
                throw interrupted_exc_t();
            }

            new_mutex_in_line_t acq =
                store->get_in_line_for_sindex_queue(queue_superblock.get());
            acq.acq_signal()->wait_lazily_unordered();

            while (mod_queue->size() > 0) {
                if (lock.get_drain_signal()->is_pulsed()) {
                    sindexes.clear();
                    queue_txn->commit(store->rocksh().rocks, std::move(queue_superblock));
                    throw interrupted_exc_t();
                }
                // The `disk_backed_queue_wrapper` can sometimes be non-empty, but not
                // have a value available because it's still loading from disk.
                // In that case we must wait until a value becomes available.
                while (!mod_queue->available->get()) {
                    // TODO: The availability_callback_t interface on passive producers
                    //   is difficult to use and should be simplified.
                    struct on_availability_t : public availability_callback_t {
                        void on_source_availability_changed() {
                            cond.pulse_if_not_already_pulsed();
                        }
                        cond_t cond;
                    } on_availability;
                    mod_queue->available->set_callback(&on_availability);
                    try {
                        wait_interruptible(&on_availability.cond,
                                           lock.get_drain_signal());
                    } catch (const interrupted_exc_t &) {
                        mod_queue->available->unset_callback();
                        sindexes.clear();
                        queue_txn->commit(store->rocksh().rocks, std::move(queue_superblock));
                        throw;
                    }
                    mod_queue->available->unset_callback();
                }
                rdb_modification_report_t mod_report = mod_queue->pop();
                // We only need to apply modifications that fall in the range that
                // has actually been constructed.
                // If it's in the range that is still to be constructed we ignore it.
                if (!construction_range_inout->contains_key(mod_report.primary_key)) {
                    rdb_update_sindexes(store->rocksh(),
                                        store,
                                        queue_superblock.get(),
                                        sindexes,
                                        &mod_report,
                                        NULL,
                                        NULL,
                                        NULL);
                }
            }

            // Mark parts of the index up to date (except for what remains in
            // `construction_range_inout`).
            store->mark_index_up_to_date(sindex_id_to_bring_up_to_date,
                                         queue_superblock.get(),
                                         *construction_range_inout);
            store->deregister_sindex_queue(mod_queue.get(), &acq);

            sindexes.clear();
            queue_txn->commit(store->rocksh().rocks, std::move(queue_superblock));

            return;
        }
    } catch (const interrupted_exc_t &) {
        // We were interrupted so we just exit. Sindex post construct is in an
        // indeterminate state and will be cleaned up at a later point.
    }

    if (lock.get_drain_signal()->is_pulsed()) {
        /* We were interrupted, this means we can't deregister the sindex queue
         * the standard way because it requires blocks. Use the emergency
         * method instead. */
        store->emergency_deregister_sindex_queue(mod_queue.get());
    } else {
        /* The sindexes we were post constructing were all deleted. Time to
         * deregister the queue. */
        write_token_t token;
        store->new_write_token(&token);

        scoped_ptr_t<txn_t> queue_txn;
        scoped_ptr_t<real_superblock_lock> queue_superblock;

        cond_t non_interruptor;
        store->acquire_superblock_for_write(
            2,
            write_durability_t::HARD,
            &token,
            &queue_txn,
            &queue_superblock,
            &non_interruptor);

        queue_superblock->sindex_block_write_signal()->wait();

        new_mutex_in_line_t acq =
            store->get_in_line_for_sindex_queue(queue_superblock.get());
        store->deregister_sindex_queue(mod_queue.get(), &acq);

        queue_txn->commit(store->rocksh().rocks, std::move(queue_superblock));
    }
}

}  // namespace rdb_protocol

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

#if RDB_CF
    region_t operator()(const changefeed_subscribe_t &) const {
        return region_t::universe();
    }

    region_t operator()(const changefeed_limit_subscribe_t &s) const {
        return s.region;
    }

    region_t operator()(const changefeed_stamp_t &) const {
        return region_t::universe();
    }

    region_t operator()(const changefeed_point_stamp_t &t) const {
        return rdb_protocol::monokey_region(t.key);
    }
#endif  // RDB_CF

    region_t operator()(const dummy_read_t &d) const {
        return d.region;
    }
};

region_t read_t::get_region() const THROWS_NOTHING {
    return boost::apply_visitor(rdb_r_get_region_visitor(), read);
}

#if RDB_CF
struct route_to_primary_visitor_t : public boost::static_visitor<bool> {
    // `include_initial` changefeed reads must be routed to the primary, since
    // that's where changefeeds are managed.
    bool operator()(RDB_CF_UNUSED const rget_read_t &rget) const {
        return rget.stamp.has_value();
    }
    bool operator()(RDB_CF_UNUSED const intersecting_geo_read_t &geo_read) const {
        return geo_read.stamp.has_value();
    }

    bool operator()(const point_read_t &) const {                 return false; }
    bool operator()(const dummy_read_t &) const {                 return false; }
    bool operator()(const nearest_geo_read_t &) const {           return false; }
    bool operator()(const changefeed_subscribe_t &) const {       return true;  }
    bool operator()(const changefeed_limit_subscribe_t &) const { return true;  }
    bool operator()(const changefeed_stamp_t &) const {           return true;  }
    bool operator()(const changefeed_point_stamp_t &) const {     return true;  }
};

// Route changefeed reads to the primary replica. For other reads we don't care.
bool read_t::route_to_primary() const THROWS_NOTHING {
    return boost::apply_visitor(route_to_primary_visitor_t(), read);
}
#endif  // RDB_CF

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
        const optional<ql::deterministic_func> &_write_hook,
        conflict_behavior_t _conflict_behavior,
        const optional<ql::deterministic_func> &_conflict_func,
        const ql::configured_limits_t &_limits,
        serializable_env_t s_env,
        return_changes_t _return_changes)
        : inserts(std::move(_inserts)),
          pkey(_pkey),
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
#if RDB_CF
RDB_IMPL_SERIALIZABLE_2_FOR_CLUSTER(
    rget_read_response_t, stamp_response, result);
#else
RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(
    rget_read_response_t, result);
#endif
RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(nearest_geo_read_response_t, results_or_error);

#if RDB_CF
RDB_IMPL_SERIALIZABLE_2_FOR_CLUSTER(
    changefeed_subscribe_response_t, server_uuids, addrs);
RDB_IMPL_SERIALIZABLE_2_FOR_CLUSTER(
    changefeed_limit_subscribe_response_t, shards, limit_addrs);
RDB_IMPL_SERIALIZABLE_2_FOR_CLUSTER(
    shard_stamp_info_t, stamp, last_read_start);
RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(changefeed_stamp_response_t, stamp_infos);

RDB_IMPL_SERIALIZABLE_2_FOR_CLUSTER(
    changefeed_point_stamp_response_t::valid_response_t, stamp, initial_val);
RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(
    changefeed_point_stamp_response_t, resp);
#endif  // RDB_CF

RDB_IMPL_SERIALIZABLE_3_FOR_CLUSTER(read_response_t, response, event_log, n_shards);
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
#if RDB_CF
RDB_IMPL_SERIALIZABLE_10_FOR_CLUSTER(
    rget_read_t,
    stamp,
    region,
    primary_keys,
    serializable_env,
    table_name,
    batchspec,
    transforms,
    terminal,
    sindex,
    sorting);
#else
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
#endif  // RDB_CF
#if RDB_CF
RDB_IMPL_SERIALIZABLE_8_FOR_CLUSTER(
    intersecting_geo_read_t,
    stamp,
    serializable_env,
    table_name,
    batchspec,
    transforms,
    terminal,
    sindex,
    query_geometry);
#else
RDB_IMPL_SERIALIZABLE_7_FOR_CLUSTER(
    intersecting_geo_read_t,
    serializable_env,
    table_name,
    batchspec,
    transforms,
    terminal,
    sindex,
    query_geometry);
#endif  // RDB_CF
RDB_IMPL_SERIALIZABLE_7_FOR_CLUSTER(
    nearest_geo_read_t,
    serializable_env,
    center,
    max_dist,
    max_results,
    geo_system,
    table_name,
    sindex_id);

#if RDB_CF
RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(changefeed_subscribe_t, addr);
RDB_IMPL_SERIALIZABLE_6_FOR_CLUSTER(
    changefeed_limit_subscribe_t,
    addr,
    uuid,
    spec,
    table,
    serializable_env,
    region);
RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(changefeed_stamp_t, addr);
RDB_IMPL_SERIALIZABLE_2_FOR_CLUSTER(changefeed_point_stamp_t, addr, key);
#endif  // RDB_CF

RDB_IMPL_SERIALIZABLE_3_FOR_CLUSTER(read_t, read, profile, read_mode);

RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(point_write_response_t, result);
RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(point_delete_response_t, result);
RDB_IMPL_SERIALIZABLE_0_FOR_CLUSTER(sync_response_t);
RDB_IMPL_SERIALIZABLE_0_FOR_CLUSTER(dummy_write_response_t);

RDB_IMPL_SERIALIZABLE_3_FOR_CLUSTER(write_response_t, response, event_log, n_shards);

RDB_IMPL_SERIALIZABLE_6_FOR_CLUSTER(
        batched_replace_t,
        keys,
        pkey,
        f,
        write_hook,
        serializable_env,
        return_changes);
RDB_IMPL_SERIALIZABLE_8_FOR_CLUSTER(
        batched_insert_t,
        inserts,
        pkey,
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


