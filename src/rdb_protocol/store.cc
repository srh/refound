// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "rdb_protocol/store.hpp"

#include "btree/backfill_debug.hpp"
#include "btree/reql_specific.hpp"
#include "btree/operations.hpp"
#include "concurrency/cross_thread_signal.hpp"
#include "concurrency/cross_thread_watchable.hpp"
#include "concurrency/wait_any.hpp"
#include "containers/archive/vector_stream.hpp"
#include "rdb_protocol/btree.hpp"
#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/erase_range.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/shards.hpp"
#include "rdb_protocol/store_metainfo.hpp"
#include "rdb_protocol/table_common.hpp"

void store_t::note_reshard(const region_t &shard_region) {
    // We acquire `changefeed_servers_lock` and move the matching pointer out of
    // `changefeed_servers`. We then destruct the server in a separate step,
    // after releasing the lock.
    // The reason we do this is to avoid deadlocks that could happen if someone
    // was holding a lock on the drainer in one of the changefeed servers,
    // and was at the same time trying to acquire the `changefeed_servers_lock`.
    scoped_ptr_t<ql::changefeed::server_t> to_destruct;
    {
        rwlock_acq_t acq(&changefeed_servers_lock, access_t::write);
        ASSERT_NO_CORO_WAITING;
        // Shards use unbounded right boundaries, while changefeed queries use MAX_KEY.
        // We must convert the boundary here so it matches.
        region_t modified_region = shard_region;
        modified_region.inner.right =
            key_range_t::right_bound_t(modified_region.inner.right_or_max());
        auto it = changefeed_servers.find(modified_region);
        if (it != changefeed_servers.end()) {
            to_destruct = std::move(it->second);
            changefeed_servers.erase(it);
        }
    }
    // The changefeed server is actually getting destructed here. This might
    // block.
}

reql_version_t update_sindex_last_compatible_version(
        rockshard rocksh,
        secondary_index_t *sindex,
        sindex_block_lock *sindex_block) {
    sindex_disk_info_t sindex_info;
    deserialize_sindex_info_or_crash(sindex->opaque_definition, &sindex_info);

    reql_version_t res = sindex_info.mapping_version_info.original_reql_version;

    if (sindex_info.mapping_version_info.latest_checked_reql_version
        != reql_version_t::LATEST) {

        sindex_info.mapping_version_info.latest_compatible_reql_version = res;
        sindex_info.mapping_version_info.latest_checked_reql_version =
            reql_version_t::LATEST;

        write_message_t wm;
        serialize_sindex_info(&wm, sindex_info);

        vector_stream_t stream;
        stream.reserve(wm.size());
        int write_res = send_write_message(&stream, &wm);
        guarantee(write_res == 0);

        sindex->opaque_definition = stream.vector();

        ::set_secondary_index(rocksh, sindex_block, sindex->id, *sindex);
    }

    return res;
}

void store_t::help_construct_bring_sindexes_up_to_date() {
    // Make sure to continue bringing sindexes up-to-date if it was interrupted earlier

    // This uses a dummy interruptor because this is the only thing using the store at
    //  the moment (since we are still in the constructor), so things should complete
    //  rather quickly.
    cond_t dummy_interruptor;
    write_token_t token;
    new_write_token(&token);

    scoped_ptr_t<txn_t> txn;
    scoped_ptr_t<real_superblock_t> superblock;
    acquire_superblock_for_write(1,
                                 write_durability_t::SOFT,
                                 &token,
                                 &txn,
                                 &superblock,
                                 &dummy_interruptor);

    sindex_block_lock sindex_block(
        superblock->get(),
        access_t::write);

    superblock.reset();

    auto clear_sindex = [this](uuid_u sindex_id,
                               auto_drainer_t::lock_t store_keepalive) {
        try {
            /* Clear the sindex. */
            clear_sindex_data(
                sindex_id,
                key_range_t::universe(),
                store_keepalive.get_drain_signal());

            /* Drop the sindex, now that it's empty. */
            drop_sindex(sindex_id);
        } catch (const interrupted_exc_t &e) {
            /* Ignore */
        }
    };

    // Get the map of indexes and check if any were postconstructing or being deleted.
    // Kick off coroutines to finish the respective operations
    {
        std::map<sindex_name_t, secondary_index_t> sindexes;
        get_secondary_indexes(rocksh(), &sindex_block, &sindexes);
        for (auto it = sindexes.begin(); it != sindexes.end(); ++it) {
            if (it->second.being_deleted) {
                coro_t::spawn_sometime(std::bind(clear_sindex,
                                                 it->second.id, drainer.lock()));
            } else if (!it->second.post_construction_complete()) {
                // TODO: rocks read operations in resume_construct_sindex.
                coro_t::spawn_sometime(std::bind(&rdb_protocol::resume_construct_sindex,
                                                 it->second.id,
                                                 it->second.needs_post_construction_range,
                                                 this,
                                                 drainer.lock()));
            }
        }
    }

    sindex_block.reset_buf_lock();
    txn->commit();
}

scoped_ptr_t<sindex_superblock_t> acquire_sindex_for_read(
    store_t *store,
    real_superblock_t *superblock,
    release_superblock_t release_superblock,
    const std::string &table_name,
    const std::string &sindex_id,
    sindex_disk_info_t *sindex_info_out,
    uuid_u *sindex_uuid_out) {
    rassert(sindex_info_out != NULL);
    rassert(sindex_uuid_out != NULL);

    scoped_ptr_t<sindex_superblock_t> sindex_sb;
    std::vector<char> sindex_mapping_data;

    uuid_u sindex_uuid;
    try {
        bool found = store->acquire_sindex_superblock_for_read(
            sindex_name_t(sindex_id),
            table_name,
            superblock,
            release_superblock,
            &sindex_sb,
            &sindex_mapping_data,
            &sindex_uuid);
        // TODO: consider adding some logic on the machine handling the
        // query to attach a real backtrace here.
        rcheck_toplevel(found, ql::base_exc_t::OP_FAILED,
                strprintf("Index `%s` was not found on table `%s`.",
                          sindex_id.c_str(), table_name.c_str()));
    } catch (const sindex_not_ready_exc_t &e) {
        throw ql::exc_t(
            ql::base_exc_t::OP_FAILED, e.what(), ql::backtrace_id_t::empty());
    }

    try {
        deserialize_sindex_info_or_crash(sindex_mapping_data, sindex_info_out);
    } catch (const archive_exc_t &e) {
        crash("%s", e.what());
    }

    *sindex_uuid_out = sindex_uuid;
    return sindex_sb;
}

// TODO: Is sindex_id_out always nullptr?
void do_snap_read(
        rockshard rocksh,
        ql::env_t *env,
        store_t *store,
        btree_slice_t *btree,
        real_superblock_t *superblock,
        const rget_read_t &rget,
        rget_read_response_t *res,
        optional<uuid_u> *sindex_id_out) {
    guarantee(rget.current_shard.has_value());
    if (!rget.sindex.has_value()) {
        // rget using a primary index
        if (sindex_id_out != nullptr) {
            *sindex_id_out = r_nullopt;
        }
        superblock->read_acq_signal()->wait_lazily_ordered();
        rockstore::snapshot snap = make_snapshot(rocksh.rocks);
        superblock->release();

        rdb_rget_snapshot_slice(
            snap.snap,
            rocksh,
            btree,
            *rget.current_shard,
            rget.region.inner,
            rget.primary_keys,
            env,
            rget.batchspec,
            rget.transforms,
            rget.terminal,
            rget.sorting,
            res);
    } else {
        // rget using a secondary index
        sindex_disk_info_t sindex_info;
        uuid_u sindex_uuid;
        scoped_ptr_t<sindex_superblock_t> sindex_sb;
        key_range_t sindex_range;
        try {
            sindex_sb =
                acquire_sindex_for_read(
                    store,
                    superblock,
                    release_superblock_t::RELEASE,
                    rget.table_name,
                    rget.sindex->id,
                    &sindex_info,
                    &sindex_uuid);
            if (sindex_id_out != nullptr) {
                *sindex_id_out = make_optional(sindex_uuid);
            }
            reql_version_t reql_version =
                sindex_info.mapping_version_info.latest_compatible_reql_version;
            res->reql_version = reql_version;
            if (rget.sindex->region.has_value()) {
                sindex_range = rget.sindex->region->inner;
            } else {
                sindex_range =
                    rget.sindex->datumspec.covering_range().to_sindex_keyrange(
                        reql_version);
            }
            if (sindex_info.geo == sindex_geo_bool_t::GEO) {
                res->result = ql::exc_t(
                    ql::base_exc_t::LOGIC,
                    strprintf(
                        "Index `%s` is a geospatial index.  Only get_nearest and "
                        "get_intersecting can use a geospatial index.",
                        rget.sindex->id.c_str()),
                    ql::backtrace_id_t::empty());
                return;
            }

            sindex_sb->read_acq_signal()->wait_lazily_ordered();
            rockstore::snapshot snap = make_snapshot(rocksh.rocks);
            sindex_sb->release();

            rdb_rget_secondary_snapshot_slice(
                snap.snap,
                rocksh,
                sindex_uuid,
                store->get_sindex_slice(sindex_uuid),
                *rget.current_shard,
                rget.sindex->datumspec,
                sindex_range,
                env,
                rget.batchspec,
                rget.transforms,
                rget.terminal,
                rget.region.inner,
                rget.sorting,
                rget.sindex->require_sindex_val,
                sindex_info,
                res);
        } catch (const ql::exc_t &e) {
            res->result = e;
            return;
        } catch (const ql::datum_exc_t &e) {
            // TODO: consider adding some logic on the machine handling the
            // query to attach a real backtrace here.
            res->result = ql::exc_t(e, ql::backtrace_id_t::empty());
            return;
        }
    }
}

// TODO: Remove this?
void do_read(rockshard rocksh,
             ql::env_t *env,
             store_t *store,
             btree_slice_t *btree,
             real_superblock_t *superblock,
             const rget_read_t &rget,
             rget_read_response_t *res,
             release_superblock_t release_superblock,
             optional<uuid_u> *sindex_id_out) {
    guarantee(rget.current_shard.has_value());
    if (!rget.sindex.has_value()) {
        // rget using a primary index
        if (sindex_id_out != nullptr) {
            *sindex_id_out = r_nullopt;
        }
        rdb_rget_slice(
            rocksh,
            btree,
            *rget.current_shard,
            rget.region.inner,
            rget.primary_keys,
            superblock,
            env,
            rget.batchspec,
            rget.transforms,
            rget.terminal,
            rget.sorting,
            res,
            release_superblock);
    } else {
        // rget using a secondary index
        sindex_disk_info_t sindex_info;
        uuid_u sindex_uuid;
        scoped_ptr_t<sindex_superblock_t> sindex_sb;
        key_range_t sindex_range;
        try {
            sindex_sb =
                acquire_sindex_for_read(
                    store,
                    superblock,
                    release_superblock,
                    rget.table_name,
                    rget.sindex->id,
                    &sindex_info,
                    &sindex_uuid);
            if (sindex_id_out != nullptr) {
                *sindex_id_out = make_optional(sindex_uuid);
            }
            reql_version_t reql_version =
                sindex_info.mapping_version_info.latest_compatible_reql_version;
            res->reql_version = reql_version;
            if (rget.sindex->region.has_value()) {
                sindex_range = rget.sindex->region->inner;
            } else {
                sindex_range =
                    rget.sindex->datumspec.covering_range().to_sindex_keyrange(
                        reql_version);
            }
            if (sindex_info.geo == sindex_geo_bool_t::GEO) {
                res->result = ql::exc_t(
                    ql::base_exc_t::LOGIC,
                    strprintf(
                        "Index `%s` is a geospatial index.  Only get_nearest and "
                        "get_intersecting can use a geospatial index.",
                        rget.sindex->id.c_str()),
                    ql::backtrace_id_t::empty());
                return;
            }

            rdb_rget_secondary_slice(
                rocksh,
                sindex_uuid,
                store->get_sindex_slice(sindex_uuid),
                *rget.current_shard,
                rget.sindex->datumspec,
                sindex_range,
                sindex_sb.get(),
                env,
                rget.batchspec,
                rget.transforms,
                rget.terminal,
                rget.region.inner,
                rget.sorting,
                rget.sindex->require_sindex_val,
                sindex_info,
                res,
                release_superblock_t::RELEASE);
        } catch (const ql::exc_t &e) {
            res->result = e;
            return;
        } catch (const ql::datum_exc_t &e) {
            // TODO: consider adding some logic on the machine handling the
            // query to attach a real backtrace here.
            res->result = ql::exc_t(e, ql::backtrace_id_t::empty());
            return;
        }
    }
}

// TODO: get rid of this extra response_t copy on the stack
struct rdb_read_visitor_t : public boost::static_visitor<void> {
    void operator()(const changefeed_subscribe_t &s) {
        auto cserver = store->get_or_make_changefeed_server(s.shard_region);
        guarantee(cserver.first != nullptr);
        cserver.first->add_client(s.addr, s.shard_region, cserver.second);
        response->response = changefeed_subscribe_response_t();
        auto res = boost::get<changefeed_subscribe_response_t>(&response->response);
        guarantee(res != NULL);
        res->server_uuids.insert(cserver.first->get_uuid());
        res->addrs.insert(cserver.first->get_stop_addr());
    }

    void operator()(const changefeed_limit_subscribe_t &s) {
        ql::env_t env(
            ctx,
            ql::return_empty_normal_batches_t::NO,
            interruptor,
            s.serializable_env,
            trace);
        ql::raw_stream_t stream;
        optional<uuid_u> sindex_id;
        {
            std::vector<scoped_ptr_t<ql::op_t> > ops;
            for (const auto &transform : s.spec.range.transforms) {
                ops.push_back(make_op(transform));
            }
            rget_read_t rget;
            rget.region = s.region;
            rget.current_shard = s.current_shard;
            rget.table_name = s.table;
            rget.batchspec = ql::batchspec_t::all(); // Terminal takes care of stopping.
            if (s.spec.range.sindex) {
                rget.terminal.set(ql::limit_read_t{
                    is_primary_t::NO,
                    s.spec.limit,
                    s.region,
                    !reversed(s.spec.range.sorting)
                        ? store_key_t::min()
                        : store_key_t::max(),
                    s.spec.range.sorting,
                    &ops});
                rget.sindex.set(sindex_rangespec_t(
                    *s.spec.range.sindex,
                    r_nullopt, // We just want to use whole range.
                    s.spec.range.datumspec));
            } else {
                rget.terminal.set(ql::limit_read_t{
                    is_primary_t::YES,
                    s.spec.limit,
                    s.region,
                    !reversed(s.spec.range.sorting)
                        ? store_key_t::min()
                        : store_key_t::max(),
                    s.spec.range.sorting,
                    &ops});
            }
            rget.sorting = s.spec.range.sorting;

            // The superblock will instead be released in `store_t::read`
            // shortly after this function returns.
            rget_read_response_t resp;
            do_read(store->rocksh(), &env, store, btree, superblock, rget, &resp,
                    release_superblock_t::KEEP,
                    &sindex_id);
            auto *gs = boost::get<ql::grouped_t<ql::stream_t> >(&resp.result);
            if (gs == NULL) {
                auto *exc = boost::get<ql::exc_t>(&resp.result);
                guarantee(exc != NULL);
                response->response = resp;
                return;
            }
            ql::stream_t read_stream = groups_to_batch(gs->get_underlying_map());
            guarantee(read_stream.substreams.size() <= 1);
            if (read_stream.substreams.size() == 1) {
                stream = std::move(read_stream.substreams.begin()->second.stream);
            } else {
                guarantee(stream.size() == 0);
            }
        }
        auto lvec = ql::changefeed::mangle_sort_truncate_stream(
            std::move(stream),
            s.spec.range.sindex ? is_primary_t::NO : is_primary_t::YES,
            s.spec.range.sorting,
            s.spec.limit);

        guarantee(s.current_shard.has_value());
        auto cserver = store->get_or_make_changefeed_server(*s.current_shard);
        guarantee(cserver.first != nullptr);
        cserver.first->add_limit_client(
            s.addr,
            s.region,
            s.table,
            sindex_id,
            ctx,
            s.serializable_env,
            s.uuid,
            s.spec,
            ql::changefeed::limit_order_t(s.spec.range.sorting),
            std::move(lvec),
            cserver.second);
        auto addr = cserver.first->get_limit_stop_addr();
        std::vector<decltype(addr)> vec{addr};
        response->response = changefeed_limit_subscribe_response_t(1, std::move(vec));
    }

    changefeed_stamp_response_t do_stamp(const changefeed_stamp_t &s,
                                         const region_t &current_shard,
                                         const store_key_t &read_start) {
        superblock->get()->read_acq_signal()->wait_lazily_ordered();

        auto cserver = store->changefeed_server(s.region);
        if (cserver.first != nullptr) {
            if (optional<uint64_t> stamp
                    = cserver.first->get_stamp(s.addr, cserver.second)) {
                changefeed_stamp_response_t out;
                out.stamp_infos.set(std::map<uuid_u, shard_stamp_info_t>());
                (*out.stamp_infos)[cserver.first->get_uuid()] = shard_stamp_info_t{
                    *stamp,
                    current_shard,
                    read_start};
                return out;
            }
        }
        return changefeed_stamp_response_t();
    }

    void operator()(const changefeed_stamp_t &s) {
        response->response = do_stamp(s, s.region, s.region.inner.left);
    }

    void operator()(const changefeed_point_stamp_t &s) {
        // Need to wait for the superblock to make sure we get the right changefeed
        // stamp.
        superblock->get()->read_acq_signal()->wait_lazily_ordered();

        response->response = changefeed_point_stamp_response_t();
        auto *res = boost::get<changefeed_point_stamp_response_t>(&response->response);
        auto cserver = store->changefeed_server(s.key);
        if (cserver.first != nullptr) {
            res->resp.set(changefeed_point_stamp_response_t::valid_response_t());
            auto *vres = &*res->resp;
            if (optional<uint64_t> stamp
                    = cserver.first->get_stamp(s.addr, cserver.second)) {
                vres->stamp = std::make_pair(cserver.first->get_uuid(), *stamp);
            } else {
                // The client was removed, so no future messages are coming.
                vres->stamp = std::make_pair(cserver.first->get_uuid(),
                                             std::numeric_limits<uint64_t>::max());
            }
            point_read_response_t val;
            rdb_get(store->rocksh(), s.key, superblock, &val);
            vres->initial_val = val.data;
        } else {
            res->resp.reset();
        }
    }

    void operator()(const point_read_t &get) {
        response->response = point_read_response_t();
        point_read_response_t *res =
            boost::get<point_read_response_t>(&response->response);
        rdb_get(store->rocksh(), get.key, superblock, res);
    }

    void operator()(const intersecting_geo_read_t &geo_read) {
        // TODO: We construct this kind of early.
        ql::env_t ql_env(
            ctx,
            ql::return_empty_normal_batches_t::NO,
            interruptor,
            geo_read.serializable_env,
            trace);

        response->response = rget_read_response_t();
        rget_read_response_t *res =
            boost::get<rget_read_response_t>(&response->response);

        if (geo_read.stamp.has_value()) {
            res->stamp_response.set(changefeed_stamp_response_t());

            store_key_t read_left = geo_read.sindex.region
                ? geo_read.sindex.region->inner.left
                : store_key_t::min();

            changefeed_stamp_response_t r = do_stamp(
                *geo_read.stamp,
                geo_read.region,
                read_left);
            if (r.stamp_infos) {
                res->stamp_response.set(r);
            } else {
                res->result = ql::exc_t(
                    ql::base_exc_t::OP_FAILED,
                    "Feed aborted before initial values were read.",
                    ql::backtrace_id_t::empty());
                return;
            }
        }
        // TODO: Verify that snapshotting (below) in the case of a stamped query is
        // okay (by examing source code).  I think there was a mistake of implementation.

        sindex_disk_info_t sindex_info;
        uuid_u sindex_uuid;
        scoped_ptr_t<sindex_superblock_t> sindex_sb;
        try {
            sindex_sb =
                acquire_sindex_for_read(
                    store,
                    superblock,
                    release_superblock_t::RELEASE,
                    geo_read.table_name,
                    geo_read.sindex.id,
                &sindex_info, &sindex_uuid);
        } catch (const ql::exc_t &e) {
            res->result = e;
            return;
        }
        res->reql_version =
            sindex_info.mapping_version_info.latest_compatible_reql_version;

        if (sindex_info.geo != sindex_geo_bool_t::GEO) {
            res->result = ql::exc_t(
                ql::base_exc_t::LOGIC,
                strprintf(
                    "Index `%s` is not a geospatial index.  get_intersecting can only "
                    "be used with a geospatial index.",
                    geo_read.sindex.id.c_str()),
                ql::backtrace_id_t::empty());
            return;
        }

        sindex_sb->read_acq_signal()->wait_lazily_ordered();
        rockshard rocksh = store->rocksh();
        rockstore::snapshot snap = make_snapshot(rocksh.rocks);
        sindex_sb.reset();

        guarantee(geo_read.sindex.region);
        rdb_get_intersecting_slice(
            snap.snap,
            store->rocksh(),
            sindex_uuid,
            store->get_sindex_slice(sindex_uuid),
            geo_read.region, // This happens to always be the shard for geo reads.
            geo_read.query_geometry,
            geo_read.sindex.region->inner,
            &ql_env,
            geo_read.batchspec,
            geo_read.transforms,
            geo_read.terminal,
            geo_read.region.inner,
            sindex_info,
            geo_read.stamp ? is_stamp_read_t::YES : is_stamp_read_t::NO,
            res);
    }

    void operator()(const nearest_geo_read_t &geo_read) {
        ql::env_t ql_env(
            ctx,
            ql::return_empty_normal_batches_t::NO,
            interruptor,
            geo_read.serializable_env,
            trace);

        response->response = nearest_geo_read_response_t();
        nearest_geo_read_response_t *res =
            boost::get<nearest_geo_read_response_t>(&response->response);

        sindex_disk_info_t sindex_info;
        uuid_u sindex_uuid;
        scoped_ptr_t<sindex_superblock_t> sindex_sb;
        try {
            sindex_sb =
                acquire_sindex_for_read(
                    store,
                    superblock,
                    release_superblock_t::RELEASE,
                    geo_read.table_name,
                    geo_read.sindex_id,
                &sindex_info, &sindex_uuid);
        } catch (const ql::exc_t &e) {
            res->results_or_error = e;
            return;
        }

        if (sindex_info.geo != sindex_geo_bool_t::GEO) {
            res->results_or_error = ql::exc_t(
                ql::base_exc_t::LOGIC,
                strprintf(
                    "Index `%s` is not a geospatial index.  get_nearest can only be "
                    "used with a geospatial index.",
                    geo_read.sindex_id.c_str()),
                ql::backtrace_id_t::empty());
            return;
        }

        sindex_sb->read_acq_signal()->wait_lazily_ordered();
        rockshard rocksh = store->rocksh();
        rockstore::snapshot snap = make_snapshot(rocksh.rocks);
        sindex_sb.reset();

        rdb_get_nearest_slice(
            snap.snap,
            store->rocksh(),
            sindex_uuid,
            store->get_sindex_slice(sindex_uuid),
            geo_read.center,
            geo_read.max_dist,
            geo_read.max_results,
            geo_read.geo_system,
            &ql_env,
            geo_read.region.inner,
            sindex_info,
            res);
    }

    void operator()(const rget_read_t &rget) {
        response->response = rget_read_response_t();
        auto *res = boost::get<rget_read_response_t>(&response->response);

        if (rget.stamp) {
            res->stamp_response.set(changefeed_stamp_response_t());
            r_sanity_check(rget.current_shard);
            r_sanity_check(rget.sorting == sorting_t::UNORDERED);
            store_key_t read_left;
            if (rget.sindex) {
                // We're over-conservative with he `read_left` if we don't have an
                // sindex region yet (usually on the first read). This should be ok
                // for our current requirements and simplifies the code.
                read_left = rget.sindex->region
                    ? rget.sindex->region->inner.left
                    : store_key_t::min();
            } else {
                read_left = rget.region.inner.left;
            }
            changefeed_stamp_response_t r = do_stamp(
                *rget.stamp,
                *rget.current_shard,
                read_left);
            if (r.stamp_infos) {
                res->stamp_response.set(r);
            } else {
                res->result = ql::exc_t(
                    ql::base_exc_t::OP_FAILED,
                    "Feed aborted before initial values were read.",
                    ql::backtrace_id_t::empty());
                return;
            }

            // We didn't snapshot the dag when we first acquired the superblock because
            // we first needed to get the changefeed stamps (see
            // `use_snapshot_visitor_t`).
            // However now it's safe to use a snapshot for the rest of the read.

            // (Non-changefeed reads use snapshotting too, and they don't have
            // this changefeed stamp business that needs to happen before
            // subsequent writes.)
        }

        if (rget.transforms.size() != 0 || rget.terminal) {
            // This asserts that the optargs have been initialized.  (There is always
            // a 'db' optarg.)  We have the same assertion in
            // rdb_r_unshard_visitor_t.
            rassert(rget.serializable_env.global_optargs.has_optarg("db"));
        }
        ql::env_t ql_env(
            ctx,
            ql::return_empty_normal_batches_t::NO,
            interruptor,
            rget.serializable_env,
            trace);
        do_snap_read(store->rocksh(), &ql_env, store, btree, superblock, rget, res,
                     nullptr);
    }

    void operator()(const distribution_read_t &dg) {
        superblock->read_acq_signal()->wait_lazily_ordered();
        superblock->release();
        response->response = distribution_read_response_t();
        distribution_read_response_t *res = boost::get<distribution_read_response_t>(&response->response);
        // TODO: Replace max_depth option of distribution_read_t (when we break
        // the clustering protocol).  And make result_limit forced nonzero.
        // TODO: Reuse the scale_down_distribution function in rdb_distribution_get code.
        int keys_limit = dg.result_limit > 0 ? dg.result_limit : 1 << (4 * dg.max_depth);
        rdb_distribution_get(store->rocksh(), keys_limit, dg.region.inner, res);
        // TODO: This filtering by region is now unnecessary.
        for (std::map<store_key_t, int64_t>::iterator it = res->key_counts.begin(); it != res->key_counts.end(); ) {
            if (!dg.region.inner.contains_key(store_key_t(it->first))) {
                std::map<store_key_t, int64_t>::iterator tmp = it;
                ++it;
                res->key_counts.erase(tmp);
            } else {
                ++it;
            }
        }

        // If the result is larger than the requested limit, scale it down
        if (dg.result_limit > 0 && res->key_counts.size() > dg.result_limit) {
            scale_down_distribution(dg.result_limit, &res->key_counts);
        }

        res->region = dg.region;
    }

    void operator()(const dummy_read_t &) {
        response->response = dummy_read_response_t();
    }

    rdb_read_visitor_t(btree_slice_t *_btree,
                       store_t *_store,
                       real_superblock_t *_superblock,
                       rdb_context_t *_ctx,
                       read_response_t *_response,
                       profile::trace_t *_trace,
                       signal_t *_interruptor) :
        response(_response),
        ctx(_ctx),
        interruptor(_interruptor),
        btree(_btree),
        store(_store),
        superblock(_superblock),
        trace(_trace) { }

private:

    read_response_t *const response;
    rdb_context_t *const ctx;
    signal_t *const interruptor;
    btree_slice_t *const btree;
    store_t *const store;
    real_superblock_t *const superblock;
    profile::trace_t *const trace;

    DISABLE_COPYING(rdb_read_visitor_t);
};

void store_t::protocol_read(const read_t &_read,
                            read_response_t *response,
                            real_superblock_t *superblock,
                            signal_t *interruptor) {
    scoped_ptr_t<profile::trace_t> trace = ql::maybe_make_profile_trace(_read.profile);

    {
        PROFILE_STARTER_IF_ENABLED(
            _read.profile == profile_bool_t::PROFILE, "Perform read on shard.", trace);
        rdb_read_visitor_t v(btree.get(), this,
                             superblock,
                             ctx, response, trace.get_or_null(), interruptor);
        boost::apply_visitor(v, _read.read);
    }

    response->n_shards = 1;
    if (trace.has()) {
        response->event_log = std::move(*trace).extract_event_log();
    }
    // This is a tad hacky, this just adds a stop event to signal the end of the
    // parallel task.

    // TODO: Is this is the right thing to do if profiling's not enabled?
    response->event_log.push_back(profile::stop_t());
}


class func_replacer_t : public btree_batched_replacer_t {
public:
    func_replacer_t(ql::env_t *_env,
                    std::string _pkey,
                    const ql::wire_func_t &wf,
                    counted_t<const ql::func_t> wh,
                    return_changes_t _return_changes)
        : env(_env),
          pkey(std::move(_pkey)),
          f(wf.compile_wire_func()),
          write_hook(std::move(wh)),
          return_changes(_return_changes) { }
    ql::datum_t replace(
        const ql::datum_t &d, size_t) const {
        ql::datum_t res = f->call(env, d, ql::LITERAL_OK)->as_datum();

        const ql::datum_t &write_timestamp = env->get_deterministic_time();
        r_sanity_check(write_timestamp.has());
        return apply_write_hook(pkey, d, res, write_timestamp, write_hook);
    }
    return_changes_t should_return_changes() const { return return_changes; }
private:
    ql::env_t *const env;
    datum_string_t pkey;
    const counted_t<const ql::func_t> f;
    const counted_t<const ql::func_t> write_hook;
    const return_changes_t return_changes;
};

class datum_replacer_t : public btree_batched_replacer_t {
public:
    explicit datum_replacer_t(ql::env_t *_env,
                              const batched_insert_t &bi)
        : env(_env),
          datums(&bi.inserts),
          conflict_behavior(bi.conflict_behavior),
          pkey(bi.pkey),
          return_changes(bi.return_changes) {
        if (bi.conflict_func.has_value()) {
            conflict_func.set(bi.conflict_func->compile_wire_func());
        }
        if (bi.write_hook.has_value()) {
            write_hook = bi.write_hook->compile_wire_func();
        }
    }
    ql::datum_t replace(const ql::datum_t &d,
                        size_t index) const {
        guarantee(index < datums->size());
        ql::datum_t newd = (*datums)[index];
        ql::datum_t res = resolve_insert_conflict(env,
                                             pkey,
                                             d,
                                             newd,
                                             conflict_behavior,
                                             conflict_func);
        const ql::datum_t &write_timestamp = env->get_deterministic_time();
        r_sanity_check(write_timestamp.has());
        res = apply_write_hook(datum_string_t(pkey), d, res, write_timestamp,
                               write_hook);
        return res;
    }
    return_changes_t should_return_changes() const { return return_changes; }
private:
    ql::env_t *env;

    counted_t<const ql::func_t> write_hook;

    const std::vector<ql::datum_t> *const datums;
    const conflict_behavior_t conflict_behavior;
    const std::string pkey;
    const return_changes_t return_changes;
    optional<counted_t<const ql::func_t> > conflict_func;
};

struct rdb_write_visitor_t : public boost::static_visitor<void> {
    // TODO: Do reads from rockstore when performing these writes.
    void operator()(const batched_replace_t &br) {
        ql::env_t ql_env(
            ctx,
            ql::return_empty_normal_batches_t::NO,
            interruptor,
            br.serializable_env,
            trace);
        rdb_modification_report_cb_t sindex_cb(
            store, &sindex_block,
            auto_drainer_t::lock_t(&store->drainer));

        counted_t<const ql::func_t> write_hook;
        if (br.write_hook.has_value()) {
            write_hook = br.write_hook->compile_wire_func();
        }

        func_replacer_t replacer(&ql_env,
                                 br.pkey,
                                 br.f,
                                 write_hook,
                                 br.return_changes);

        response->response =
            rdb_batched_replace(
                store->rocksh(),
                btree_info_t(btree, timestamp, datum_string_t(br.pkey)),
                std::move(superblock),
                br.keys,
                &replacer,
                &sindex_cb,
                ql_env.limits(),
                sampler,
                trace);
    }

    void operator()(const batched_insert_t &bi) {
        rdb_modification_report_cb_t sindex_cb(
            store, &sindex_block,
            auto_drainer_t::lock_t(&store->drainer));
        ql::env_t ql_env(
            ctx,
            ql::return_empty_normal_batches_t::NO,
            interruptor,
            bi.serializable_env,
            trace);
        datum_replacer_t replacer(&ql_env,
                                  bi);
        std::vector<store_key_t> keys;
        keys.reserve(bi.inserts.size());
        for (auto it = bi.inserts.begin(); it != bi.inserts.end(); ++it) {
            keys.emplace_back(it->get_field(datum_string_t(bi.pkey)).print_primary());
        }
        response->response =
            rdb_batched_replace(
                store->rocksh(),
                btree_info_t(btree, timestamp, datum_string_t(bi.pkey)),
                std::move(superblock),
                keys,
                &replacer,
                &sindex_cb,
                bi.limits,
                sampler,
                trace);
    }

    void operator()(const point_write_t &w) {
        sampler->new_sample();
        response->response = point_write_response_t();
        point_write_response_t *res =
            boost::get<point_write_response_t>(&response->response);

        backfill_debug_key(w.key, strprintf("upsert %" PRIu64, timestamp.longtime));

        rdb_modification_report_t mod_report(w.key);
        rdb_set(store->rocksh(),
                w.key, w.data, w.overwrite, btree, timestamp, superblock.get(),
                res, &mod_report.info, trace, real_superblock_t::no_passback);

        update_sindexes(mod_report);
    }

    void operator()(const point_delete_t &d) {
        sampler->new_sample();
        response->response = point_delete_response_t();
        point_delete_response_t *res =
            boost::get<point_delete_response_t>(&response->response);

        backfill_debug_key(d.key, strprintf("delete %" PRIu64, timestamp.longtime));

        rdb_modification_report_t mod_report(d.key);
        rdb_delete(store->rocksh(),
                d.key, btree, timestamp, superblock.get(),
                delete_mode_t::REGULAR_QUERY, res, &mod_report.info, trace, real_superblock_t::no_passback);

        update_sindexes(mod_report);
    }

    void operator()(const sync_t &) {
        sampler->new_sample();
        response->response = sync_response_t();

        // TODO: Can't sync individual tables anymore (can we?).
        store->rocks->sync(rockstore::write_options::TODO());

        // We know this sync_t operation will force all preceding write transactions
        // (on our cache_conn_t) to flush before or at the same time, because the
        // cache guarantees that.  (Right now it will force _all_ preceding write
        // transactions to flush, on any conn, because they all touch the metainfo in
        // the superblock.)
    }

    void operator()(const dummy_write_t &) {
        response->response = dummy_write_response_t();
    }

    rdb_write_visitor_t(btree_slice_t *_btree,
                        store_t *_store,
                        txn_t *_txn,
                        scoped_ptr_t<real_superblock_t> &&_superblock,
                        repli_timestamp_t _timestamp,
                        rdb_context_t *_ctx,
                        profile::sampler_t *_sampler,
                        profile::trace_t *_trace,
                        write_response_t *_response,
                        signal_t *_interruptor) :
        btree(_btree),
        store(_store),
        txn(_txn),
        response(_response),
        ctx(_ctx),
        interruptor(_interruptor),
        superblock(std::move(_superblock)),
        timestamp(_timestamp),
        sampler(_sampler),
        trace(_trace),
        sindex_block(superblock->get(),
                     access_t::write) {
    }

private:
    void update_sindexes(const rdb_modification_report_t &mod_report) {
        std::vector<rdb_modification_report_t> mod_reports;
        // This copying of the mod_report is inefficient, but it seems this
        // function is only used for unit tests at the moment anyway.
        mod_reports.push_back(mod_report);
        store->update_sindexes(txn, &sindex_block, mod_reports,
                               true /* release_sindex_block */);
    }

    btree_slice_t *const btree;
    store_t *const store;
    txn_t *const txn;
    write_response_t *const response;
    rdb_context_t *const ctx;
    signal_t *const interruptor;
    scoped_ptr_t<real_superblock_t> superblock;
    const repli_timestamp_t timestamp;
    profile::sampler_t *const sampler;
    profile::trace_t *const trace;
    sindex_block_lock sindex_block;
    profile::event_log_t event_log_out;

    DISABLE_COPYING(rdb_write_visitor_t);
};

void store_t::protocol_write(const write_t &_write,
                             write_response_t *response,
                             state_timestamp_t timestamp,
                             scoped_ptr_t<real_superblock_t> superblock,
                             signal_t *interruptor) {
    scoped_ptr_t<profile::trace_t> trace = ql::maybe_make_profile_trace(_write.profile);

    {
        profile::sampler_t start_write("Perform write on shard.", trace);
        txn_t *txn = superblock->get()->txn();
        rdb_write_visitor_t v(btree.get(),
                              this,
                              txn,
                              std::move(superblock),
                              timestamp.to_repli_timestamp(),
                              ctx,
                              &start_write,
                              trace.get_or_null(),
                              response,
                              interruptor);
        boost::apply_visitor(v, _write.write);
    }

    response->n_shards = 1;
    if (trace.has()) {
        response->event_log = std::move(*trace).extract_event_log();
    }
    // This is a tad hacky, this just adds a stop event to signal the end of the
    // parallel task.

    // TODO: Is this the right thing to do if profiling's not enabled?
    response->event_log.push_back(profile::stop_t());
}

void store_t::delayed_clear_and_drop_sindex(
        secondary_index_t sindex,
        auto_drainer_t::lock_t store_keepalive)
        THROWS_NOTHING {
    try {
        /* Clear the sindex */
        clear_sindex_data(sindex.id,
                          key_range_t::universe(),
                          store_keepalive.get_drain_signal());

        /* Drop the sindex, now that it's empty. */
        drop_sindex(sindex.id);
    } catch (const interrupted_exc_t &e) {
        /* Ignore. The sindex deletion will continue when the store
        is next started up. */
    }
}

namespace_id_t const &store_t::get_table_id() const {
    return table_id;
}

store_t::sindex_context_map_t *store_t::get_sindex_context_map() {
    return &sindex_context;
}

std::pair<ql::changefeed::server_t *, auto_drainer_t::lock_t> store_t::changefeed_server(
        const region_t &_region,
        const rwlock_acq_t *acq) {
    acq->guarantee_is_holding(&changefeed_servers_lock);
    for (auto &&pair : changefeed_servers) {
        if (pair.first.inner.is_superset(_region.inner)) {
            return std::make_pair(pair.second.get(), pair.second->get_keepalive());
        }
    }
    return std::pair<ql::changefeed::server_t *, auto_drainer_t::lock_t>(
        nullptr, auto_drainer_t::lock_t());
}

std::pair<const std::map<region_t, scoped_ptr_t<ql::changefeed::server_t> > *,
          scoped_ptr_t<rwlock_acq_t> > store_t::access_changefeed_servers() {
    return std::make_pair(&changefeed_servers,
                          make_scoped<rwlock_acq_t>(&changefeed_servers_lock,
                                                    access_t::read));
}

std::pair<ql::changefeed::server_t *, auto_drainer_t::lock_t> store_t::changefeed_server(
        const region_t &_region) {
    rwlock_acq_t acq(&changefeed_servers_lock, access_t::read);
    return changefeed_server(_region, &acq);
}

std::pair<ql::changefeed::server_t *, auto_drainer_t::lock_t> store_t::changefeed_server(
        const store_key_t &key) {
    rwlock_acq_t acq(&changefeed_servers_lock, access_t::read);
    for (auto &&pair : changefeed_servers) {
        if (pair.first.inner.contains_key(key)) {
            return std::make_pair(pair.second.get(), pair.second->get_keepalive());
        }
    }
    return std::pair<ql::changefeed::server_t *, auto_drainer_t::lock_t>(
        nullptr, auto_drainer_t::lock_t());
}

std::pair<ql::changefeed::server_t *, auto_drainer_t::lock_t>
        store_t::get_or_make_changefeed_server(const region_t &_region) {
    rwlock_acq_t acq(&changefeed_servers_lock, access_t::write);
    // We assume that changefeeds use MAX_KEY instead of `unbounded` right bounds.
    // If this ever changes, `note_reshard` will need to be updated.
    guarantee(!_region.inner.right.unbounded);
    guarantee(ctx != nullptr);
    guarantee(ctx->manager != nullptr);
    auto existing = changefeed_server(_region, &acq);
    if (existing.first != nullptr) {
        return existing;
    }
    for (auto &&pair : changefeed_servers) {
        guarantee(!pair.first.inner.overlaps(_region.inner));
    }
    auto it = changefeed_servers.insert(
        std::make_pair(
            region_t(_region),
            make_scoped<ql::changefeed::server_t>(ctx->manager, this))).first;
    return std::make_pair(it->second.get(), it->second->get_keepalive());
}
