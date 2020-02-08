#include "fdb/xtore.hpp"

#include "errors.hpp"
#include <boost/variant/static_visitor.hpp>

#include "fdb/typed.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/protocol.hpp"

struct fdb_read_visitor : public boost::static_visitor<void> {
#if 0
    void operator()(const changefeed_subscribe_t &s) {
        auto cserver = store->get_or_make_changefeed_server();
        guarantee(cserver.first != nullptr);
        cserver.first->add_client(s.addr, cserver.second);
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
            rget.table_name = s.table;
            rget.batchspec = ql::batchspec_t::all(); // Terminal takes care of stopping.
            if (s.spec.range.sindex) {
                rget.terminal.set(ql::limit_read_t{
                    is_primary_t::NO,
                    s.spec.limit,
                    s.region,
                    !reversed(s.spec.range.sorting)
                        ? ql::limit_read_last_key::min()
                        : ql::limit_read_last_key::infinity(),
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
                        ? ql::limit_read_last_key::min()
                        : ql::limit_read_last_key::infinity(),
                    s.spec.range.sorting,
                    &ops});
            }
            rget.sorting = s.spec.range.sorting;

            // The superblock will instead be released in `store_t::read`
            // shortly after this function returns.
            rget_read_response_t resp;
            do_read_for_changefeed(store->rocksh(), &env, store, btree, superblock.get(), rget, &resp,
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

        auto cserver = store->get_or_make_changefeed_server();
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
        superblock->read_acq_signal()->wait_lazily_ordered();

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
        response->response = do_stamp(s, s.region, s.region.left);
    }

    void operator()(const changefeed_point_stamp_t &s) {
        // Need to wait for the superblock to make sure we get the right changefeed
        // stamp.
        superblock->read_acq_signal()->wait_lazily_ordered();

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
            rdb_get(store->rocksh(), s.key, superblock.get(), &val);
            vres->initial_val = val.data;
        } else {
            res->resp.reset();
        }
    }

    void operator()(const point_read_t &get) {
        response->response = point_read_response_t();
        point_read_response_t *res =
            boost::get<point_read_response_t>(&response->response);
        rdb_get(store->rocksh(), get.key, superblock.get(), res);
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
                ? geo_read.sindex.region->left
                : store_key_t::min();

            changefeed_stamp_response_t r = do_stamp(
                *geo_read.stamp,
                region_t::universe(),
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
        try {
            acquire_sindex_for_read(
                    store,
                    superblock.get(),
                    geo_read.table_name,
                    geo_read.sindex.id,
                    &sindex_info, &sindex_uuid);
        } catch (const ql::exc_t &e) {
            res->result = e;
            return;
        }

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

        superblock->read_acq_signal()->wait_lazily_ordered();
        rockshard rocksh = store->rocksh();
        rockstore::snapshot snap = make_snapshot(rocksh.rocks);
        superblock.reset();

        guarantee(geo_read.sindex.region);
        rdb_get_intersecting_slice(
            snap.snap,
            store->rocksh(),
            sindex_uuid,
            store->get_sindex_slice(sindex_uuid),
            geo_read.query_geometry,
            *geo_read.sindex.region,
            &ql_env,
            geo_read.batchspec,
            geo_read.transforms,
            geo_read.terminal,
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
        try {
            acquire_sindex_for_read(
                    store,
                    superblock.get(),
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

        superblock->read_acq_signal()->wait_lazily_ordered();
        rockshard rocksh = store->rocksh();
        rockstore::snapshot snap = make_snapshot(rocksh.rocks);
        superblock.reset();

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
            sindex_info,
            res);
    }

    void operator()(const rget_read_t &rget) {
        response->response = rget_read_response_t();
        auto *res = boost::get<rget_read_response_t>(&response->response);

        if (rget.stamp) {
            res->stamp_response.set(changefeed_stamp_response_t());
            r_sanity_check(rget.sorting == sorting_t::UNORDERED);
            store_key_t read_left;
            if (rget.sindex) {
                // We're over-conservative with he `read_left` if we don't have an
                // sindex region yet (usually on the first read). This should be ok
                // for our current requirements and simplifies the code.
                read_left = rget.sindex->region
                    ? rget.sindex->region->left
                    : store_key_t::min();
            } else {
                read_left = rget.region.left;
            }
            changefeed_stamp_response_t r = do_stamp(
                *rget.stamp,
                region_t::universe(),  // TODO: current_shard
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
        do_snap_read(store->rocksh(), &ql_env, store, btree, std::move(superblock), rget, res);
    }

    void operator()(const distribution_read_t &dg) {
        superblock->read_acq_signal()->wait_lazily_ordered();
        // We reset the buf lock early, because rocksdb doesn't offer consistent
        // access to key range statistics? Or at least we don't really need it.
        superblock.reset();
        response->response = distribution_read_response_t();
        distribution_read_response_t *res = boost::get<distribution_read_response_t>(&response->response);
        // TODO: Replace max_depth option of distribution_read_t (when we break
        // the clustering protocol).  And make result_limit forced nonzero.
        // TODO: Reuse the scale_down_distribution function in rdb_distribution_get code.
        int keys_limit = dg.result_limit > 0 ? dg.result_limit : 1 << (4 * dg.max_depth);
        rdb_distribution_get(store->rocksh(), keys_limit, dg.region, res);
        // TODO: This filtering by region is now unnecessary.
        for (std::map<store_key_t, int64_t>::iterator it = res->key_counts.begin(); it != res->key_counts.end(); ) {
            if (!dg.region.contains_key(store_key_t(it->first))) {
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

#endif  // 0

    void operator()(const dummy_read_t &) {
        response->response = dummy_read_response_t();
    }

    // OOO: Remove this.
    template <class T>
    void operator()(const T&) {
        crash("Unimplemented read op for fdb");
    }

    fdb_read_visitor(FDBTransaction *_txn,
            reqlfdb_config_version _expected_cv,
            const namespace_id_t &_table_id,
            const table_config_t *_table_config,
            profile::trace_t *_trace_or_null,
            read_response_t *_response,
            const signal_t *_interruptor) :
        txn_(_txn),
        expected_cv_(_expected_cv),
        cv_fut_(transaction_get_config_version(_txn)),
        table_id_(_table_id),
        table_config_(_table_config),
        trace(_trace_or_null),
        response(_response),
        interruptor(_interruptor) {}

private:
    FDBTransaction *const txn_;
    const reqlfdb_config_version expected_cv_;
    fdb_value_fut<reqlfdb_config_version> cv_fut_;
    const namespace_id_t table_id_;
    const table_config_t *table_config_;
    // TODO: Rename trace to trace_or_null?
    profile::trace_t *const trace;
    read_response_t *const response;
    const signal_t *const interruptor;

    DISABLE_COPYING(fdb_read_visitor);
};

read_response_t apply_read(FDBTransaction *txn,
        reqlfdb_config_version expected_cv,
        const namespace_id_t &table_id,
        const table_config_t &table_config,
        const read_t &_read,
        const signal_t *interruptor) {
    scoped_ptr_t<profile::trace_t> trace = ql::maybe_make_profile_trace(_read.profile);
    read_response_t response;
    {
        PROFILE_STARTER_IF_ENABLED(
            _read.profile == profile_bool_t::PROFILE, "Perform read on shard.", trace);
        fdb_read_visitor v(txn, expected_cv, table_id, &table_config, trace.get_or_null(),
            &response, interruptor);
        boost::apply_visitor(v, _read.read);
    }

    // TODO: Remove n_shards.
    response.n_shards = 1;
    if (trace.has()) {
        response.event_log = std::move(*trace).extract_event_log();
    }

    // (Taken from store_t::protocol_read.)
    // This is a tad hacky, this just adds a stop event to signal the end of the
    // parallel task.

    // TODO: Is this is the right thing to do if profiling's not enabled?
    response.event_log.push_back(profile::stop_t());
    return response;
}
