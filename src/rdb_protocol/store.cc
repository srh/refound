// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "rdb_protocol/store.hpp"

#include "btree/reql_specific.hpp"
#include "btree/operations.hpp"
#include "concurrency/cross_thread_signal.hpp"
#include "concurrency/cross_thread_watchable.hpp"
#include "concurrency/wait_any.hpp"
#include "containers/archive/vector_stream.hpp"
#include "rdb_protocol/btree.hpp"
#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/shards.hpp"
#include "rdb_protocol/table_common.hpp"

#if RDB_CF
void store_t::note_reshard() {
    // This is no longer for resharding, it just shuts off changefeeds for
    // local_replicator_t.  Maybe this is unnecessary; maybe we could let the destructor
    // do its work.

    // We acquire `changefeed_servers_lock` and move the matching pointer out of
    // `changefeed_servers`. We then destruct the server in a separate step,
    // after releasing the lock.
    // The reason we do this is to avoid deadlocks that could happen if someone
    // was holding a lock on the drainer in one of the changefeed servers,
    // and was at the same time trying to acquire the `changefeed_servers_lock`.
    scoped_ptr_t<ql::changefeed::server_t> to_destruct;
    {
        rwlock_acq_t acq(&the_changefeed_server_lock, access_t::write);
        ASSERT_NO_CORO_WAITING;
        if (the_changefeed_server.has()) {
            to_destruct = std::move(the_changefeed_server);
            the_changefeed_server.reset();
        }
    }
    // The changefeed server is actually getting destructed here. This might
    // block.
}
#endif


void store_t::help_construct_bring_sindexes_up_to_date() {
    // Make sure to continue bringing sindexes up-to-date if it was interrupted earlier

    // This uses a dummy interruptor because this is the only thing using the store at
    //  the moment (since we are still in the constructor), so things should complete
    //  rather quickly.
    cond_t dummy_interruptor;
    write_token_t token;
    new_write_token(&token);

    scoped_ptr_t<txn_t> txn;
    scoped_ptr_t<real_superblock_lock> superblock;
    acquire_superblock_for_write(1,
                                 write_durability_t::SOFT,
                                 &token,
                                 &txn,
                                 &superblock,
                                 &dummy_interruptor);

    superblock->sindex_block_write_signal()->wait();

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
        get_secondary_indexes(rocksh(), superblock.get(), &sindexes);
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

    txn->commit(rocks, std::move(superblock));
}

#if RDB_CF
void acquire_sindex_for_read(
    store_t *store,
    real_superblock_lock *superblock,
    const std::string &table_name,
    const std::string &sindex_id,
    sindex_disk_info_t *sindex_info_out,
    uuid_u *sindex_uuid_out) {
    rassert(sindex_info_out != NULL);
    rassert(sindex_uuid_out != NULL);

    std::vector<char> sindex_mapping_data;

    sindex_disk_info_t sindex_info;
    uuid_u sindex_uuid;
    try {
        bool found = store->acquire_sindex_superblock_for_read(
            sindex_name_t(sindex_id),
            table_name,
            superblock,
            &sindex_info,
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

    *sindex_info_out = sindex_info;
    *sindex_uuid_out = sindex_uuid;
}

// TODO: Remove this?
void do_read_for_changefeed(rockshard rocksh,
             ql::env_t *env,
             store_t *store,
             btree_slice_t *btree,
             real_superblock_lock *superblock,
             const rget_read_t &rget,
             rget_read_response_t *res,
             optional<uuid_u> *sindex_id_out) {
    if (!rget.sindex.has_value()) {
        // rget using a primary index
        *sindex_id_out = r_nullopt;
        rdb_rget_slice(
            rocksh,
            btree,
            rget.region,
            rget.primary_keys,
            superblock,
            env,
            rget.batchspec,
            rget.transforms,
            rget.terminal,
            rget.sorting,
            res,
            release_superblock_t::KEEP);
    } else {
        // rget using a secondary index
        sindex_disk_info_t sindex_info;
        uuid_u sindex_uuid;
        key_range_t sindex_range;
        try {
            acquire_sindex_for_read(
                    store,
                    superblock,
                    rget.table_name,
                    rget.sindex->id,
                    &sindex_info,
                    &sindex_uuid);
            *sindex_id_out = make_optional(sindex_uuid);
            if (rget.sindex->region.has_value()) {
                sindex_range = *rget.sindex->region;
            } else {
                sindex_range =
                    rget.sindex->datumspec.covering_range().to_sindex_keyrange();
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
                rget.sindex->datumspec,
                sindex_range,
                superblock,
                env,
                rget.batchspec,
                rget.transforms,
                rget.terminal,
                rget.region,
                rget.sorting,
                rget.sindex->require_sindex_val,
                sindex_info,
                res,
                release_superblock_t::KEEP);
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
#endif  // RDB_CF

class func_replacer_t : public btree_batched_replacer_t {
public:
    func_replacer_t(ql::env_t *_env,
                    std::string _pkey,
                    const ql::deterministic_func &wf,
                    counted_t<const ql::func_t> wh,
                    return_changes_t _return_changes)
        : env(_env),
          pkey(std::move(_pkey)),
          f(wf.det_func.compile_wire_func()),
          write_hook(std::move(wh)),
          return_changes(_return_changes) { }
    ql::datum_t replace(
        const ql::datum_t &d, size_t) const {
        ql::datum_t res = f->call(env, d, ql::LITERAL_OK)->as_datum(env);

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
          return_changes(bi.return_changes),
          conflict_func(bi.conflict_func) {
        if (bi.write_hook.has_value()) {
            write_hook = bi.write_hook->det_func.compile_wire_func();
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
    optional<ql::deterministic_func> conflict_func;
};

struct rdb_write_visitor_t : public boost::static_visitor<void> {
    void operator()(const batched_replace_t &br) {
        try {
            ql::env_t ql_env(
                ctx,
                ql::return_empty_normal_batches_t::NO,
                interruptor,
                br.serializable_env,
                trace);
            rdb_modification_report_cb_t sindex_cb(
                store, superblock.get(),
                auto_drainer_t::lock_t(&store->drainer));

            counted_t<const ql::func_t> write_hook;
            if (br.write_hook.has_value()) {
                write_hook = br.write_hook->det_func.compile_wire_func();
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
                    superblock.get(),
                    br.keys,
                    &replacer,
                    &sindex_cb,
                    ql_env.limits(),
                    sampler,
                    trace);
        } catch (const interrupted_exc_t &exc) {
            txn->commit(store->rocksh().rocks, std::move(superblock));
            throw;
        }
        txn->commit(store->rocksh().rocks, std::move(superblock));
    }

    void operator()(const batched_insert_t &bi) {
        try {
            rdb_modification_report_cb_t sindex_cb(
                store, superblock.get(),
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
                    superblock.get(),
                    keys,
                    &replacer,
                    &sindex_cb,
                    bi.limits,
                    sampler,
                    trace);
        } catch (const interrupted_exc_t &exc) {
            txn->commit(store->rocksh().rocks, std::move(superblock));
            throw;
        }
        txn->commit(store->rocksh().rocks, std::move(superblock));
    }

    void operator()(const point_write_t &w) {
        sampler->new_sample();
        response->response = point_write_response_t();
        point_write_response_t *res =
            boost::get<point_write_response_t>(&response->response);

        // TODO: Previously we didn't pass back the superblock.
        rdb_modification_report_t mod_report(w.key);
        promise_t<real_superblock_lock *> pass_back_superblock;
        rdb_set(store->rocksh(),
                w.key, w.data, w.overwrite, btree, timestamp, superblock.get(),
                res, &mod_report.info, trace, &pass_back_superblock);
        pass_back_superblock.wait();
        update_sindexes(mod_report);
    }

    void operator()(const point_delete_t &d) {
        sampler->new_sample();
        response->response = point_delete_response_t();
        point_delete_response_t *res =
            boost::get<point_delete_response_t>(&response->response);

        // TODO: Previously we didn't pass back the superblock.
        rdb_modification_report_t mod_report(d.key);
        promise_t<real_superblock_lock *> pass_back_superblock;
        rdb_delete(store->rocksh(),
                d.key, btree, timestamp, superblock.get(),
                delete_mode_t::REGULAR_QUERY, res, &mod_report.info, trace,
                &pass_back_superblock);
        pass_back_superblock.wait();
        update_sindexes(mod_report);
    }

    void operator()(const sync_t &) {
        sampler->new_sample();
        response->response = sync_response_t();
        superblock->write_acq_signal()->wait();

        // TODO: Can't sync individual tables anymore (can we?).
        store->rocks->sync();

        // We know this sync_t operation will force all preceding write transactions
        // (on our cache_conn_t) to flush before or at the same time, because the
        // cache guarantees that.  (Right now it will force _all_ preceding write
        // transactions to flush, on any conn, because they all touch the metainfo in
        // the superblock.)
        // TODO: More like, txn->no-commit.
        txn->commit(store->rocksh().rocks, std::move(superblock));
    }

    void operator()(const dummy_write_t &) {
        response->response = dummy_write_response_t();
        // TODO: More like, txn->no-commit?
        txn->commit(store->rocksh().rocks, std::move(superblock));
    }

    rdb_write_visitor_t(scoped_ptr_t<txn_t> &&_txn,
                        btree_slice_t *_btree,
                        store_t *_store,
                        scoped_ptr_t<real_superblock_lock> &&_superblock,
                        repli_timestamp_t _timestamp,
                        rdb_context_t *_ctx,
                        profile::sampler_t *_sampler,
                        profile::trace_t *_trace,
                        write_response_t *_response,
                        const signal_t *_interruptor) :
        txn(std::move(_txn)),
        btree(_btree),
        store(_store),
        response(_response),
        ctx(_ctx),
        interruptor(_interruptor),
        superblock(std::move(_superblock)),
        timestamp(_timestamp),
        sampler(_sampler),
        trace(_trace) {
    }

private:
    void update_sindexes(const rdb_modification_report_t &mod_report) {
        std::vector<rdb_modification_report_t> mod_reports;
        // This copying of the mod_report is inefficient, but it seems this
        // function is only used for unit tests at the moment anyway.
        mod_reports.push_back(mod_report);
        store->update_sindexes(txn.get(), std::move(superblock), mod_reports);
    }

    scoped_ptr_t<txn_t> txn;
    btree_slice_t *const btree;
    store_t *const store;
    write_response_t *const response;
    rdb_context_t *const ctx;
    const signal_t *const interruptor;
    scoped_ptr_t<real_superblock_lock> superblock;
    const repli_timestamp_t timestamp;
    profile::sampler_t *const sampler;
    profile::trace_t *const trace;

    DISABLE_COPYING(rdb_write_visitor_t);
};

void store_t::protocol_write(scoped_ptr_t<txn_t> txn,
                             const write_t &_write,
                             write_response_t *response,
                             state_timestamp_t timestamp,
                             scoped_ptr_t<real_superblock_lock> superblock,
                             const signal_t *interruptor) {
    scoped_ptr_t<profile::trace_t> trace = ql::maybe_make_profile_trace(_write.profile);

    {
        profile::sampler_t start_write("Perform write on shard.", trace);
        rdb_write_visitor_t v(std::move(txn),
                            btree.get(),
                            this,
                            std::move(superblock),
                            timestamp.to_repli_timestamp(),
                            ctx,
                            &start_write,
                            trace.get_or_null(),
                            response,
                            interruptor);
        boost::apply_visitor(v, _write.write);
    }

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

// TODO: Cleanup this fluff.
#if RDB_CF
std::pair<ql::changefeed::server_t *, auto_drainer_t::lock_t> store_t::changefeed_server(
        const rwlock_acq_t *acq) {
    acq->guarantee_is_holding(&the_changefeed_server_lock);
    if (the_changefeed_server.has()) {
        return std::make_pair(the_changefeed_server.get(), the_changefeed_server->get_keepalive());
    }
    return std::pair<ql::changefeed::server_t *, auto_drainer_t::lock_t>(
        nullptr, auto_drainer_t::lock_t());
}

std::pair<const scoped_ptr_t<ql::changefeed::server_t> *,
          scoped_ptr_t<rwlock_acq_t> > store_t::access_the_changefeed_server() {
    return std::make_pair(&the_changefeed_server,
                          make_scoped<rwlock_acq_t>(&the_changefeed_server_lock,
                                                    access_t::read));
}

std::pair<ql::changefeed::server_t *, auto_drainer_t::lock_t> store_t::changefeed_server(
        const region_t &) {
    // TODO: What is region?  Region is unused.
    rwlock_acq_t acq(&the_changefeed_server_lock, access_t::read);
    return changefeed_server(&acq);
}

std::pair<ql::changefeed::server_t *, auto_drainer_t::lock_t> store_t::changefeed_server(
        const store_key_t &) {
    rwlock_acq_t acq(&the_changefeed_server_lock, access_t::read);
    if (the_changefeed_server.has()) {
        return std::make_pair(the_changefeed_server.get(), the_changefeed_server->get_keepalive());
    }
    return std::pair<ql::changefeed::server_t *, auto_drainer_t::lock_t>(
        nullptr, auto_drainer_t::lock_t());
}

std::pair<ql::changefeed::server_t *, auto_drainer_t::lock_t>
        store_t::get_or_make_changefeed_server() {
    rwlock_acq_t acq(&the_changefeed_server_lock, access_t::write);
    guarantee(ctx != nullptr);
    guarantee(ctx->manager != nullptr);
    auto existing = changefeed_server(&acq);
    if (existing.first != nullptr) {
        return existing;
    }
    the_changefeed_server =
            make_scoped<ql::changefeed::server_t>(ctx->manager, this);
    return std::make_pair(the_changefeed_server.get(), the_changefeed_server->get_keepalive());
}
#endif  // RDB_CF
