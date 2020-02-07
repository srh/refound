#include "fdb/xtore.hpp"

#include "fdb/typed.hpp"
#include "rdb_protocol/env.hpp"  // TODO: Remove include
#include "rdb_protocol/reqlfdb_config_cache.hpp"
#include "rdb_protocol/protocol.hpp"



struct fdb_write_visitor : public boost::static_visitor<void> {
// OOO: Update these functions.
#if 0
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

#endif  // 0
    void operator()(const sync_t &) {
        // TODO: Understand what this does.
        sampler->new_sample();

        // We have to check cv, for the usual reasons: to make sure table name->id
        // mapping we used was legit.
        reqlfdb_config_version cv = cv_fut_.block_and_deserialize(interruptor);
        check_cv(expected_cv_, cv);
        response->response = sync_response_t();
    }

    void operator()(const dummy_write_t &) {
        // We have to check cv, for the usual reasons: to make sure table name->id
        // mapping we used was legit.
        reqlfdb_config_version cv = cv_fut_.block_and_deserialize(interruptor);
        check_cv(expected_cv_, cv);
        response->response = dummy_write_response_t();
    }

    // OOO: Remove this!
    template <class T>
    void operator()(const T&) { }

    // One responsibility all visitor methods have is to check expected_cv.  Preferably
    // before they try anything long and expensive.

    fdb_write_visitor(FDBTransaction *_txn,
            reqlfdb_config_version _expected_cv,
            const namespace_id_t &_table_id,
            profile::sampler_t *_sampler,
            profile::trace_t *_trace_or_null,
            write_response_t *_response,
            const signal_t *_interruptor) :
        txn_(_txn),
        expected_cv_(_expected_cv),
        cv_fut_(transaction_get_config_version(_txn)),
        table_id_(_table_id),
        sampler(_sampler),
        trace(_trace_or_null),
        response(_response),
        interruptor(_interruptor) {}

private:
// OOO: Remove/uncomment this.
#if 0
    void update_sindexes(const rdb_modification_report_t &mod_report) {
        std::vector<rdb_modification_report_t> mod_reports;
        // This copying of the mod_report is inefficient, but it seems this
        // function is only used for unit tests at the moment anyway.
        mod_reports.push_back(mod_report);
        store->update_sindexes(txn.get(), std::move(superblock), mod_reports);
    }
#endif  // 0

    FDBTransaction *const txn_;
    const reqlfdb_config_version expected_cv_;
    fdb_value_fut<reqlfdb_config_version> cv_fut_;
    const namespace_id_t table_id_;
    profile::sampler_t *const sampler;
    // TODO: Rename trace to trace_or_null?
    profile::trace_t *const trace;
    write_response_t *const response;
    const signal_t *const interruptor;

    DISABLE_COPYING(fdb_write_visitor);
};


// QQQ: Think twice about passing in an FDBTransaction here.  We might want to break up
// batched writes into separate transactions per key.
write_response_t apply_write(FDBTransaction *txn,
        reqlfdb_config_version expected_cv,
        const namespace_id_t &table_id,
        const write_t &write,
        const signal_t *interruptor) {
    scoped_ptr_t<profile::trace_t> trace = ql::maybe_make_profile_trace(write.profile);
    write_response_t response;
    {
        profile::sampler_t start_write("Perform write on shard.", trace);  // TODO: Change message.
        // TODO: Pass &response.response, actually.
        fdb_write_visitor v(txn, expected_cv, table_id, &start_write, trace.get_or_null(), &response, interruptor);
        boost::apply_visitor(v, write.write);
    }

    response.n_shards = 1;
    if (trace.has()) {
        response.event_log = std::move(*trace).extract_event_log();
    }

    // (Taken from store_t::protocol_write.)
    // This is a tad hacky, this just adds a stop event to signal the end of the
    // parallel task.

    // TODO: Is this the right thing to do if profiling's not enabled?
    response.event_log.push_back(profile::stop_t());
    return response;
}
