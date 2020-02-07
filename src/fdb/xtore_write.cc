#include "fdb/xtore.hpp"

#include <unordered_map>
#include <unordered_set>

#include "btree/operations.hpp"
#include "clustering/administration/tables/table_metadata.hpp"
#include "fdb/btree_utils.hpp"
#include "fdb/jobs/index_create.hpp"
#include "fdb/typed.hpp"
#include "rdb_protocol/btree.hpp"
#include "rdb_protocol/env.hpp"  // TODO: Remove include
#include "rdb_protocol/reqlfdb_config_cache.hpp"
#include "rdb_protocol/serialize_datum_onto_blob.hpp"
#include "rdb_protocol/protocol.hpp"

// OOO: Move to btree/keys.hpp
namespace std {
template<> struct hash<store_key_t> {
    size_t operator()(const store_key_t& x) const {
        return std::hash<std::string>()(x.str());
    }
};
}  // namespace std

struct jobstate_futs {
    // The strings are of course unique sindex names.
    std::vector<std::pair<std::string, fdb_value_fut<fdb_index_jobstate>>> futs_by_sindex;
};

jobstate_futs get_jobstates(
        FDBTransaction *txn, const table_config_t &table_config) {
    jobstate_futs ret;
    for (const auto &el : table_config.fdb_sindexes) {
        const sindex_metaconfig_t &second = el.second;
        if (!second.creation_task_or_nil.value.is_nil()) {
            ret.futs_by_sindex.emplace_back(el.first,
                transaction_lookup_uq_index<index_jobstate_by_task>(txn,
                    second.creation_task_or_nil));
        }
    }
    return ret;
}

// You must check_cv before calling this.
std::unordered_map<std::string, fdb_index_jobstate>
block_on_jobstates(jobstate_futs &&futs, const signal_t *interruptor) {
    std::unordered_map<std::string, fdb_index_jobstate> ret;
    for (auto &pair : futs.futs_by_sindex) {
        fdb_index_jobstate js = pair.second.block_and_deserialize(interruptor);
        ret.emplace(std::move(pair.first), std::move(js));
    }
    return ret;
}

void update_fdb_sindexes(
        FDBTransaction *txn,
        const namespace_id_t &table_id,
        const table_config_t &table_config,
        rdb_modification_report_t &&modification,
        jobstate_futs &&jobstate_futs,
        const signal_t *interruptor) {
    // The thing is, we know the sindex has to be in good shape.

    // TODO: We only need to block on jobstates whose sindexes have a mutation.
    std::unordered_map<std::string, fdb_index_jobstate> jobstates
        = block_on_jobstates(std::move(jobstate_futs), interruptor);

    for (const auto &pair : table_config.sindexes) {
        const sindex_config_t &sindex_config = pair.second;
        const auto fdb_sindexes_it = table_config.fdb_sindexes.find(pair.first);
        guarantee(fdb_sindexes_it != table_config.fdb_sindexes.end());
        const sindex_metaconfig_t &fdb_sindex_config = fdb_sindexes_it->second;

        auto jobstates_it = jobstates.find(pair.first);
        if (jobstates_it != jobstates.end()) {
            const fdb_index_jobstate &js = jobstates_it->second;
            if (js.unindexed_lower_bound.ukey <= modification.primary_key.str() &&
                    js.unindexed_upper_bound.ukey > modification.primary_key.str()) {
                continue;
            }
        }


        // TODO: Making this copy is gross -- would be better if compute_keys took sindex_config.
        sindex_disk_info_t sindex_info{
            sindex_config.func,
            sindex_reql_version_info_t{sindex_config.func_version,sindex_config.func_version,sindex_config.func_version},  // TODO: Verify we just dumbly use latest_compatible_reql_version.
            sindex_config.multi,
            sindex_config.geo};

        std::unordered_set<store_key_t> deletion_keys;

        if (modification.info.deleted.first.has()) {
            try {
                // TODO: datum copy performance?
                ql::datum_t deleted = modification.info.deleted.first;

                // TODO: The ql::datum_t value is unused.  Remove it once FDB-ized fully.
                std::vector<std::pair<store_key_t, ql::datum_t> > keys;
                compute_keys(
                    modification.primary_key, deleted, sindex_info,
                    &keys, nullptr);
                for (auto &p : keys) {
                    deletion_keys.emplace(std::move(p.first));
                }
            } catch (const ql::base_exc_t &) {
                // Do nothing (it wasn't actually in the index).
            }
        }

        std::unordered_set<store_key_t> addition_keys;

        if (modification.info.added.first.has()) {
            try {
                // TODO: datum copy performance?
                ql::datum_t added = modification.info.added.first;

                std::vector<std::pair<store_key_t, ql::datum_t> > keys;
                compute_keys(
                    modification.primary_key, added, sindex_info,
                    &keys, nullptr);
                for (auto &p : keys) {
                    addition_keys.emplace(std::move(p.first));
                }
            } catch (const ql::base_exc_t &) {
                // Do nothing (we just drop the row from the index).
            }
        }

        std::string fdb_key = table_index_prefix(table_id,
            fdb_sindex_config.sindex_id);
        const size_t index_prefix_size = fdb_key.size();

        for (const store_key_t &key : deletion_keys) {
            auto add_it = addition_keys.find(key);
            if (add_it == addition_keys.end()) {
                // TODO: Make sure fdb key limits are followed.
                rdbtable_sindex_fdb_key_onto(&fdb_key, key);
                transaction_clear_std_str(txn, fdb_key);
                fdb_key.resize(index_prefix_size);
            } else {
                addition_keys.erase(add_it);
            }
        }

        for (const store_key_t &key : addition_keys) {
            // OOO: Dedup sindex writing/deletion logic.
            rdbtable_sindex_fdb_key_onto(&fdb_key, key);
            uint8_t value[1];
            fdb_transaction_set(txn,
                as_uint8(fdb_key.data()), int(fdb_key.size()),
                value, 0);
            fdb_key.resize(index_prefix_size);
        }
    }  // for each sindex
}

// TODO: Large value impl will rewrite kv_location_set and kv_location_get (and any
// other kv_location funcs).

// The signature will need to change with large values because we'll need to wipe out the old value (if it's larger and uses more keys).
MUST_USE ql::serialization_result_t
kv_location_set(
        FDBTransaction *txn, const std::string &kv_location,
        const ql::datum_t &data) {
    std::string str;
    ql::serialization_result_t res = datum_serialize_to_string(data, &str);
    if (bad(res)) {
        return res;
    }

    transaction_set_std_str(txn, kv_location, str);
    return res;
}

struct fdb_datum_fut : public fdb_future {
    explicit fdb_datum_fut(fdb_future &&ff) : fdb_future{std::move(ff)} {}
};

fdb_datum_fut kv_location_get(FDBTransaction *txn, const std::string &kv_location) {
    return fdb_datum_fut{transaction_get_std_str(txn, kv_location)};
}

void rdb_fdb_set(
        FDBTransaction *txn,
        const namespace_id_t &table_id,
        const store_key_t &key,
        ql::datum_t data,
        /* Right now, via point_write_t::overwrite this is always true. */
        // TODO: Remove this param.
        bool overwrite,
        point_write_response_t *response_out,
        rdb_modification_info_t *mod_info,
        const signal_t *interruptor) {
    // TODO: Create these perfmons.
    // slice->stats.pm_keys_set.record();
    // slice->stats.pm_total_keys_set += 1;
    std::string kv_location = rfdb::table_primary_key(table_id, key);
    fdb_datum_fut old_value_fut = kv_location_get(txn, kv_location);

    fdb_value old_value = future_block_on_value(old_value_fut.fut, interruptor);

    if (old_value.present) {
        mod_info->deleted.first = datum_deserialize_from_uint8(old_value.data, size_t(old_value.length));
    }

    // TODO: Review datum_t copy performance.
    mod_info->added.first = data;

    if (overwrite || !old_value.present) {
        ql::serialization_result_t res = kv_location_set(txn, kv_location, data);
        if (res & ql::serialization_result_t::ARRAY_TOO_BIG) {
            rfail_typed_target(&data, "Array too large for disk writes "
                               "(limit 100,000 elements).");
        } else if (res & ql::serialization_result_t::EXTREMA_PRESENT) {
            rfail_typed_target(&data, "`r.minval` and `r.maxval` cannot be "
                               "written to disk.");
        }
        r_sanity_check(!ql::bad(res));  // TODO: Compile time assertion.
    }

    response_out->result =
        (old_value.present ? point_write_result_t::DUPLICATE : point_write_result_t::STORED);
}


void rdb_fdb_delete(
        FDBTransaction *txn,
        const namespace_id_t &table_id,
        const store_key_t &key,
        point_delete_response_t *response,
        rdb_modification_info_t *mod_info,
        const signal_t *interruptor) {
    // OOO: Create these perfmons.
    // slice->stats.pm_keys_set.record();
    // slice->stats.pm_total_keys_set += 1;

    // NNN: Make table pkey/skey wrappers for the purpose of dealing with large values.

    std::string kv_location = rfdb::table_primary_key(table_id, key);
    fdb_future old_value_fut = transaction_get_std_str(txn, kv_location);

    // NNN: Read any sindex jobstates concurrently with the old value.  And check the
    // config version _before_ we do a second round of sindex old-value reading.

    fdb_value old_value = future_block_on_value(old_value_fut.fut, interruptor);

    /* Update the modification report. */
    if (old_value.present) {
        mod_info->deleted.first = datum_deserialize_from_uint8(old_value.data, size_t(old_value.length));
        transaction_clear_std_str(txn, kv_location);
    }

    response->result = (old_value.present ? point_delete_result_t::DELETED : point_delete_result_t::MISSING);
}

struct fdb_write_visitor : public boost::static_visitor<void> {
// OOO: Update these functions.
#if 0
// TODO: trace not used used in this type.
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
#endif  // 0

    void operator()(const point_write_t &w) {
        // TODO: Understand this line vvv
        sampler->new_sample();
        response->response = point_write_response_t();
        point_write_response_t *res =
            boost::get<point_write_response_t>(&response->response);

        // TODO: Previously we didn't pass back the superblock.
        rdb_modification_report_t mod_report(w.key);
        rdb_fdb_set(txn_, table_id_, w.key, w.data, w.overwrite, res,
            &mod_report.info, interruptor);

        update_fdb_sindexes(txn_, table_id_, *table_config_, std::move(mod_report),
            std::move(jobstate_futs_), interruptor);
    }

    // TODO: This is only used in unit tests.  We could use regular writes instead.
    void operator()(const point_delete_t &d) {
        // TODO: Understand this line vvv
        sampler->new_sample();
        response->response = point_delete_response_t();
        point_delete_response_t *res =
            boost::get<point_delete_response_t>(&response->response);

        rdb_modification_report_t mod_report(d.key);
        rdb_fdb_delete(txn_, table_id_, d.key, res,
            &mod_report.info, interruptor);
        reqlfdb_config_version cv = cv_fut_.block_and_deserialize(interruptor);
        check_cv(expected_cv_, cv);

        update_fdb_sindexes(txn_, table_id_, *table_config_, std::move(mod_report),
            std::move(jobstate_futs_), interruptor);
    }

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
            const table_config_t *_table_config,
            profile::sampler_t *_sampler,
            profile::trace_t *_trace_or_null,
            write_response_t *_response,
            const signal_t *_interruptor) :
        txn_(_txn),
        expected_cv_(_expected_cv),
        cv_fut_(transaction_get_config_version(_txn)),
        jobstate_futs_(get_jobstates(_txn, *_table_config)),
        table_id_(_table_id),
        table_config_(_table_config),
        sampler(_sampler),
        trace(_trace_or_null),
        response(_response),
        interruptor(_interruptor) {}

private:

    FDBTransaction *const txn_;
    const reqlfdb_config_version expected_cv_;
    fdb_value_fut<reqlfdb_config_version> cv_fut_;
    // TODO: We use these for everything except dummy writes, right?  Are we going to remove sync_t?
    jobstate_futs jobstate_futs_;
    const namespace_id_t table_id_;
    const table_config_t *table_config_;
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
        const table_config_t &table_config,
        const write_t &write,
        const signal_t *interruptor) {
    scoped_ptr_t<profile::trace_t> trace = ql::maybe_make_profile_trace(write.profile);
    write_response_t response;
    {
        profile::sampler_t start_write("Perform write on shard.", trace);  // TODO: Change message.
        // TODO: Pass &response.response, actually.
        fdb_write_visitor v(txn, expected_cv, table_id, &table_config, &start_write, trace.get_or_null(), &response, interruptor);
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
