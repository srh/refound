#include "fdb/xtore.hpp"

#include <unordered_map>
#include <unordered_set>

#include "errors.hpp"
#include <boost/variant/static_visitor.hpp>

#include "btree/operations.hpp"
#include "clustering/administration/tables/table_metadata.hpp"
#include "fdb/btree_utils.hpp"
#include "fdb/jobs/index_create.hpp"
#include "fdb/typed.hpp"
#include "rdb_protocol/btree.hpp"
#include "rdb_protocol/env.hpp"  // TODO: Remove include
#include "rdb_protocol/protocol.hpp"
#include "rdb_protocol/reqlfdb_config_cache.hpp"
#include "rdb_protocol/serialize_datum_onto_blob.hpp"
#include "rdb_protocol/table_common.hpp"

// This isn't in btree/keys.hpp only because I don't want to include <functional> in it.
// Stupid, yeah.
struct store_key_hash {
    size_t operator()(const store_key_t& x) const {
        return std::hash<std::string>()(x.str());
    }
};

struct jobstate_futs {
    // If jobstates has a value, the futs are empty and consumed.  Otherwise, they are
    // non-empty.

    // The strings are of course unique sindex names.
    std::vector<std::pair<std::string, fdb_value_fut<fdb_index_jobstate>>> futs_by_sindex;
    optional<std::unordered_map<std::string, fdb_index_jobstate>> jobstates;

    // You must check_cv before calling this.
    const std::unordered_map<std::string, fdb_index_jobstate> &
    block_on_jobstates(const signal_t *interruptor) {
        if (!jobstates.has_value()) {
            std::unordered_map<std::string, fdb_index_jobstate> mp;
            for (auto &pair : futs_by_sindex) {
                fdb_index_jobstate js;
                if (!pair.second.block_and_deserialize(interruptor, &js)) {
                    crash("check_cv should be called before this");
                }
                mp.emplace(std::move(pair.first), std::move(js));
            }
            jobstates.set(std::move(mp));
            futs_by_sindex.clear();
        }
        return *jobstates;
    }
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

void update_fdb_sindexes(
        FDBTransaction *txn,
        const namespace_id_t &table_id,
        const table_config_t &table_config,
        rdb_modification_report_t &&modification,
        jobstate_futs *jobstate_futs,
        const signal_t *interruptor) {
    // The thing is, we know the sindex has to be in good shape.

    // TODO: We only need to block on jobstates whose sindexes have a mutation.
    const auto &jobstates = jobstate_futs->block_on_jobstates(interruptor);

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
        sindex_disk_info_t sindex_info = rfdb::sindex_config_to_disk_info(sindex_config);

        std::unordered_set<store_key_t, store_key_hash> deletion_keys;

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

        std::unordered_set<store_key_t, store_key_hash> addition_keys;

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

        std::string fdb_key = rfdb::table_index_prefix(table_id,
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
            // QQQ: Dedup sindex writing/deletion logic.
            rdbtable_sindex_fdb_key_onto(&fdb_key, key);
            uint8_t value[1];
            fdb_transaction_set(txn,
                as_uint8(fdb_key.data()), int(fdb_key.size()),
                value, 0);
            fdb_key.resize(index_prefix_size);
        }
    }  // for each sindex
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
    rfdb::datum_fut old_value_fut = rfdb::kv_location_get(txn, kv_location);

    fdb_value old_value = future_block_on_value(old_value_fut.fut, interruptor);

    if (old_value.present) {
        mod_info->deleted.first = datum_deserialize_from_uint8(old_value.data, size_t(old_value.length));
    }

    // TODO: Review datum_t copy performance.
    mod_info->added.first = data;

    if (overwrite || !old_value.present) {
        ql::serialization_result_t res = rfdb::kv_location_set(txn, kv_location, data);
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
    // QQQ: Create these perfmons.
    // slice->stats.pm_keys_set.record();
    // slice->stats.pm_total_keys_set += 1;

    std::string kv_location = rfdb::table_primary_key(table_id, key);
    rfdb::datum_fut old_value_fut = rfdb::kv_location_get(txn, kv_location);

    fdb_value old_value = future_block_on_value(old_value_fut.fut, interruptor);

    /* Update the modification report. */
    if (old_value.present) {
        mod_info->deleted.first = datum_deserialize_from_uint8(old_value.data, size_t(old_value.length));
        rfdb::kv_location_delete(txn, kv_location);
    }

    response->result = (old_value.present ? point_delete_result_t::DELETED : point_delete_result_t::MISSING);
}

class one_fdb_replace_t {
public:
    one_fdb_replace_t(const btree_batched_replacer_t *_replacer, size_t _index)
        : replacer(_replacer), index(_index) { }

    ql::datum_t replace(const ql::datum_t &d) const {
        return replacer->replace(d, index);
    }
    return_changes_t should_return_changes() const { return replacer->should_return_changes(); }
private:
    const btree_batched_replacer_t *const replacer;
    const size_t index;
};

// TODO: Consider making each replace in a separate fdb transaction.

// Note that "and_return_superblock" in the name is just to explain how the code evolved
// from pre-fdb functions.  There is no superblock.
batched_replace_response_t rdb_fdb_replace_and_return_superblock(
        FDBTransaction *txn,
        const table_config_t &table_config,
        const store_key_t &key,
        const std::string &precomputed_kv_location,
        const one_fdb_replace_t *replacer,
        rfdb::datum_fut &&old_value_fut,
        rdb_modification_info_t *mod_info_out,
        const signal_t *interruptor) {
    const return_changes_t return_changes = replacer->should_return_changes();
    // TODO: Remove these lines or supply them somehow.
    // TODO: Pass in primary_key instead of recomputing it every time.
    const datum_string_t primary_key(table_config.basic.primary_key);

    {
        // TODO: Add these pm's.
        // info.btree->slice->stats.pm_keys_set.record();
        // info.btree->slice->stats.pm_total_keys_set += 1;

        fdb_value maybe_fdb_value = future_block_on_value(old_value_fut.fut, interruptor);

        ql::datum_t old_val;
        if (!maybe_fdb_value.present) {
            // If there's no entry with this key, pass NULL to the function.
            old_val = ql::datum_t::null();
        } else {
            // Otherwise pass the entry with this key to the function.
            old_val = datum_deserialize_from_uint8(maybe_fdb_value.data, size_t(maybe_fdb_value.length));
            guarantee(old_val.get_field(primary_key, ql::NOTHROW).has());
        }
        guarantee(old_val.has());

        ql::datum_t new_val;
        try {
            /* Compute the replacement value for the row */
            new_val = replacer->replace(old_val);

            /* Validate the replacement value and generate a stats object to return to
            the user, but don't return it yet if we need to make changes. The reason for
            this odd order is that we need to validate the change before we write the
            change. */
            rcheck_row_replacement(primary_key, key, old_val, new_val);
            bool was_changed;
            ql::datum_t resp = make_row_replacement_stats(
                primary_key, key, old_val, new_val, return_changes, &was_changed);
            if (!was_changed) {
                return resp;
            }

            /* Now that the change has passed validation, write it to ~disk~ fdb */
            if (new_val.get_type() == ql::datum_t::R_NULL) {
                rfdb::kv_location_delete(txn, precomputed_kv_location);
            } else {
                // TODO: Remove this sanity check, we already did rcheck_row_replacement.
                r_sanity_check(new_val.get_field(primary_key, ql::NOTHROW).has());
                ql::serialization_result_t res =
                    rfdb::kv_location_set(txn, precomputed_kv_location, new_val);
                if (res & ql::serialization_result_t::ARRAY_TOO_BIG) {
                    rfail_typed_target(&new_val, "Array too large for disk writes "
                                       "(limit 100,000 elements).");
                } else if (res & ql::serialization_result_t::EXTREMA_PRESENT) {
                    rfail_typed_target(&new_val, "`r.minval` and `r.maxval` cannot be "
                                       "written to disk.");
                }
                r_sanity_check(!ql::bad(res));
            }

            /* Report the changes for sindex and change-feed purposes */
            // TODO: Can we just assign R_NULL values to deleted and added?
            if (old_val.get_type() != ql::datum_t::R_NULL) {
                mod_info_out->deleted.first = old_val;
            }
            if (new_val.get_type() != ql::datum_t::R_NULL) {
                mod_info_out->added.first = new_val;
            }

            return resp;

        } catch (const ql::base_exc_t &e) {
            return make_row_replacement_error_stats(old_val,
                                                    new_val,
                                                    return_changes,
                                                    e.what());
        }
    }
}


batched_replace_response_t rdb_fdb_batched_replace(
        FDBTransaction *txn,
        const namespace_id_t &table_id,
        const table_config_t &table_config,
        const std::vector<store_key_t> &keys,
        const btree_batched_replacer_t *replacer,
        ql::configured_limits_t limits,
        const signal_t *interruptor,
        std::vector<rdb_modification_report_t> *mod_reports_out) {
    std::vector<std::string> kv_locations;
    kv_locations.reserve(keys.size());
    std::vector<rfdb::datum_fut> old_value_futs;
    old_value_futs.reserve(keys.size());

    // TODO: Might we perform too many concurrent reads from fdb?  We had
    // MAX_CONCURRENT_REPLACES=8 before.
    for (size_t i = 0; i < keys.size(); ++i) {
        kv_locations.push_back(rfdb::table_primary_key(table_id, keys[i]));
        old_value_futs.push_back(rfdb::kv_location_get(txn, kv_locations.back()));
    }

    ql::datum_t stats = ql::datum_t::empty_object();

    std::set<std::string> conditions;

    // TODO: Might we perform too many concurrent reads from fdb?  We had
    // MAX_CONCURRENT_REPLACES=8 before.
    for (size_t i = 0; i < keys.size(); ++i) {
        mod_reports_out->emplace_back(keys[i]);
        rdb_modification_report_t &mod_report = mod_reports_out->back();
        // TODO: Is one_replace_t fluff?
        one_fdb_replace_t one_replace(replacer, i);

        // QQQ: This is awful -- send them to FDB in parallel.
        ql::datum_t res = rdb_fdb_replace_and_return_superblock(
            txn,
            table_config,
            keys[i],
            kv_locations[i],
            &one_replace,
            std::move(old_value_futs[i]),
            &mod_report.info,
            interruptor);

        // TODO: This is just going to be shitty performance.
        stats = stats.merge(res, ql::stats_merge, limits, &conditions);
    }

    ql::datum_object_builder_t out(stats);
    out.add_warnings(conditions, limits);
    return std::move(out).to_datum();
}

class func_fdb_replacer_t : public btree_batched_replacer_t {
public:
    func_fdb_replacer_t(ql::env_t *_env,
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



class datum_fdb_replacer_t : public btree_batched_replacer_t {
public:
    explicit datum_fdb_replacer_t(ql::env_t *_env,
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

void handle_mod_reports(FDBTransaction *txn,
        const namespace_id_t &table_id, const table_config_t &table_config,
        std::vector<rdb_modification_report_t> &&reports,
        jobstate_futs *futs, const signal_t *interruptor) {
    for (rdb_modification_report_t &report : reports) {
        if (report.info.deleted.first.has() || report.info.added.first.has()) {
            update_fdb_sindexes(txn, table_id, table_config,
                std::move(report),
                futs,
                interruptor);
        }
    }
}


struct fdb_write_visitor : public boost::static_visitor<void> {
    void operator()(const batched_replace_t &br) {
        // TODO: Does trace really get used after we put it in ql_env?
        ql::env_t ql_env(
            nullptr,    // QQQ: Include global optargs in op_term_t::is_deterministic impl.
            ql::return_empty_normal_batches_t::NO,
            interruptor,
            br.serializable_env,
            trace);

        // TODO: Make func_fdb_replacer_t take a deterministic_func write hook.
        counted_t<const ql::func_t> write_hook;
        if (br.write_hook.has_value()) {
            write_hook = br.write_hook->det_func.compile_wire_func();
        }

        func_fdb_replacer_t replacer(&ql_env, br.pkey, br.f,
            write_hook, br.return_changes);
        std::vector<rdb_modification_report_t> mod_reports;
        response->response =
            rdb_fdb_batched_replace(
                txn_,
                table_id_,
                *table_config_,
                br.keys,
                &replacer,
                ql_env.limits(),
                interruptor,
                &mod_reports);

        // We call check_cv before using jobstate_futs_.
        reqlfdb_config_version cv = cv_fut_.block_and_deserialize(interruptor);
        check_cv(expected_cv_, cv);

        handle_mod_reports(txn_, table_id_, *table_config_, std::move(mod_reports),
            &jobstate_futs_, interruptor);
    }

    // QQQ: Is batched_insert_t::pkey merely the table's pkey?  Seems weird to have.
    void operator()(const batched_insert_t &bi) {
        ql::env_t ql_env(
            nullptr,  // QQQ: Include global optargs in op_term_t::is_deterministic impl.
            ql::return_empty_normal_batches_t::NO,
            interruptor,
            bi.serializable_env,
            trace);
        // TODO: Does the type datum_replacer_t or datum_fdb_replacer_t really need to exist?
        datum_fdb_replacer_t replacer(&ql_env, bi);
        std::vector<store_key_t> keys;
        keys.reserve(bi.inserts.size());
        for (auto it = bi.inserts.begin(); it != bi.inserts.end(); ++it) {
            keys.emplace_back(it->get_field(datum_string_t(bi.pkey)).print_primary());
        }
        std::vector<rdb_modification_report_t> mod_reports;
        response->response =
            rdb_fdb_batched_replace(
                txn_,
                table_id_,
                *table_config_,
                keys,
                &replacer,
                bi.limits,
                interruptor,
                &mod_reports);

        // We call check_cv before using jobstate_futs_.
        reqlfdb_config_version cv = cv_fut_.block_and_deserialize(interruptor);
        check_cv(expected_cv_, cv);

        handle_mod_reports(txn_, table_id_, *table_config_, std::move(mod_reports),
            &jobstate_futs_, interruptor);
    }

    void operator()(const point_write_t &w) {
        // TODO: Understand this line vvv
        sampler->new_sample();
        response->response = point_write_response_t();
        point_write_response_t *res =
            boost::get<point_write_response_t>(&response->response);

        rdb_modification_report_t mod_report(w.key);
        rdb_fdb_set(txn_, table_id_, w.key, w.data, w.overwrite, res,
            &mod_report.info, interruptor);
        reqlfdb_config_version cv = cv_fut_.block_and_deserialize(interruptor);
        check_cv(expected_cv_, cv);

        update_fdb_sindexes(txn_, table_id_, *table_config_, std::move(mod_report),
            &jobstate_futs_, interruptor);
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
            &jobstate_futs_, interruptor);
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
    // TODO: Maybe don't compute jobstate_futs for every write, like sync_t, if that still exists.
    // Note that we should call check_cv before we call code that assumes the jobstate
    // futs are legit.
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
        // TODO: The read version has some PROFILER_STARER_IF_ENABLED macro.
        profile::sampler_t start_write("Perform write on shard.", trace);  // TODO: Change message.
        // TODO: Pass &response.response, actually.
        fdb_write_visitor v(txn, expected_cv, table_id, &table_config, &start_write,
            trace.get_or_null(), &response, interruptor);
        boost::apply_visitor(v, write.write);
    }

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
