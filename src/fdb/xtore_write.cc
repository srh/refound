#include "fdb/xtore_write.hpp"

#include <set>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

#include "errors.hpp"
#include <boost/variant/static_visitor.hpp>

#include "clustering/tables/table_metadata.hpp"
#include "fdb/btree_utils.hpp"
#include "fdb/jobs/index_create.hpp"
#include "fdb/typed.hpp"
#include "math.hpp"
#include "rdb_protocol/btree.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/protocol.hpp"
#include "rdb_protocol/reqlfdb_config_cache.hpp"
#include "rdb_protocol/serialize_datum_onto_blob.hpp"
#include "rdb_protocol/table_common.hpp"
#include "rdb_protocol/val.hpp"

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
    for (const auto &el : table_config.sindexes) {
        const sindex_metaconfig_t &second = el.second;
        if (!second.creation_task_or_nil.value.is_nil()) {
            ret.futs_by_sindex.emplace_back(el.first,
                transaction_lookup_uq_index<index_jobstate_by_task>(
                    txn,
                    second.creation_task_or_nil,
                    false));
        }
    }
    return ret;
}

bool upper_bound_exceeds_pkey(const optional<ukey_string> &upper_bound, const store_key_t &key) {
    // r_nullopt represents +infinity
    return !upper_bound.has_value() || key.str() < upper_bound->ukey;
}

bool upper_bound_lt(const optional<ukey_string> &a, const optional<ukey_string> &b) {
    return !b.has_value() || (a.has_value() && a->ukey < b->ukey);
}

struct forced_index_lower_bound {
    store_key_t key;
    // value.has() is always true -- we ignore deletions.
    ql::datum_t value;
};

approx_txn_size update_fdb_sindexes(
        FDBTransaction *txn,
        const namespace_id_t &table_id,
        const table_config_t &table_config,
        const store_key_t &primary_key,
        rdb_modification_info_t &&info,
        jobstate_futs *jobstate_futs,
        std::unordered_map<std::string, forced_index_lower_bound> *sindex_max_claimkeys,
        const signal_t *interruptor) {
    // The thing is, we know the sindex has to be in good shape.

    // TODO: We only need to block on jobstates whose sindexes have a mutation.
    const auto &jobstates = jobstate_futs->block_on_jobstates(interruptor);

    approx_txn_size txn_size{0};

    for (const auto &pair : table_config.sindexes) {
        const sindex_metaconfig_t &fdb_sindex_config = pair.second;
        const sindex_config_t &sindex_config = fdb_sindex_config.config;

        auto jobstates_it = jobstates.find(pair.first);
        if (jobstates_it != jobstates.end()) {
            const fdb_index_jobstate &js = jobstates_it->second;
            if (upper_bound_exceeds_pkey(js.unindexed_upper_bound, primary_key)) {
                if (!upper_bound_exceeds_pkey(js.claimed_bound, primary_key)) {
                    if (info.added.has()) {
                        // Update claimkeys (for pushing along index build jobs that have a
                        // claim interval).
                        auto claimkey_it = sindex_max_claimkeys->find(pair.first);
                        if (claimkey_it == sindex_max_claimkeys->end()) {
                            sindex_max_claimkeys->emplace(
                                    pair.first,
                                    forced_index_lower_bound{primary_key, info.added});
                        } else if (claimkey_it->second.key < primary_key) {
                            claimkey_it->second = forced_index_lower_bound{primary_key, info.added};
                        }
                    }
                }
                continue;
            }
        }

        // TODO: Making this copy is gross -- would be better if compute_keys took sindex_config.
        sindex_disk_info_t sindex_info = rfdb::sindex_config_to_disk_info(sindex_config);

        std::unordered_set<store_key_t, store_key_hash> deletion_keys;

        if (info.deleted.has()) {
            try {
                ql::datum_t deleted = info.deleted;

                // TODO: The ql::datum_t value is unused.  Remove it once FDB-ized fully.
                std::vector<store_key_t> keys;
                compute_keys(
                    primary_key, deleted, sindex_info,
                    &keys, nullptr);
                for (auto &k : keys) {
                    deletion_keys.emplace(std::move(k));
                }
            } catch (const ql::base_exc_t &) {
                // Do nothing (it wasn't actually in the index).
            }
        }

        std::unordered_set<store_key_t, store_key_hash> addition_keys;

        if (info.added.has()) {
            try {
                ql::datum_t added = info.added;

                std::vector<store_key_t> keys;
                compute_keys(
                    primary_key, added, sindex_info,
                    &keys, nullptr);
                for (auto &k : keys) {
                    addition_keys.emplace(std::move(k));
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
                txn_size.value += fdb_key.size();
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
            txn_size.value += fdb_key.size();
            fdb_key.resize(index_prefix_size);
        }
    }  // for each sindex

    return txn_size;
}

optional<store_key_t> convert_upper_bound_to_store_key(const optional<ukey_string> &k) {
    optional<store_key_t> ret;
    if (k.has_value()) {
        ret.set(store_key_t(k->ukey));
    }
    return ret;
}

void push_along_sindex_creation(
        const signal_t *interruptor, FDBTransaction *txn,
        const namespace_id_t &table_id, const table_config_t &table_config,
        const std::unordered_map<std::string, forced_index_lower_bound> &sindex_max_claimkeys,
        const std::unordered_map<std::string, fdb_index_jobstate> &jobstates) {

    // To avoid duplicate reads, we combine overlapping intervals.  Because each
    // store_key_t in sindex_max_claimkeys is the _maximum_ key less than its jobstate's
    // unindexed_upper_bound, it follows that all overlapping intervals have the same left
    // edge.  (Otherwise, with overlapping intervals, the lesser left edge key wouldn't be
    // the maximum key less than its jobstate's unindexed_upper_bound.)  So we combine
    // them with this hash map, instead of some kind of ordered map.
    //
    // Carries the store_key_t's datum from forced_index_lower_bound.
    // Also carries a list of sindexes we conflict with (so that we can map back from interval to sindex later).
    std::unordered_map<store_key_t, std::tuple<ql::datum_t, optional<ukey_string>, std::vector<std::string>>, store_key_hash> intervals_to_read;


    for (const auto &pair : sindex_max_claimkeys) {
        // TODO: remove commented lines
        // const sindex_metaconfig_t &metaconfig = sindexes.at(pair.first);
        // const sindex_config_t &sindex_config = metaconfig.config;

        const fdb_index_jobstate &js = jobstates.at(pair.first);

        auto it = intervals_to_read.find(pair.second.key);

        if (it == intervals_to_read.end()) {
            intervals_to_read.emplace(pair.second.key,
                std::make_tuple(pair.second.value, js.unindexed_upper_bound, std::vector<std::string>{pair.first}));
        } else {
            if (upper_bound_lt(std::get<1>(it->second), js.unindexed_upper_bound)) {
                std::get<1>(it->second) = js.unindexed_upper_bound;
                std::get<2>(it->second).push_back(pair.first);
            }
        }
    }

    std::string kv_prefix = rfdb::table_pkey_prefix(table_id);
    // TODO: Break datum iterator usage into prep_for_step() and block_for_step().

    std::vector<std::pair<store_key_t, rfdb::datum_range_iterator>> iters;
    for (const auto &interval : intervals_to_read) {
        store_key_t lowerbound = interval.first;
        optional<store_key_t> upperbound;
        if (!lowerbound.increment1()) {
            // We had the max key, so (interval.first, +infinity) is the empty interval.
            // We'll just (wastefully) construct an iterator in this rare edge case.
            lowerbound = store_key_t::min();
            upperbound.set(store_key_t::min());
        } else {
            upperbound = convert_upper_bound_to_store_key(std::get<1>(interval.second));
        }
        // TODO: Figure out why we use ukeys instead of store_key_t's.
        iters.emplace_back(interval.first,
            rfdb::primary_prefix_make_iterator(
                    kv_prefix, lowerbound, upperbound.ptr_or_null(),
                    false, false));
    }

    for (auto &iterpair : iters) {
        // TODO: We'd really want to do these operations in parallel, if we have multiple
        // outstanding index builds.  But this is an edge case where slowing down writes
        // if that allows index operations to pass by is actually okay.
        size_t bytes_read_discard;
        bool more = false;
        std::vector<std::pair<store_key_t, ql::datum_t>> kvs;
        kvs.emplace_back(iterpair.first, std::get<0>(intervals_to_read.at(iterpair.first)));
        do {
            std::pair<std::vector<std::pair<store_key_t, std::vector<uint8_t>>>, bool>
                results = iterpair.second.query_and_step(txn, interruptor, FDB_STREAMING_MODE_WANT_ALL,
                    0, &bytes_read_discard);
            more = results.second;

            for (auto &elem : results.first) {
                kvs.emplace_back(elem.first,
                    ql::parse_table_value(as_char(elem.second.data()), elem.second.size()));
            }
        } while (more);

        for (const std::string &sindex : std::get<2>(intervals_to_read.at(iterpair.first))) {
            const sindex_metaconfig_t &metaconfig = table_config.sindexes.at(sindex);
            const sindex_config_t &sindex_config = metaconfig.config;

            // TODO: Making this copy is gross -- would be better if compute_keys took sindex_config.
            sindex_disk_info_t index_info = rfdb::sindex_config_to_disk_info(sindex_config);
            std::string fdb_key = rfdb::table_index_prefix(table_id, metaconfig.sindex_id);
            const size_t index_prefix_size = fdb_key.size();

            for (const auto &elem : kvs) {
                // TODO: This is basically copy/pasted from index_create.cc, except for the
                // parse_table_value call being earlier.
                try {
                    // OOO: Sanity-check that compute_keys doesn't do any deranged truncation, and
                    // secondary index keys in FDB are not incremented.  (They probably are.)
                    // Maybe make a separate type for secondary index keys, amidst general key
                    // type cleanup.
                    std::vector<store_key_t> keys;
                    compute_keys(elem.first, elem.second, index_info, &keys, nullptr);

                    for (auto &sindex_key : keys) {
                        // TODO: Make sure fdb key limits are followed.
                        rdbtable_sindex_fdb_key_onto(&fdb_key, sindex_key);
                        uint8_t value[1];
                        fdb_transaction_set(txn,
                            as_uint8(fdb_key.data()), int(fdb_key.size()),
                            value, 0);
                        fdb_key.resize(index_prefix_size);
                    }
                } catch (const ql::base_exc_t &) {
                    // Do nothing (the row doesn't get put into the index)
                }
            }

            // Now make a single-key conflict range for the jobstate key, which we'd
            // previously read with a snapshot read.
            guarantee(!metaconfig.creation_task_or_nil.value.is_nil());
            std::string jobstate_key = uq_index_fdb_key<index_jobstate_by_task>(metaconfig.creation_task_or_nil);
            // This is "clever" but we pass an incremented jobstate key simply by exposing
            // the c_str() with its '\0' terminator.
            fdb_error_t err = fdb_transaction_add_conflict_range(
                txn,
                as_uint8(jobstate_key.data()), int(jobstate_key.size()),
                as_uint8(jobstate_key.c_str()), int(jobstate_key.size() + 1),
                FDB_CONFLICT_RANGE_TYPE_READ);
            if (err != 0) {
                throw fdb_transaction_exception(err);
            }

            const fdb_index_jobstate &js = jobstates.at(sindex);

            // And here we update the sindex's jobstate.  It's never removed here -- this
            // can never finish the index build transaction, as always traverse to a known
            // key -- never off the front of the index.
            fdb_index_jobstate new_jobstate = fdb_index_jobstate{
                r_nullopt,  // filled in the next statement
                make_optional(ukey_string{iterpair.first.str()})};
            if (upper_bound_lt(js.claimed_bound, new_jobstate.unindexed_upper_bound)) {
                new_jobstate.claimed_bound = js.claimed_bound;
            }
            transaction_set_uq_index<index_jobstate_by_task>(txn, metaconfig.creation_task_or_nil,
                new_jobstate);
        }

        // Now set the conflict range on the key range.  (This is very duplicative of
        // index_create code.)
        {
            std::string lower_bound = rfdb::datum_range_lower_bound(kv_prefix, iterpair.first);
            optional<store_key_t> skey_upper_bound = convert_upper_bound_to_store_key(std::get<1>(intervals_to_read.at(iterpair.first)));
            std::string upper_bound = rfdb::datum_range_upper_bound(kv_prefix, skey_upper_bound.ptr_or_null());
            fdb_error_t err = fdb_transaction_add_conflict_range(
                    txn,
                    as_uint8(lower_bound.data()), int(lower_bound.size()),
                    as_uint8(upper_bound.data()), int(upper_bound.size()),
                    FDB_CONFLICT_RANGE_TYPE_WRITE);
            if (err != 0) {
                throw fdb_transaction_exception(err);
            }
        }
    }
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

    optional<std::vector<uint8_t>> old_value
        = block_and_read_unserialized_datum(txn, std::move(old_value_fut), interruptor);

    if (old_value.has_value()) {
        mod_info->deleted = datum_deserialize_from_uint8(old_value->data(), old_value->size());
    }

    mod_info->added = data;

    if (overwrite || !old_value.has_value()) {
        // OOO: This does duplicate code with the other place that calls kv_location_set.
        std::string str;
        ql::serialization_result_t res = datum_serialize_to_string(data, &str);

        if (res & ql::serialization_result_t::ARRAY_TOO_BIG) {
            rfail_typed_target(&data, "Array too large for disk writes "
                               "(limit 100,000 elements).");
        } else if (res & ql::serialization_result_t::EXTREMA_PRESENT) {
            rfail_typed_target(&data, "`r.minval` and `r.maxval` cannot be "
                               "written to disk.");
        }
        r_sanity_check(!ql::bad(res));  // TODO: Compile time assertion.

        if (str.size() > REQLFDB_MAX_LARGE_VALUE_SIZE) {
            rfail_typed_target(&data,
                "Document too large for disk writes (limit " REQLFDB_MAX_LARGE_VALUE_SIZE_STR ")");
        }

        rfdb::kv_location_set(txn, kv_location, str);
    }

    response_out->result =
        (old_value.has_value() ? point_write_result_t::DUPLICATE : point_write_result_t::STORED);
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

    optional<std::vector<uint8_t>> old_value
        = block_and_read_unserialized_datum(txn, std::move(old_value_fut), interruptor);

    /* Update the modification report. */
    if (old_value.has_value()) {
        mod_info->deleted = datum_deserialize_from_uint8(old_value->data(), old_value->size());
        rfdb::kv_location_delete(txn, kv_location);
    }

    response->result = (old_value.has_value() ? point_delete_result_t::DELETED : point_delete_result_t::MISSING);
}

// TODO: Consider making each replace in a separate fdb transaction.

// Note that "and_return_superblock" in the name is just to explain how the code evolved
// from pre-fdb functions.  There is no superblock.
//
// Returns nullopt if serialized_size_limit would overflow.
// The return value's approx_txn_size is some vaguely approximate size we increase the transaction by.
optional<std::pair<batched_replace_response_t, approx_txn_size>> rdb_fdb_replace_and_return_superblock(
        FDBTransaction *txn,
        const datum_string_t &primary_key,
        const store_key_t &key,
        const std::string &precomputed_kv_location,
        const btree_batched_replacer_t *replacer,
        const size_t index,
        rfdb::datum_fut &&old_value_fut,
        size_t serialized_size_limit,
        rdb_modification_info_t *mod_info_out,
        const signal_t *interruptor) {
    const return_changes_t return_changes = replacer->should_return_changes();
    // TODO: Remove these lines or supply them somehow.

    {
        // TODO: Add these pm's.
        // info.btree->slice->stats.pm_keys_set.record();
        // info.btree->slice->stats.pm_total_keys_set += 1;

        optional<std::vector<uint8_t>> maybe_fdb_value
            = block_and_read_unserialized_datum(txn, std::move(old_value_fut), interruptor);

        ql::datum_t old_val;
        if (!maybe_fdb_value.has_value()) {
            // If there's no entry with this key, pass NULL to the function.
            old_val = ql::datum_t::null();
        } else {
            // Otherwise pass the entry with this key to the function.
            old_val = datum_deserialize_from_uint8(maybe_fdb_value->data(), maybe_fdb_value->size());
            guarantee(old_val.get_field(primary_key, ql::NOTHROW).has());
        }
        guarantee(old_val.has());

        ql::datum_t new_val;
        try {
            /* Compute the replacement value for the row */
            new_val = replacer->replace(old_val, index);

            /* Validate the replacement value and generate a stats object to return to
            the user, but don't return it yet if we need to make changes. The reason for
            this odd order is that we need to validate the change before we write the
            change. */
            rcheck_row_replacement(primary_key, key, old_val, new_val);
            bool was_changed;
            ql::datum_t resp = make_row_replacement_stats(
                primary_key, key, old_val, new_val, return_changes, &was_changed);
            if (!was_changed) {
                return make_optional(std::make_pair(std::move(resp), approx_txn_size{0}));
            }

            approx_txn_size our_size{0};

            /* Now that the change has passed validation, write it to ~disk~ fdb */
            if (new_val.get_type() == ql::datum_t::R_NULL) {
                our_size = rfdb::kv_location_delete(txn, precomputed_kv_location);
            } else {
                // TODO: Remove this sanity check, we already did rcheck_row_replacement.
                r_sanity_check(new_val.get_field(primary_key, ql::NOTHROW).has());
                std::string serialized_new_val;
                ql::serialization_result_t res = datum_serialize_to_string(new_val, &serialized_new_val);

                if (res & ql::serialization_result_t::ARRAY_TOO_BIG) {
                    rfail_typed_target(&new_val, "Array too large for disk writes "
                                       "(limit 100,000 elements).");
                } else if (res & ql::serialization_result_t::EXTREMA_PRESENT) {
                    rfail_typed_target(&new_val, "`r.minval` and `r.maxval` cannot be "
                                       "written to disk.");
                }
                r_sanity_check(!ql::bad(res));
                if (serialized_new_val.size() > REQLFDB_MAX_LARGE_VALUE_SIZE) {
                    rfail_typed_target(&new_val,
                        "Document too large for disk writes (limit " REQLFDB_MAX_LARGE_VALUE_SIZE_STR ")");
                }

                if (serialized_new_val.size() > serialized_size_limit) {
                    return r_nullopt;
                }

                our_size = rfdb::kv_location_set(txn, precomputed_kv_location, serialized_new_val);
            }

            /* Report the changes for sindex and change-feed purposes */
            // TODO: Can we just assign R_NULL values to deleted and added?
            if (old_val.get_type() != ql::datum_t::R_NULL) {
                mod_info_out->deleted = old_val;
            }
            if (new_val.get_type() != ql::datum_t::R_NULL) {
                mod_info_out->added = new_val;
            }

            return make_optional(std::make_pair(std::move(resp), our_size));

        } catch (const ql::base_exc_t &e) {
            return make_optional(std::make_pair(
                make_row_replacement_error_stats(
                    old_val, new_val, return_changes, e.what()),
                approx_txn_size{0}));
        }
    }
}

// See also "struct secondary_batch_size_calc".
struct batch_size_calc {
    explicit batch_size_calc(size_t initial_recommendation)
        : next_recommended_batch(initial_recommendation), last_batch(initial_recommendation) {}
    size_t next_recommended_batch;
    size_t last_batch;

    void note_completed_batch(size_t size) {
        last_batch = size;
        size_t x = std::max<size_t>(1, std::max<size_t>(next_recommended_batch / 2, last_batch));
        // Grotesque hackish nonsense to avoid some perpetual small-batch scenario.  But
        // this will needlessly sawtooth batch sizes.
        next_recommended_batch = add_rangeclamped<size_t>(x, ceil_divide(x, 32));
    }

    // Caller might need to clamp this by the number of keys.
    size_t recommended_batch() const {
        return next_recommended_batch;
    }
};


batched_replace_response_t rdb_fdb_batched_replace(
        FDBDatabase *fdb,
        const reqlfdb_config_version prior_cv,
        const auth::user_context_t &user_context,
        const namespace_id_t &table_id,
        const table_config_t &table_config,
        const bool needs_config_permission,
        const std::vector<store_key_t> &keys,
        const btree_batched_replacer_t *replacer,
        ql::configured_limits_t limits,
        const signal_t *interruptor) {

    try {
        batched_replace_response_t stats = ql::datum_t::empty_object();
        std::set<std::string> conditions;
        size_t keys_complete = 0;
        const size_t INIT_SPLIT_SIZE = 128;  // TODO: This comes from real_table.cc.
        batch_size_calc calc(std::min<size_t>(INIT_SPLIT_SIZE, keys.size() - keys_complete));
        while (keys_complete < keys.size()) {
            const size_t recommended_batch = std::min<size_t>(keys.size() - keys_complete, calc.recommended_batch());

            uint64_t retry_count = 0;
            std::tuple<batched_replace_response_t, std::set<std::string>, size_t> p =  perform_write_operation<std::tuple<batched_replace_response_t, std::set<std::string>, size_t>>(fdb, interruptor, prior_cv, user_context, table_id, table_config,
                    needs_config_permission, [&](const signal_t *interruptor, FDBTransaction *txn, cv_auth_check_fut_write &&cva) {
                const uint64_t count = retry_count++;

                const size_t keys_to_process = std::max<size_t>(1, recommended_batch >> count);
                // So, we're going to process keys in [keys_complete, keys_complete + keys_to_process).

                jobstate_futs jobstate_futs = get_jobstates(txn, table_config);
                batched_replace_response_t response;

                // Parallel arrays (with the subslice keys[keys_complete...keys_complete + keys_to_process]).
                std::vector<std::string> kv_locations;
                kv_locations.reserve(keys_to_process);
                std::vector<rfdb::datum_fut> old_value_futs;
                old_value_futs.reserve(keys_to_process);

                // TODO: Might we perform too many concurrent reads from fdb?  We had
                // MAX_CONCURRENT_REPLACES=8 before.
                for (size_t i = keys_complete; i < keys_complete + keys_to_process; ++i) {
                    kv_locations.push_back(rfdb::table_primary_key(table_id, keys[i]));
                    old_value_futs.push_back(rfdb::kv_location_get(txn, kv_locations.back()));
                }

                // We need to check the cvc before using jobstate_futs_.
                cva.cvc.block_and_check(interruptor);
                // Might as well check auths here too.
                cva.block_and_check_auths(interruptor);

                ql::datum_t stats = ql::datum_t::empty_object();

                std::set<std::string> conditions;

                const datum_string_t primary_key_column(table_config.basic.primary_key);

                std::unordered_map<std::string, forced_index_lower_bound> sindex_max_claimkeys;

                size_t keys_processed = 0;

                approx_txn_size size_processed{0};

                // TODO: Might we perform too many concurrent reads from fdb?  We had
                // MAX_CONCURRENT_REPLACES=8 before.
                for (size_t i2 = 0; i2 < keys_to_process; ++i2) {
                    rdb_modification_info_t mod_info;

                    if (size_processed.value >= REQLFDB_WRITE_TXN_SOFT_LIMIT) {
                        // Break if the previous writes put us over the soft transaction
                        // size limit.  We may do this later in the loop if the _current_
                        // write puts us over the soft limit.
                        break;
                    }

                    /* We know REQLFDB_WRITE_TXN_SOFT_LIMIT - size_processed.value is
                       positive because of the preceding comparison.  We pass in
                       effectively no size limit for the first document --
                       rdb_fdb_replace_and_return_superblock respeects the hard limit,
                       which is REQLFDB_MAX_LARGE_VALUE_SIZE. */
                    const size_t soft_limit = i2 == 0 ? REQLFDB_MAX_LARGE_VALUE_SIZE * 2 :
                        REQLFDB_WRITE_TXN_SOFT_LIMIT - size_processed.value;

                    optional<std::pair<ql::datum_t, approx_txn_size>> res = rdb_fdb_replace_and_return_superblock(
                        txn,
                        primary_key_column,
                        keys[keys_complete + i2],
                        kv_locations[i2],
                        replacer,
                        keys_complete + i2,
                        std::move(old_value_futs[i2]),
                        soft_limit,
                        &mod_info,
                        interruptor);

                    if (!res.has_value()) {
                        // Break if the current write would put us over the soft limit
                        break;
                    }

                    size_processed.value += res->second.value;
                    if (mod_info.has_any()) {
                        approx_txn_size sz = update_fdb_sindexes(txn, table_id, table_config, keys[keys_complete + i2],
                            std::move(mod_info), &jobstate_futs, &sindex_max_claimkeys,
                            interruptor);
                        size_processed.value += sz.value;
                    }

                    // TODO: This is just going to be shitty performance.
                    stats = stats.merge(res->first, ql::stats_merge, limits, &conditions);
                    keys_processed += 1;
                }

                if (!sindex_max_claimkeys.empty()) {
                    push_along_sindex_creation(interruptor, txn,
                        table_id, table_config, sindex_max_claimkeys,
                        jobstate_futs.block_on_jobstates(interruptor));
                }

                commit(txn, interruptor);

                ql::datum_object_builder_t out(stats);
                out.add_warnings(conditions, limits);
                return std::make_tuple(std::move(out).to_datum(), std::move(conditions), keys_processed);
            });

            conditions.insert(std::get<1>(p).begin(), std::get<1>(p).end());
            stats = stats.merge(std::get<0>(p), ql::stats_merge, limits, &conditions);
            const size_t latest_batch = std::get<2>(p);
            keys_complete += latest_batch;
            calc.note_completed_batch(latest_batch);
        }
        ql::datum_object_builder_t tmp(std::move(stats));
        tmp.add_warnings(conditions, limits);
        return std::move(tmp).to_datum();
    } catch (const provisional_assumption_exception &exc) {
        throw config_version_exc_t();
    }
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
        ql::datum_t res = f->call(env, d, ql::eval_flags_t::LITERAL_OK)->as_datum(env);

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
                                  const batched_insert_t &bi,
                                  const table_config_t &table_config)
        : env(_env),
          datums(&bi.inserts),
          conflict_behavior(bi.conflict_behavior),
          pkey(bi.pkey),
          return_changes(bi.return_changes),
          conflict_func(bi.conflict_func) {
        if (bi.ignore_write_hook == ignore_write_hook_t::NO && table_config.write_hook.has_value()) {
            // TODO: We pay no attention to write_hook->func_version.
            write_hook = table_config.write_hook->func.det_func.compile_wire_func();
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

struct fdb_write_visitor : public boost::static_visitor<void> {
    void operator()(const batched_replace_t &br) {
        const bool needs_config_permission = br.ignore_write_hook == ignore_write_hook_t::YES;

        // TODO: Does trace really get used after we put it in ql_env?
        ql::env_t ql_env(
            nullptr,    // QQQ: Include global optargs in op_term_t::is_deterministic impl.
            ql::return_empty_normal_batches_t::NO,
            interruptor,
            br.serializable_env,
            trace);

        // TODO: Make func_fdb_replacer_t take a deterministic_func write hook.
        counted_t<const ql::func_t> write_hook;
        if (br.ignore_write_hook == ignore_write_hook_t::NO && table_config_->write_hook.has_value()) {
            write_hook = table_config_->write_hook->func.det_func.compile_wire_func();
        }

        const func_fdb_replacer_t replacer(&ql_env, br.pkey, br.f,
            write_hook, br.return_changes);

        response_->response =
            rdb_fdb_batched_replace(
                fdb_,
                prior_cv_,
                *user_context_,
                table_id_,
                *table_config_,
                needs_config_permission,
                br.keys,
                &replacer,
                ql_env.limits(),
                interruptor);
    }

    // QQQ: Is batched_insert_t::pkey merely the table's pkey?  Seems weird to have.
    void operator()(const batched_insert_t &bi) {
        const bool needs_config_permission = bi.ignore_write_hook == ignore_write_hook_t::YES;

        ql::env_t ql_env(
            nullptr,  // QQQ: Include global optargs in op_term_t::is_deterministic impl.
            ql::return_empty_normal_batches_t::NO,
            interruptor,
            bi.serializable_env,
            trace);
        // TODO: Does the type datum_replacer_t or datum_fdb_replacer_t really need to exist?
        const datum_fdb_replacer_t replacer(&ql_env, bi, *table_config_);

        std::vector<store_key_t> keys;
        keys.reserve(bi.inserts.size());
        for (auto it = bi.inserts.begin(); it != bi.inserts.end(); ++it) {
            keys.emplace_back(it->get_field(datum_string_t(bi.pkey)).print_primary());
        }

        response_->response =
            rdb_fdb_batched_replace(
                fdb_,
                prior_cv_,
                *user_context_,
                table_id_,
                *table_config_,
                needs_config_permission,
                keys,
                &replacer,
                bi.limits,
                interruptor);
    }

    void operator()(const point_write_t &w) {
        const bool needs_config_permission = false;
        response_->response = perform_write_operation<point_write_response_t>(fdb_, interruptor, prior_cv_, *user_context_, table_id_, *table_config_,
                needs_config_permission, [&](const signal_t *interruptor, FDBTransaction *txn, cv_auth_check_fut_write &&cva) {
            jobstate_futs jobstate_futs = get_jobstates(txn, *table_config_);
            // TODO: Understand this line vvv
            sampler->new_sample();
            point_write_response_t res;

            rdb_modification_info_t mod_info;
            rdb_fdb_set(txn, table_id_, w.key, w.data, w.overwrite, &res,
                &mod_info, interruptor);

            // We need to check the cvc before jobstates.
            cva.cvc.block_and_check(interruptor);
            // Might as well check auths here too.
            cva.block_and_check_auths(interruptor);

            // TODO: Force sindex update?  This is only used in unit tests though.
            std::unordered_map<std::string, forced_index_lower_bound> sindex_max_claimkeys;
            update_fdb_sindexes(txn, table_id_, *table_config_, w.key, std::move(mod_info),
                &jobstate_futs, &sindex_max_claimkeys, interruptor);
            commit(txn, interruptor);
            return res;
        });
    }

    // TODO: This is only used in unit tests.  We could use regular writes instead.
    void operator()(const point_delete_t &d) {
        const bool needs_config_permission = false;
        response_->response = perform_write_operation<point_delete_response_t>(fdb_, interruptor, prior_cv_, *user_context_, table_id_, *table_config_,
                needs_config_permission, [&](const signal_t *interruptor, FDBTransaction *txn, cv_auth_check_fut_write &&cva) {
            jobstate_futs jobstate_futs = get_jobstates(txn, *table_config_);
            write_response_t response;

            // TODO: Understand this line vvv
            sampler->new_sample();
            response.response = point_delete_response_t();
            point_delete_response_t res;

            rdb_modification_info_t mod_info;
            rdb_fdb_delete(txn, table_id_, d.key, &res,
                &mod_info, interruptor);

            cva.cvc.block_and_check(interruptor);
            cva.block_and_check_auths(interruptor);

            // TODO: Force sindex update?  This is only used in unit tests though.
            std::unordered_map<std::string, forced_index_lower_bound> sindex_max_claimkeys;
            update_fdb_sindexes(txn, table_id_, *table_config_, d.key, std::move(mod_info),
                &jobstate_futs, &sindex_max_claimkeys, interruptor);

            commit(txn, interruptor);
            return res;
        });
    }

    void operator()(const sync_t &) {
        const bool needs_config_permission = false;
        response_->response = perform_write_operation<sync_response_t>(fdb_, interruptor, prior_cv_, *user_context_, table_id_, *table_config_,
                needs_config_permission, [&](const signal_t *interruptor, FDBTransaction *, cv_auth_check_fut_write &&cva) {
            // TODO: Understand what this does.
            sampler->new_sample();

            // We have to check cv, for the usual reasons: to make sure table name->id
            // mapping we used was legit.
            cva.cvc.block_and_check(interruptor);
            // And we have to check permissions.
            cva.block_and_check_auths(interruptor);

            // Nothing to commit.  Writes already sync.

            return sync_response_t();
        });
    }

    void operator()(const dummy_write_t &) {
        const bool needs_config_permission = false;
        response_->response = perform_write_operation<dummy_write_response_t>(fdb_, interruptor, prior_cv_, *user_context_, table_id_, *table_config_,
                needs_config_permission, [&](const signal_t *interruptor, FDBTransaction *, cv_auth_check_fut_write &&cva) {
            // We have to check cv, for the usual reasons: to make sure table name->id
            // mapping we used was legit.
            cva.cvc.block_and_check(interruptor);
            cva.block_and_check_auths(interruptor);

            // Nothing to commit.

            return dummy_write_response_t();
        });
    }

    // One responsibility all visitor methods have is to check expected_cv.  Preferably
    // before they try anything long and expensive.

    fdb_write_visitor(
            FDBDatabase *fdb,
            reqlfdb_config_version prior_cv,
            const auth::user_context_t *user_context,
            const namespace_id_t &_table_id,
            const table_config_t *_table_config,
            profile::sampler_t *_sampler,
            profile::trace_t *_trace_or_null,
            write_response_t *_response,
            const signal_t *_interruptor) :
        fdb_(fdb),
        prior_cv_(prior_cv),
        user_context_(user_context),
        table_id_(_table_id),
        table_config_(_table_config),
        sampler(_sampler),
        trace(_trace_or_null),
        response_(_response),
        interruptor(_interruptor) {}

private:
    FDBDatabase *const fdb_;
    const reqlfdb_config_version prior_cv_;
    const auth::user_context_t *const user_context_;
    const namespace_id_t table_id_;
    const table_config_t *table_config_;
    profile::sampler_t *const sampler;
    profile::trace_t *const trace;
    write_response_t *const response_;
    const signal_t *const interruptor;

    DISABLE_COPYING(fdb_write_visitor);
};


write_response_t apply_write(FDBDatabase *fdb,
        reqlfdb_config_version prior_cv,
        const auth::user_context_t &user_context,
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
        fdb_write_visitor v(fdb, prior_cv, &user_context, table_id, &table_config, &start_write,
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
