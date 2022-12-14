#include "fdb/xtore_read.hpp"

#include "errors.hpp"
#include <boost/variant/static_visitor.hpp>

#include "clustering/auth/user_context.hpp"
#include "clustering/auth/user_fut.hpp"
#include "clustering/tables/table_metadata.hpp"
#include "fdb/btree_utils.hpp"
#include "fdb/retry_loop.hpp"
#include "fdb/typed.hpp"
#include "math.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/geo_traversal.hpp"
#include "rdb_protocol/protocol.hpp"
#include "rdb_protocol/reqlfdb_config_cache.hpp"
#include "rdb_protocol/serialize_datum_onto_blob.hpp"
#include "rdb_protocol/val.hpp"
#include "utils.hpp"

enum class direction_t {
    forward,
    backward,
};

// rocks_traversal is basically equivalent to depth first traversal.
class rocks_traversal_cb {
public:
    rocks_traversal_cb() { }
    // The implementor must copy out key and value (if they want to use it) before returning.
    virtual continue_bool_t handle_pair(
            std::pair<const char *, size_t> key, std::pair<const char *, size_t> value)
            THROWS_ONLY(interrupted_exc_t) = 0;
protected:
    virtual ~rocks_traversal_cb() {}
    DISABLE_COPYING(rocks_traversal_cb);
};

using transform_variant_t = ql::transform_variant_t;
using terminal_variant_t = ql::terminal_variant_t;

uint64_t rdb_fdb_get_count(FDBTransaction *txn, const namespace_id_t &table_id,
        const signal_t *interruptor) {
    std::string location = rfdb::table_count_location(table_id);
    uint64_t value;
    read_8byte_count(interruptor, txn, location.data(), location.size(), &value);
    return value;
}

void rdb_fdb_get(FDBTransaction *txn, const namespace_id_t &table_id,
        const store_key_t &store_key, point_read_response_t *response,
        const signal_t *interruptor) {
    std::string kv_location = rfdb::table_primary_key(table_id, store_key);
    rfdb::datum_fut value_fut = rfdb::kv_location_get(txn, kv_location);

    optional<std::vector<uint8_t>> value = block_and_read_unserialized_datum(
        txn, std::move(value_fut), interruptor);

    if (!value.has_value()) {
        response->data = ql::datum_t::null();
    } else {
        ql::datum_t datum = datum_deserialize_from_uint8(value->data(), value->size());
        response->data = std::move(datum);
    }
}

class job_data_t {
public:
    job_data_t(ql::env_t *_env,
               const ql::batchspec_t &batchspec,
               const std::vector<transform_variant_t> &_transforms,
               const optional<terminal_variant_t> &_terminal,
               ql::limit_read_last_key last_key,
               sorting_t _sorting,
               require_sindexes_t require_sindex_val)
        : env(_env),
          batcher(make_scoped<ql::batcher_t>(batchspec.to_batcher())),
          sorting(_sorting),
          accumulator(_terminal.has_value()
                      ? ql::make_terminal(*_terminal)
                      : ql::make_append(std::move(last_key),
                                        sorting,
                                        batcher.get(),
                                        require_sindex_val)) {
        for (size_t i = 0; i < _transforms.size(); ++i) {
            transformers.push_back(ql::make_op(_transforms[i]));
        }
        guarantee(transformers.size() == _transforms.size());
    }
    job_data_t(job_data_t &&) = default;

    bool should_send_batch() const {
        return accumulator->should_send_batch();
    }
private:
    friend class fdb_rget_cb;
    friend class fdb_rget_secondary_cb;
    ql::env_t *const env;
    scoped_ptr_t<ql::batcher_t> batcher;
    std::vector<scoped_ptr_t<ql::op_t> > transformers;
    sorting_t sorting;
    scoped_ptr_t<ql::accumulator_t> accumulator;
};

class fdb_rget_cb {
public:
    fdb_rget_cb(
            rget_read_response_t *_response,
            job_data_t &&_job)
            : response(_response), job(std::move(_job)) {
        // TODO: This comment is now probably false...
        // We must disable profiler events for subtasks, because multiple instances
        // of `handle_pair`are going to run in parallel which  would otherwise corrupt
        // the sequence of events in the profiler trace.
        disabler.init(new profile::disabler_t(job.env->trace));
        sampler.init(new profile::sampler_t("Range traversal doc evaluation.",
                                            job.env->trace));
    }

    continue_bool_t handle_pair(
            std::pair<const char *, size_t> keyslice, std::pair<const char *, size_t> value,
            size_t default_copies)
            THROWS_ONLY(interrupted_exc_t) /* TODO: Possibly throws fdb excs? */ {
        //////////////////////////////////////////////////
        // STUFF THAT CAN HAPPEN OUT OF ORDER GOES HERE //
        //////////////////////////////////////////////////
        sampler->new_sample();
        if (boost::get<ql::exc_t>(&response->result) != nullptr) {
            return continue_bool_t::ABORT;
        }
        // Load the key and value.
        store_key_t key(keyslice.second, reinterpret_cast<const uint8_t *>(keyslice.first));
        ql::datum_t val;
        // TODO: Add this stats somehow.
        // Count stats whether or not we deserialize the value
        // io.slice->stats.pm_keys_read.record();
        // io.slice->stats.pm_total_keys_read += 1;
        // We only load the value if we actually use it (`count` does not).
        if (job.accumulator->uses_val() || job.transformers.size() != 0) {
            val = datum_deserialize_from_vec(value.first, value.second);
        }

        // Note: Previously (before converting to use with rocksdb, we had a
        // concurrent_traversal operation that spawned this method in multiple
        // coroutines.  They'd get an order token and then we'd realign with the
        // waiter right here, and proceed to process key/value pairs in order below.)

        // TODO: We could do the rest of this rocks_rget_cb::handle_pair function concurrently.

        // TODO: Can this function still throw interrupted_exc_t?

        ///////////////////////////////////////////////////////
        // STUFF THAT HAS TO HAPPEN IN ORDER GOES BELOW HERE //
        ///////////////////////////////////////////////////////

        try {
            auto lazy_sindex_val = []() -> ql::datum_t {
                return ql::datum_t();
            };

            // Check whether we're outside the sindex range.
            // We only need to check this if we are on the boundary of the sindex range, and
            // the involved keys are truncated.
            size_t copies = default_copies;

            ql::groups_t data = {{ql::datum_t(), ql::datums_t(copies, val)}};

            for (auto it = job.transformers.begin(); it != job.transformers.end(); ++it) {
                (*it)->apply_op(job.env, &data, lazy_sindex_val);
            }
            // We need lots of extra data for the accumulation because we might be
            // accumulating `rget_item_t`s for a batch.
            continue_bool_t cont = job.accumulator->apply_accumulator(job.env, &data, key, lazy_sindex_val);
            return cont;
        } catch (const ql::exc_t &e) {
            response->result = e;
            return continue_bool_t::ABORT;
        } catch (const ql::datum_exc_t &e) {
    #ifndef NDEBUG
            unreachable();
    #else
            response->result = ql::exc_t(e, ql::backtrace_id_t::empty());
            return continue_bool_t::ABORT;
    #endif // NDEBUG
        }

    }

    void finish(continue_bool_t last_cb) THROWS_ONLY(interrupted_exc_t) {
        job.accumulator->finish(last_cb, &response->result);
    }
private:
    rget_read_response_t *const response;
    job_data_t job; // What to do next (stateful).

    // State for internal bookkeeping.
    scoped_ptr_t<profile::disabler_t> disabler;
    scoped_ptr_t<profile::sampler_t> sampler;
};


// See also "struct batch_size_calc".
struct secondary_batch_size_calc {
    explicit secondary_batch_size_calc()
        : first_batch(true), next_recommended_batch(0), last_batch(0) {}
    bool first_batch;
    size_t next_recommended_batch;
    size_t last_batch;

    void note_completed_batch(size_t size) {
        if (first_batch) {
            first_batch = false;
            next_recommended_batch = size;
            last_batch = size;
        } else {
            last_batch = size;
            size_t x = std::max<size_t>(1, std::max<size_t>(next_recommended_batch / 2, last_batch));
            // Grotesque hackish nonsense to avoid some perpetual small-batch scenario.  But
            // this will needlessly sawtooth batch sizes.
            next_recommended_batch = add_rangeclamped<size_t>(x, ceil_divide(x, 32));
        }
    }

    // Caller might need to clamp this by the number of keys.
    optional<size_t> recommended_batch() const {
        return first_batch ? r_nullopt : make_optional(next_recommended_batch);
    }
};

// QQQ: Make rocks_traversal_cb take a vector of results (to amortize overhead)
// TODO: At some point... rename rocks_traversal_cb.

continue_bool_t fdb_traversal_secondary(
        const signal_t *interruptor,
        FDBDatabase *fdb,
        reqlfdb_config_version prior_cv,
        const auth::user_context_t &user_context,
        const namespace_id_t &table_id,
        const table_config_t &table_config,
        const std::string &primary_prefix,
        const std::string &secondary_prefix,
        const key_range_t &rangeparam,
        direction_t direction,
        rocks_traversal_cb *cb) {
    const fdb_bool_t reverse = direction != direction_t::forward;

    // OOO: We are now mixing rethinkdb batching and foundationdb batching.  Instead,
    // use foundationdb batching (fixup old code, yadda yadda, see fdb_traversal_primary
    // comment).

    key_range_t range = rangeparam;

    secondary_batch_size_calc batch_calc;
    for (;;) {
        struct read_op_slug {
            std::vector<std::pair<store_key_t, std::vector<uint8_t>>> key_values;
            int kv_count;
            fdb_bool_t more;
            std::string last_key_view;
            size_t last_batch_size;
        };

        optional<size_t> recommended_batch = batch_calc.recommended_batch();

        uint64_t retry_count = 0;
        read_op_slug slug = perform_read_operation<read_op_slug>(fdb, interruptor, prior_cv, user_context, table_id, table_config,
                [&](const signal_t *interruptor, FDBTransaction *txn, cv_auth_check_fut_read&& cva) {
            const uint64_t count = retry_count++;

            const int limit_param = std::min<size_t>(INT_MAX, std::max<size_t>(1, recommended_batch.value_or(0) >> count));

            // We use MEDIUM on the first run (before we supply a key limit)
            rfdb::secondary_range_fut fut = rfdb::secondary_prefix_get_range(
                txn, secondary_prefix, range.left,
                rfdb::lower_bound::closed,
                range.right.unbounded ? nullptr : &range.right.internal_key,
                limit_param, 0, recommended_batch.has_value() ? FDB_STREAMING_MODE_LARGE : FDB_STREAMING_MODE_MEDIUM,
                0, false, reverse);

            cva.cvc.block_and_check(interruptor);
            cva.auth_fut.block_and_check(interruptor);

            fut.future.block_coro(interruptor);


            const FDBKeyValue *kvs;
            int kv_count;
            fdb_bool_t more;
            fdb_error_t err = fdb_future_get_keyvalue_array(fut.future.fut, &kvs, &kv_count, &more);
            check_for_fdb_transaction(err);

            // The store_key_t is the sindex store key.  Do we actually use it?
            std::vector<std::pair<store_key_t, rfdb::datum_fut>> primary_futs;
            primary_futs.reserve(kv_count);

            // Initialized and used if kv_count > 0.
            key_view last_key_view;

            // Overoptimization: We append/resize to avoid allocations.
            std::string kv_location = primary_prefix;
            for (int i = 0; i < kv_count; ++i) {
                key_view full_key{void_as_uint8(kvs[i].key), kvs[i].key_length};
                key_view store_key = full_key.guarantee_without_prefix(secondary_prefix);
                last_key_view = store_key;
                // Right now sindexes have no value.
                rassert(kvs[i].value_length == 0);

                // TODO: We -could- make extract_primary append to a string.
                store_key_t primary = ql::datum_t::extract_primary(
                    as_char(store_key.data), size_t(store_key.length));

                kv_location += primary.str();
                primary_futs.emplace_back(
                    store_key_t(std::string(as_char(store_key.data), size_t(store_key.length))),
                    rfdb::kv_location_get(txn, kv_location));
                kv_location.resize(primary_prefix.size());
            }

            std::vector<std::pair<store_key_t, std::vector<uint8_t>>> key_values;
            key_values.reserve(kv_count);

            for (auto &pair : primary_futs) {
                optional<std::vector<uint8_t>> value = block_and_read_unserialized_datum(
                    txn, std::move(pair.second), interruptor);
                guarantee(value.has_value());  // TODO: fdb consistency, graceful, msg
                key_values.emplace_back(std::move(pair.first), std::move(*value));
            }

            return read_op_slug{key_values, kv_count, more, last_key_view.to_string(), static_cast<size_t>(kv_count)};
        });
        batch_calc.note_completed_batch(slug.last_batch_size);

        for (size_t i = 0, e = slug.key_values.size(); i < e; ++i) {
            const store_key_t &key = slug.key_values[i].first;
            const std::vector<uint8_t> &value = slug.key_values[i].second;
            // TODO: Make handle_pair take a const uint8_t * -- since it casts the char * back to that.
            continue_bool_t contbool = cb->handle_pair(
                std::make_pair(key.str().data(), key.str().size()),
                std::make_pair(as_char(value.data()), value.size()));
            // OOO: It's bad thinking to abandon the reads we've performed here.  We should consume everything we've read from fdb.
            // OOO: ^^ huh?
            if (contbool == continue_bool_t::ABORT) {
                return continue_bool_t::ABORT;
            }
        }

        if (!slug.more) {
            return continue_bool_t::CONTINUE;
        }
        if (slug.kv_count > 0) {
            if (reverse) {
                // Key is excluded from next read, and right bound is open.
                range.right.internal_key.assign(slug.last_key_view.size(), as_uint8(slug.last_key_view.data()));
                range.right.unbounded = false;
            } else {
                // Key is excluded from next read, so we increment.
                range.left.assign(slug.last_key_view.size(), as_uint8(slug.last_key_view.data()));
                range.left.increment1();
            }
        }
    }

}

void rdb_fdb_rget_snapshot_slice(
        const signal_t *interruptor,
        FDBDatabase *fdb,
        reqlfdb_config_version prior_cv,
        const auth::user_context_t &user_context,
        const namespace_id_t &table_id,
        const table_config_t &table_config,
        const key_range_t &range,
        const optional<std::map<store_key_t, uint64_t> > &primary_keys,
        ql::env_t *ql_env,
        const ql::batchspec_t &batchspec,
        const std::vector<transform_variant_t> &transforms,
        const optional<terminal_variant_t> &terminal,
        sorting_t sorting,
        rget_read_response_t *response_) {

    r_sanity_check(boost::get<ql::exc_t>(&response_->result) == nullptr);
    PROFILE_STARTER_IF_ENABLED(
        ql_env->profile() == profile_bool_t::PROFILE,
        "Do range scan on primary index.",
        ql_env->trace);

    std::string fdb_kv_prefix = rfdb::table_pkey_prefix(table_id);
    if (primary_keys.has_value()) {
        fdb_rget_cb callback(
            response_,
            job_data_t(ql_env,
                       batchspec,
                       transforms,
                       terminal,
                       !reversed(sorting)
                           ? ql::limit_read_last_key(range.left)
                           : ql::limit_read_last_key(key_or_max::from_right_bound(range.right)),
                       sorting,
                       require_sindexes_t::NO));

        continue_bool_t cont = continue_bool_t::CONTINUE;
        // QQQ: Is the sorting actually used when primary_keys.has_value()?  I guess so.

        // Has length of primary_keys.
        std::vector<optional<std::vector<uint8_t>>> unprocessed_values =
            perform_read_operation<std::vector<optional<std::vector<uint8_t>>>>(fdb, interruptor, prior_cv, user_context, table_id, table_config,
                [&](const signal_t *interruptor, FDBTransaction *txn, cv_auth_check_fut_read&& cva) {

            std::vector<rfdb::datum_fut> value_futs;
            value_futs.reserve(primary_keys->size());
            for (auto& pair : *primary_keys) {
                value_futs.push_back(rfdb::kv_location_get(txn, fdb_kv_prefix + rfdb::table_pkey_keystr(pair.first)));
            }

            cva.cvc.block_and_check(interruptor);
            cva.auth_fut.block_and_check(interruptor);

            std::vector<optional<std::vector<uint8_t>>> resp;
            resp.reserve(value_futs.size());
            for (auto& fut : value_futs) {
                resp.push_back(block_and_read_unserialized_datum(
                                txn, std::move(fut), interruptor));
            }

            return resp;
        });

        if (!reversed(sorting)) {
            size_t unprocessed_ix = 0;
            for (auto it = primary_keys->begin(); it != primary_keys->end(); ++it) {
                optional<std::vector<uint8_t>>& unprocessed = unprocessed_values[unprocessed_ix];
                ++unprocessed_ix;

                if (unprocessed.has_value()) {
                    continue_bool_t c = callback.handle_pair(
                            std::make_pair(as_char(it->first.data()), it->first.size()),
                            std::make_pair(as_char(unprocessed->data()), unprocessed->size()),
                            it->second);
                    if (c == continue_bool_t::ABORT) {
                        // If required the superblock will get released further up the stack.
                        cont = continue_bool_t::ABORT;
                        break;
                    }
                }
            }
        } else {
            size_t unprocessed_ix = unprocessed_values.size();
            for (auto it = primary_keys->rbegin(); it != primary_keys->rend(); ++it) {
                --unprocessed_ix;
                optional<std::vector<uint8_t>>& unprocessed = unprocessed_values[unprocessed_ix];

                if (unprocessed.has_value()) {
                    continue_bool_t c = callback.handle_pair(
                            std::make_pair(as_char(it->first.data()), it->first.size()),
                            std::make_pair(as_char(unprocessed->data()), unprocessed->size()),
                            it->second);
                    if (c == continue_bool_t::ABORT) {
                        // If required the superblock will get released further up the stack.
                        cont = continue_bool_t::ABORT;
                        break;
                    }
                }
            }
        }
        callback.finish(cont);

    } else {
        direction_t direction = reversed(sorting) ? direction_t::backward : direction_t::forward;

        const fdb_bool_t reverse = direction != direction_t::forward;
        const fdb_bool_t snapshot = false;

        fdb_rget_cb callback(
            response_,
            job_data_t(ql_env,
                       batchspec,
                       transforms,
                       terminal,
                       !reversed(sorting)
                           ? ql::limit_read_last_key(range.left)
                           : ql::limit_read_last_key(key_or_max::from_right_bound(range.right)),
                       sorting,
                       require_sindexes_t::NO));

        continue_bool_t cont = continue_bool_t::CONTINUE;
        // OOO: We are now mixing rethinkdb batching and foundationdb batching.  Instead,
        // use foundationdb batching (fixup old code commented below and make caller accept
        // a return value which happens when foundationdb returned the end-of-batch.

        rfdb::datum_range_iterator iter = rfdb::primary_prefix_make_iterator(fdb_kv_prefix,
            range.left, range.right.unbounded ? nullptr : &range.right.internal_key,
            snapshot, reverse);

        for (;;) {
            using raw_result = std::pair<std::vector<std::pair<store_key_t, std::vector<uint8_t>>>, bool>;

            rfdb::datum_range_iterator next_iter;

            raw_result result = perform_read_operation<raw_result>(fdb, interruptor, prior_cv, user_context, table_id, table_config,
            [&](const signal_t *interruptor, FDBTransaction *txn, cv_auth_check_fut_read&& cva) {

                // OOO: Avoid deep-copying the iterator (with its partial_document_ field)

                rfdb::datum_range_iterator tmp_iter = iter;
                // TODO: Use streaming mode iterator, or use streaming mode conditional on
                // what kind of transforms/terminals it has.
                raw_result result
                    = tmp_iter.query_and_step(txn, interruptor, FDB_STREAMING_MODE_LARGE);

                cva.cvc.block_and_check(interruptor);
                cva.auth_fut.block_and_check(interruptor);

                next_iter = std::move(tmp_iter);
                return result;
            });
            iter = std::move(next_iter);

            iter.mark_split_across_txns();

            for (const auto &elem : result.first) {
                // TODO: Make handle_pair take a const uint8_t * -- since it casts the char * back to that.
                const size_t copies = 1;
                continue_bool_t contbool = callback.handle_pair(
                    std::make_pair(as_char(elem.first.data()), elem.first.size()),
                    std::make_pair(as_char(elem.second.data()), elem.second.size()),
                    copies);
                if (contbool == continue_bool_t::ABORT) {
                    cont = continue_bool_t::ABORT;
                    goto end_txn;
                }
            }

            if (!result.second) {
                // It's already CONTINUE, just adding clarity.
                cont = continue_bool_t::CONTINUE;
                // Could be break; but we want the same destination as the other goto.
                goto end_txn;
            }
        }
        end_txn:
        ;
        callback.finish(cont);
    }
}

// TODO: Rename this, because it doesn't "acquire" anything.
void acquire_fdb_sindex_for_read(
        const table_config_t &table_config,
        const std::string &rget_table_name,
        const std::string &sindex_name,
        sindex_disk_info_t *sindex_info_out,
        sindex_id_t *sindex_id_out) {
    auto sindexes_it = table_config.sindexes.find(sindex_name);
    if (sindexes_it == table_config.sindexes.end()) {
        // TODO: Dedup index not found error messages.
        rfail_toplevel(ql::base_exc_t::OP_FAILED,
                "Index `%s` was not found on table `%s`.",
                          sindex_name.c_str(), rget_table_name.c_str());
    }

    if (!sindexes_it->second.creation_task_or_nil.value.is_nil()) {
        // TODO: Dedup with or remove sindex_not_ready_exc_t.
        std::string msg = strprintf("Index `%s` on table `%s` "
                         "was accessed before its construction was finished.",
                         sindex_name.c_str(),
                         rget_table_name.c_str());
        throw ql::exc_t(
            ql::base_exc_t::OP_FAILED, msg, ql::backtrace_id_t::empty());
    }

    *sindex_info_out = rfdb::sindex_config_to_disk_info(sindexes_it->second.config);
    *sindex_id_out = sindexes_it->second.sindex_id;
}

// TODO: Rename to rget_sindex_data_t when that's gone.
class rget_fdb_sindex_data_t {
public:
    rget_fdb_sindex_data_t(key_range_t _pkey_range,
                       ql::datumspec_t _datumspec,
                       key_range_t *_active_region_range_inout,
                       reql_version_t wire_func_reql_version,
                       const ql::deterministic_func &wire_func,
                       sindex_multi_bool_t _multi)
        : pkey_range(std::move(_pkey_range)),
          datumspec(std::move(_datumspec)),
          active_region_range_inout(_active_region_range_inout),
          func_reql_version(wire_func_reql_version),
          func(wire_func.det_func.compile()),
          multi(_multi) {
        datumspec.visit<void>(
            [&](const ql::datum_range_t &r) {
                lbound_trunc_key = r.get_left_bound_trunc_key();
                rbound_trunc_key = r.get_right_bound_trunc_key();
            },
            [](const std::map<ql::datum_t, uint64_t> &) { });
    }
private:
    friend class fdb_rget_secondary_cb;
    const key_range_t pkey_range;
    const ql::datumspec_t datumspec;
    key_range_t *active_region_range_inout;
    const reql_version_t func_reql_version;
    const counted_t<const ql::func_t> func;
    const sindex_multi_bool_t multi;
    // The (truncated) boundary keys for the datum range stored in `datumspec`.
    std::string lbound_trunc_key;
    std::string rbound_trunc_key;
};

class fdb_rget_secondary_cb {
public:
    fdb_rget_secondary_cb(
            rget_read_response_t *_response,
            job_data_t &&_job,
            rget_fdb_sindex_data_t &&_sindex_data)
        : response(_response),
          job(std::move(_job)),
          sindex_data(std::move(_sindex_data)),
          bad_init(false) {

        // Secondary index functions are deterministic (so no need for an
        // rdb_context_t) and evaluated in a pristine environment (without global
        // optargs).
        sindex_env.init(new ql::env_t(job.env->interruptor,
                                      ql::return_empty_normal_batches_t::NO,
                                      sindex_data.func_reql_version));

        // We must disable profiler events for subtasks, because multiple instances
        // of `handle_pair`are going to run in parallel which  would otherwise corrupt
        // the sequence of events in the profiler trace.
        disabler.init(new profile::disabler_t(job.env->trace));
        sampler.init(new profile::sampler_t("Range traversal doc evaluation.",
                                            job.env->trace));
    }


    continue_bool_t handle_pair(
            std::pair<const char *, size_t> keyslice, std::pair<const char *, size_t> value,
            size_t default_copies,
            const std::string &skey_left)
            THROWS_ONLY(interrupted_exc_t) {
        // TODO: This seems really complicated; think about fdb sindex key
        // representation at some point.


        //////////////////////////////////////////////////
        // STUFF THAT CAN HAPPEN OUT OF ORDER GOES HERE //
        //////////////////////////////////////////////////
        sampler->new_sample();
        if (bad_init || boost::get<ql::exc_t>(&response->result) != nullptr) {
            return continue_bool_t::ABORT;
        }
        // Load the key and value.
        store_key_t key(keyslice.second, reinterpret_cast<const uint8_t *>(keyslice.first));
        if (!sindex_data.pkey_range.contains_key(ql::datum_t::extract_primary(key))) {
            return continue_bool_t::CONTINUE;
        }
        ql::datum_t val;
        // Count stats whether or not we deserialize the value
        // TODO: Add these stats back.
        // io.slice->stats.pm_keys_read.record();
        // io.slice->stats.pm_total_keys_read += 1;
        // We always load the value because secondary index uses it...
        val = datum_deserialize_from_vec(value.first, value.second);

        // Note: Previously (before converting to use with rocksdb, we had a
        // concurrent_traversal operation that spawned this method in multiple
        // coroutines.  They'd get an order token and then we'd realign with the
        // waiter right here, and proceed to process key/value pairs in order below.)

        // TODO: We could do the rest of this rocks_rget_secondary_cb::handle_pair function concurrently.

        // TODO: Can this function still throw interrupted_exc_t?

        ///////////////////////////////////////////////////////
        // STUFF THAT HAS TO HAPPEN IN ORDER GOES BELOW HERE //
        ///////////////////////////////////////////////////////

        // If the sindex portion of the key is long enough that it might be >= the
        // length of a truncated sindex, we need to rember the key so we can make
        // sure not to stop in the middle of a sindex range where some of the values
        // are out of order because of truncation.
        bool remember_key_for_sindex_batching =
            (ql::datum_t::extract_secondary(key_to_unescaped_str(key)).size()
               >= ql::datum_t::max_trunc_size());
        if (last_truncated_secondary_for_abort.has_value()) {
            std::string cur_truncated_secondary =
                ql::datum_t::extract_truncated_secondary(key_to_unescaped_str(key));
            if (cur_truncated_secondary != *last_truncated_secondary_for_abort) {
                // The semantics here are that we're returning the "last considered
                // key", which we set specially here to preserve the invariant that
                // unsharding either consumes all rows with a particular truncated
                // sindex value or none of them.
                ql::limit_read_last_key stop_key;
                if (!reversed(job.sorting)) {
                    stop_key.raw_key = key_or_max(store_key_t(cur_truncated_secondary));
                } else {
                    stop_key.raw_key = key_or_max(store_key_t(*last_truncated_secondary_for_abort));
                }
                stop_key.is_decremented = true;
                job.accumulator->stop_at_boundary(std::move(stop_key));
                return continue_bool_t::ABORT;
            }
        }

        try {
            // Update the active region range.
            if (!reversed(job.sorting)) {
                if (sindex_data.active_region_range_inout->left < key) {
                    sindex_data.active_region_range_inout->left = key;
                    sindex_data.active_region_range_inout->left.increment1();
                }
            } else {
                if (sindex_data.active_region_range_inout->right.right_of_key(key)) {
                    sindex_data.active_region_range_inout->right =
                        key_range_t::right_bound_t(key);
                }
            }

            // There are certain transformations and accumulators that need the
            // secondary index value, though many don't. We don't want to compute
            // it if we don't end up needing it, because that would be expensive.
            // So we provide a function that computes the secondary index value
            // lazily the first time it's called.
            ql::datum_t sindex_val_cache; // an empty `datum_t` until initialized
            auto lazy_sindex_val = [&]() -> ql::datum_t {
                if (!sindex_val_cache.has()) {
                    sindex_val_cache =
                        sindex_data.func->call(sindex_env.get(), val)->as_datum(sindex_env.get());
                    if (sindex_data.multi == sindex_multi_bool_t::MULTI
                        && sindex_val_cache.get_type() == ql::datum_t::R_ARRAY) {
                        uint64_t tag = ql::datum_t::extract_tag(key).get();
                        sindex_val_cache = sindex_val_cache.get(tag, ql::NOTHROW);
                        guarantee(sindex_val_cache.has());
                    }
                }
                return sindex_val_cache;
            };

            // Check whether we're outside the sindex range.
            // We only need to check this if we are on the boundary of the sindex range, and
            // the involved keys are truncated.
            size_t copies = default_copies;
            if (true) {
                /* Here's an attempt at explaining the different case distinctions handled in
                   this check (for the left bound; the right bound check is similar):
                   The case distinctions are as follows:
                   1. left_bound_is_truncated
                    If the left bound key had to be truncated, we first compare the prefix of
                    the current secondary key (skey_current), and the left bound key.
                    The comparison cannot be -1, because that would mean that we computed the
                    traversal key range incorrectly in the first place (there's no need to
                    consider keys that are *smaller* than the left bound).
                    If the comparison is 1, the current key's secondary part is larger than
                    the left bound, and we know that the corresponding datum_t value must
                    also be larger than the datum_t corresponding to the left bound.
                    Finally, since the left bound is truncated, the comparison can determine
                    that the prefix is equal for values in the btree with corresponding index
                    values that are either left of the bound (but match in the truncated
                    prefix), at the bound (which we want to include only if the left bound is
                    closed), or right of the bound (which we always want to include, as far
                    as the left bound id concerned). We can't determine which case we have,
                    by looking only at the keys. Hence we must check the number of copies for
                    `cmp == 0`. The only exception is if the current key was actually not
                    truncated, in which case we know that it will actually be smaller than
                    the left bound (that's encoded in line 825).
                   2. !left_bound_is_truncated && left_bound is closed
                    If the bound wasn't truncated, we know that the traversal range will not
                    include any values which are smaller than the left bound. Hence we can
                    skip the check for whether the sindex value is actually in the datum
                    range.
                   3. !left_bound_is_truncated && left_bound is open
                    In contrast, if the left bound is open, we compare the left bound and
                    current key. If they have the same size and their contents compare equal,
                    we actually know that they are outside the range and could set the number
                    of copies to 0. We do the slightly less optimal but simpler thing and
                    just check the number of copies in this case, so that we can share the
                    code path with case 1. */
                const size_t max_trunc_size = ql::datum_t::max_trunc_size();
                sindex_data.datumspec.visit<void>(
                [&](const ql::datum_range_t &r) {
                    bool must_check_copies = false;
                    std::string skey_current =
                        ql::datum_t::extract_truncated_secondary(key_to_unescaped_str(key));
                    const bool left_bound_is_truncated =
                        sindex_data.lbound_trunc_key.size() == max_trunc_size;
                    if (left_bound_is_truncated
                        || r.left_bound_type == key_range_t::bound_t::open) {
                        int cmp = memcmp(
                            skey_current.data(),
                            sindex_data.lbound_trunc_key.data(),
                            std::min<size_t>(skey_current.size(),
                                             sindex_data.lbound_trunc_key.size()));
                        if (skey_current.size() < sindex_data.lbound_trunc_key.size()) {
                            guarantee(cmp != 0);
                        }
                        guarantee(cmp >= 0);
                        if (cmp == 0
                            && skey_current.size() == sindex_data.lbound_trunc_key.size()) {
                            must_check_copies = true;
                        }
                    }
                    if (!must_check_copies) {
                        const bool right_bound_is_truncated =
                            sindex_data.rbound_trunc_key.size() == max_trunc_size;
                        if (right_bound_is_truncated
                            || r.right_bound_type == key_range_t::bound_t::open) {
                            int cmp = memcmp(
                                skey_current.data(),
                                sindex_data.rbound_trunc_key.data(),
                                std::min<size_t>(skey_current.size(),
                                                 sindex_data.rbound_trunc_key.size()));
                            if (skey_current.size() > sindex_data.rbound_trunc_key.size()) {
                                guarantee(cmp != 0);
                            }
                            guarantee(cmp <= 0);
                            if (cmp == 0
                                && skey_current.size() == sindex_data.rbound_trunc_key.size()) {
                                must_check_copies = true;
                            }
                        }
                    }
                    if (must_check_copies) {
                        copies = sindex_data.datumspec.copies(lazy_sindex_val());
                    } else {
                        copies = 1;
                    }
                },
                [&](const std::map<ql::datum_t, uint64_t> &) {
                    std::string skey_current =
                        ql::datum_t::extract_secondary(key_to_unescaped_str(key));
                    const bool skey_current_is_truncated =
                        skey_current.size() >= max_trunc_size;
                    const bool skey_left_is_truncated = skey_left.size() >= max_trunc_size;

                    if (skey_current_is_truncated || skey_left_is_truncated) {
                        copies = sindex_data.datumspec.copies(lazy_sindex_val());
                    } else if (skey_left != skey_current) {
                        copies = 0;
                    }
                });
                if (copies == 0) {
                    return continue_bool_t::CONTINUE;
                }
            }

            ql::groups_t data = {{ql::datum_t(), ql::datums_t(copies, val)}};

            for (auto it = job.transformers.begin(); it != job.transformers.end(); ++it) {
                (*it)->apply_op(job.env, &data, lazy_sindex_val);
            }
            // We need lots of extra data for the accumulation because we might be
            // accumulating `rget_item_t`s for a batch.
            continue_bool_t cont = job.accumulator->apply_accumulator(job.env, &data, key, lazy_sindex_val);
            if (remember_key_for_sindex_batching) {
                if (cont == continue_bool_t::ABORT) {
                    last_truncated_secondary_for_abort.set(
                        ql::datum_t::extract_truncated_secondary(key_to_unescaped_str(key)));
                }
                return continue_bool_t::CONTINUE;
            } else {
                return cont;
            }
        } catch (const ql::exc_t &e) {
            response->result = e;
            return continue_bool_t::ABORT;
        } catch (const ql::datum_exc_t &e) {
    #ifndef NDEBUG
            unreachable();
    #else
            response->result = ql::exc_t(e, ql::backtrace_id_t::empty());
            return continue_bool_t::ABORT;
    #endif // NDEBUG
        }

    }

    void finish(continue_bool_t last_cb) THROWS_ONLY(interrupted_exc_t) {
        job.accumulator->finish(last_cb, &response->result);
    }

private:
    rget_read_response_t *response;
    job_data_t job; // What to do next (stateful).
    const rget_fdb_sindex_data_t sindex_data; // Optional sindex information.

    scoped_ptr_t<ql::env_t> sindex_env;

    // State for internal bookkeeping.
    bool bad_init;
    optional<std::string> last_truncated_secondary_for_abort;
    scoped_ptr_t<profile::disabler_t> disabler;
    scoped_ptr_t<profile::sampler_t> sampler;
};

class fdb_rget_secondary_cb_wrapper : public rocks_traversal_cb {
public:
    fdb_rget_secondary_cb_wrapper(
            fdb_rget_secondary_cb *_cb,
            size_t _copies,
            std::string _skey_left)
        : cb(_cb), copies(_copies), skey_left(std::move(_skey_left)) { }
    virtual continue_bool_t handle_pair(
        std::pair<const char *, size_t> key, std::pair<const char *, size_t> value)
        THROWS_ONLY(interrupted_exc_t) {
        return cb->handle_pair(
            key, value,
            copies,
            skey_left);
    }
private:
    fdb_rget_secondary_cb *cb;
    size_t copies;
    std::string skey_left;
    DISABLE_COPYING(fdb_rget_secondary_cb_wrapper);
};


void rdb_fdb_rget_secondary_snapshot_slice(
        const signal_t *interruptor,
        FDBDatabase *fdb,
        reqlfdb_config_version prior_cv,
        const auth::user_context_t &user_context,
        const namespace_id_t &table_id,
        const table_config_t &table_config,
        const sindex_id_t &sindex_id,
        const ql::datumspec_t &datumspec,
        const key_range_t &sindex_region_range,
        ql::env_t *ql_env,
        const ql::batchspec_t &batchspec,
        const std::vector<transform_variant_t> &transforms,
        const optional<terminal_variant_t> &terminal,
        const key_range_t &pk_range,
        sorting_t sorting,
        require_sindexes_t require_sindex_val,
        const sindex_disk_info_t &sindex_info,
        rget_read_response_t *response) {
    r_sanity_check(boost::get<ql::exc_t>(&response->result) == nullptr);
    guarantee(sindex_info.geo == sindex_geo_bool_t::REGULAR);
    PROFILE_STARTER_IF_ENABLED(
        ql_env->profile() == profile_bool_t::PROFILE,
        "Do range scan on secondary index.",
        ql_env->trace);

    const reql_version_t sindex_func_reql_version =
        sindex_info.mapping_version_info.latest_compatible_reql_version;

    key_range_t active_region_range = sindex_region_range;
    fdb_rget_secondary_cb callback(
        response,
        job_data_t(ql_env,
                   batchspec,
                   transforms,
                   terminal,
                   !reversed(sorting)
                       ? ql::limit_read_last_key(sindex_region_range.left)
                       : ql::limit_read_last_key(key_or_max::from_right_bound(sindex_region_range.right)),
                   sorting,
                   require_sindex_val),
        rget_fdb_sindex_data_t(
            pk_range,
            datumspec,
            &active_region_range,
            sindex_func_reql_version,
            sindex_info.mapping,
            sindex_info.multi));

    std::string kv_prefix = rfdb::table_index_prefix(table_id, sindex_id);
    std::string pkey_prefix = rfdb::table_pkey_prefix(table_id);

    direction_t direction = reversed(sorting) ? direction_t::backward : direction_t::forward;
    auto cb = [&](const std::pair<ql::datum_range_t, uint64_t> &pair, UNUSED bool is_last) {
        key_range_t sindex_keyrange = pair.first.to_sindex_keyrange();
        fdb_rget_secondary_cb_wrapper wrapper(
            &callback,
            pair.second,
            key_to_unescaped_str(sindex_keyrange.left));
        key_range_t active_range = active_region_range.intersection(sindex_keyrange);
        // This can happen sometimes with truncated keys.
        if (active_range.is_empty()) {
            return continue_bool_t::CONTINUE;
        }
        return fdb_traversal_secondary(interruptor, fdb,
            prior_cv, user_context, table_id, table_config,
            pkey_prefix, kv_prefix,
            active_range, direction, &wrapper);
    };
    continue_bool_t cont = datumspec.iter(sorting, cb);
    // TODO: See if anybody else calls datumspec.iter, can we remove is_last parameter.
    callback.finish(cont);
}


void do_fdb_snap_read(
        const signal_t *interruptor,
        FDBDatabase *fdb,
        reqlfdb_config_version prior_cv,
        const auth::user_context_t &user_context,
        const namespace_id_t &table_id,
        const table_config_t &table_config,
        ql::env_t *env,
        const rget_read_t &rget,
        rget_read_response_t *res) {

    if (!rget.sindex.has_value()) {
        rdb_fdb_rget_snapshot_slice(
            interruptor,
            fdb,
            prior_cv,
            user_context,
            table_id,
            table_config,
            rget.region,
            rget.primary_keys,
            env,
            rget.batchspec,
            rget.transforms,
            rget.terminal,
            rget.sorting,
            res);
    } else {
        // rget using a secondary index
        try {
            // TODO: What's rget.table_name?  The table's display_name?
            sindex_disk_info_t sindex_info;
            sindex_id_t sindex_id;
            acquire_fdb_sindex_for_read(table_config, rget.table_name, rget.sindex->id,
                                        &sindex_info, &sindex_id);

            key_range_t sindex_range;
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

            rdb_fdb_rget_secondary_snapshot_slice(
                interruptor,
                fdb,
                prior_cv,
                user_context,
                table_id,
                table_config,
                sindex_id,
                rget.sindex->datumspec,
                sindex_range,
                env,
                rget.batchspec,
                rget.transforms,
                rget.terminal,
                rget.region,
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

void rdb_fdb_get_nearest_slice(
        const signal_t *interruptor,
        FDBDatabase *fdb,
        reqlfdb_config_version prior_cv,
        const auth::user_context_t &user_context,
        const namespace_id_t &table_id,
        const table_config_t &table_config,
        const sindex_id_t &sindex_id,
        const lon_lat_point_t &center,
        double max_dist,
        uint64_t max_results,
        const ellipsoid_spec_t &geo_system,
        ql::env_t *ql_env,
        const sindex_disk_info_t &sindex_info,
        nearest_geo_read_response_t *response) {

    guarantee(sindex_info.geo == sindex_geo_bool_t::GEO);
    PROFILE_STARTER_IF_ENABLED(
        ql_env->profile() == profile_bool_t::PROFILE,
        "Do nearest traversal on geospatial index.",
        ql_env->trace);

    const reql_version_t sindex_func_reql_version =
        sindex_info.mapping_version_info.latest_compatible_reql_version;

    // TODO (daniel): Instead of calling this multiple times until we are done,
    //   results should be streamed lazily. Also, even if we don't do that,
    //   the copying of the result we do here is bad.
    nearest_traversal_state_t state(center, max_results, max_dist, geo_system);
    response->results_or_error = nearest_geo_read_response_t::result_t();
    do {
        nearest_geo_read_response_t partial_response;
        try {
            nearest_traversal_cb_t callback(
                geo_sindex_data_t(sindex_info.mapping,
                                  sindex_func_reql_version, sindex_info.multi),
                ql_env,
                &state);
            geo_fdb_traversal(
                interruptor, fdb, prior_cv, user_context, table_id, table_config, sindex_id, key_range_t::universe(), &callback);
            callback.finish(&partial_response);
        } catch (const geo_exception_t &e) {
            partial_response.results_or_error =
                ql::exc_t(ql::base_exc_t::LOGIC, e.what(),
                          ql::backtrace_id_t::empty());
        }
        if (boost::get<ql::exc_t>(&partial_response.results_or_error)) {
            response->results_or_error = partial_response.results_or_error;
            return;
        } else {
            auto partial_res = boost::get<nearest_geo_read_response_t::result_t>(
                &partial_response.results_or_error);
            guarantee(partial_res != nullptr);
            auto full_res = boost::get<nearest_geo_read_response_t::result_t>(
                &response->results_or_error);
            std::move(partial_res->begin(), partial_res->end(),
                      std::back_inserter(*full_res));
        }
    } while (state.proceed_to_next_batch() == continue_bool_t::CONTINUE);
}

void rdb_fdb_get_intersecting_slice(
        const signal_t *interruptor,
        FDBDatabase *fdb,
        reqlfdb_config_version prior_cv,
        const auth::user_context_t &user_context,
        const namespace_id_t &table_id,
        const table_config_t &table_config,
        const sindex_id_t &sindex_id,
        const ql::datum_t &query_geometry,
        const key_range_t &sindex_range,
        ql::env_t *ql_env,
        const ql::batchspec_t &batchspec,
        const std::vector<ql::transform_variant_t> &transforms,
        const optional<ql::terminal_variant_t> &terminal,
        const sindex_disk_info_t &sindex_info,
        is_stamp_read_t is_stamp_read,
        rget_read_response_t *response) {
    guarantee(query_geometry.has());

    guarantee(sindex_info.geo == sindex_geo_bool_t::GEO);
    PROFILE_STARTER_IF_ENABLED(
        ql_env->profile() == profile_bool_t::PROFILE,
        "Do intersection scan on geospatial index.",
        ql_env->trace);

    const reql_version_t sindex_func_reql_version =
        sindex_info.mapping_version_info.latest_compatible_reql_version;
    collect_all_geo_intersecting_cb_t callback(
        geo_job_data_t(ql_env,
                       // The sorting is never `DESCENDING`, so this is always right.
                       ql::limit_read_last_key(sindex_range.left),
                       batchspec,
                       transforms,
                       terminal,
                       is_stamp_read),
        geo_sindex_data_t(sindex_info.mapping,
                          sindex_func_reql_version, sindex_info.multi),
        query_geometry,
        response);



    continue_bool_t cont = geo_fdb_traversal(
            interruptor, fdb, prior_cv, user_context, table_id, table_config, sindex_id, sindex_range, &callback);
    callback.finish(cont);
}


struct fdb_read_visitor : public boost::static_visitor<void> {
#if RDB_CF
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
#endif  // RDB_CF

    void operator()(const count_read_t &) {
        *response_ = perform_read_operation<read_response_t>(fdb_, interruptor, prior_cv_, *user_context_, table_id_, *table_config_,
            [&](const signal_t *interruptor, FDBTransaction *txn, cv_auth_check_fut_read&& cva) {
                read_response_t ret;
                ret.response = count_read_response_t();
                count_read_response_t *res = boost::get<count_read_response_t>(&ret.response);
                // TODO: Do cva roundtrip concurrently with rdb_fdb_get_count's roundtrip.
                cva.cvc.block_and_check(interruptor);
                cva.auth_fut.block_and_check(interruptor);
                res->value = rdb_fdb_get_count(txn, table_id_, interruptor);
                return ret;
            });
    }

    // Dedups code between our operator() and apply_point_read.
    static void do_point_read(FDBTransaction *txn, cv_check_fut *cvc, const namespace_id_t &table_id,
            const store_key_t &pkey, read_response_t *response, const signal_t *interruptor) {
        response->response = point_read_response_t();
        point_read_response_t *res =
            boost::get<point_read_response_t>(&response->response);
        rdb_fdb_get(txn, table_id, pkey, res, interruptor);
        cvc->block_and_check(interruptor);
    }

    void operator()(const point_read_t &get) {
        *response_ = perform_read_operation<read_response_t>(fdb_, interruptor, prior_cv_, *user_context_, table_id_, *table_config_,
            [&](const signal_t *interruptor, FDBTransaction *txn, cv_auth_check_fut_read&& cva) {
                read_response_t ret;
                do_point_read(txn, &cva.cvc, table_id_, get.key, &ret, interruptor);
                cva.auth_fut.block_and_check(interruptor);
                return ret;
            });
    }

    void operator()(const intersecting_geo_read_t &geo_read) {
        response_->response = rget_read_response_t();
        rget_read_response_t *res =
            boost::get<rget_read_response_t>(&response_->response);

#if RDB_CF
        guarantee(!geo_read.stamp.has_value());  // TODO: Changefeeds not supported.
#endif

        // TODO: We construct this kind of early.
        ql::env_t ql_env(
            ctx_,    // QQQ: Do the geo read's transforms/terminal code have to pass some non-deterministic test?  We might need a ctx.
            ql::return_empty_normal_batches_t::NO,
            interruptor,
            geo_read.serializable_env,
            trace_);


        sindex_disk_info_t sindex_info;
        sindex_id_t sindex_id;
        try {
            acquire_fdb_sindex_for_read(
                    *table_config_,
                    geo_read.table_name,
                    geo_read.sindex.id,
                    &sindex_info,
                    &sindex_id);
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

        // QQQ: Do we have sindex_rangespec with region?  It mentions sharding.  Maybe it only got initialized with a shard operation?  Look at what initializes it.
        guarantee(geo_read.sindex.region.has_value());
        rdb_fdb_get_intersecting_slice(
            interruptor,
            fdb_,
            prior_cv_,
            *user_context_,
            table_id_,
            *table_config_,
            sindex_id,
            geo_read.query_geometry,
            *geo_read.sindex.region,
            &ql_env,
            geo_read.batchspec,
            geo_read.transforms,
            geo_read.terminal,
            sindex_info,
#if RDB_CF
            geo_read.stamp ? is_stamp_read_t::YES : is_stamp_read_t::NO,
#else
            is_stamp_read_t::NO,
#endif
            res);
    }

    void operator()(const nearest_geo_read_t &geo_read) {
        response_->response = nearest_geo_read_response_t();
        nearest_geo_read_response_t *res =
            boost::get<nearest_geo_read_response_t>(&response_->response);

        ql::env_t ql_env(
            ctx_,    // QQQ: Do the geo read's transforms/terminal code have to pass some non-deterministic test?  We might need a ctx.
            ql::return_empty_normal_batches_t::NO,
            interruptor,
            geo_read.serializable_env,
            trace_);

        sindex_disk_info_t sindex_info;
        sindex_id_t sindex_id;
        try {
            acquire_fdb_sindex_for_read(
                *table_config_,
                geo_read.table_name,  // TODO: Wtf is this field?  display_name()?
                geo_read.sindex_id,  // TODO: Rename to sindex_name.
                &sindex_info,
                &sindex_id);
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

        rdb_fdb_get_nearest_slice(
            interruptor,
            fdb_,
            prior_cv_,
            *user_context_,
            table_id_,
            *table_config_,
            sindex_id,
            geo_read.center,
            geo_read.max_dist,
            geo_read.max_results,
            geo_read.geo_system,
            &ql_env,
            sindex_info,
            res);
    }

    void operator()(const rget_read_t &rget) {

        response_->response = rget_read_response_t();
        auto *res = boost::get<rget_read_response_t>(&response_->response);

#if RDB_CF
        if (rget.stamp) {
            // QQQ: Remove rget stamp field
            crash("rgets with stamp not supported");
        }
#endif  // RDB_CF

        if (rget.transforms.size() != 0 || rget.terminal.has_value()) {
            // This asserts that the optargs have been initialized.  (There is always
            // a 'db' optarg.)  We have the same assertion in
            // rdb_r_unshard_visitor_t.
            rassert(rget.serializable_env.global_optargs.has_optarg("db"));
        }

        // QQQ: When this is all done with we might not even construct a pristine env in read
        // and write code, except for sindex/write hook stuff.
        ql::env_t ql_env(
            ctx_,  // QQQ: Do the rget's transforms/terminal code have to pass some non-deterministic test?  We might need a ctx.  It's commented as "lazy" so it might be.
            ql::return_empty_normal_batches_t::NO,
            interruptor,
            rget.serializable_env,
            trace_);

        do_fdb_snap_read(interruptor, fdb_, prior_cv_, *user_context_, table_id_, *table_config_, &ql_env, rget, res);
    }

    void operator()(const dummy_read_t &) {
        response_->response = dummy_read_response_t();
        auto *res_ = boost::get<dummy_read_response_t>(&response_->response);
        // We do need to check user auth before doing a dummy_read_t (if only to be consistent with prior behavior).
        *res_ = perform_read_operation<dummy_read_response_t>(fdb_, interruptor, prior_cv_, *user_context_, table_id_, *table_config_,
            [&](UNUSED const signal_t *interruptor, UNUSED FDBTransaction *txn, cv_auth_check_fut_read&& cva) {
                cva.cvc.block_and_check(interruptor);
                cva.auth_fut.block_and_check(interruptor);
                return dummy_read_response_t();
            });
    }

#if RDB_CF
    // TODO: Remove this.
    template <class T>
    void operator()(const T&) {
        crash("Unimplemented read op for fdb");
    }
#endif

    fdb_read_visitor(const signal_t *_interruptor,
            FDBDatabase *fdb,
            rdb_context_t *ctx,
            reqlfdb_config_version prior_cv,
            const auth::user_context_t *user_context,
            const namespace_id_t &_table_id,
            const table_config_t *_table_config,
            profile::trace_t *trace_or_null,
            read_response_t *_response) :
        interruptor(_interruptor),
        fdb_(fdb),
        ctx_(ctx),
        prior_cv_(prior_cv),
        user_context_(user_context),
        table_id_(_table_id),
        table_config_(_table_config),
        trace_(trace_or_null),
        response_(_response) {}

private:
    const signal_t *const interruptor;
    FDBDatabase *const fdb_;
    rdb_context_t *const ctx_;
    const reqlfdb_config_version prior_cv_;
    const auth::user_context_t *const user_context_;
    const namespace_id_t table_id_;
    const table_config_t *const table_config_;
    profile::trace_t *const trace_;  // can be null
    read_response_t *const response_;

    DISABLE_COPYING(fdb_read_visitor);
};

read_response_t apply_read(FDBDatabase *fdb,
        rdb_context_t *ctx,
        reqlfdb_config_version prior_cv,
        const auth::user_context_t &user_context,
        const namespace_id_t &table_id,
        const table_config_t &table_config,
        const read_t &_read,
        const signal_t *interruptor) {

    // TODO: Move trace stuff out of this?  Back to caller?  Does apply_point_read ever even have profile turned on?
    // Some code duplication with apply_point_read w.r.t. trace logic.
    scoped_ptr_t<profile::trace_t> trace = ql::maybe_make_profile_trace(_read.profile);

    read_response_t ret;
    {
        PROFILE_STARTER_IF_ENABLED(
                _read.profile == profile_bool_t::PROFILE, "Perform read on shard.", trace);

        fdb_read_visitor v(interruptor, fdb, ctx, prior_cv, &user_context, table_id, &table_config, trace.get_or_null(), &ret);
        boost::apply_visitor(v, _read.read);
    }

    if (trace.has()) {
        ret.event_log = std::move(*trace).extract_event_log();
    }

    // (Taken from store_t::protocol_read.)
    // This is a tad hacky, this just adds a stop event to signal the end of the
    // parallel task.

    // TODO: Is this is the right thing to do if profiling's not enabled?
    ret.event_log.push_back(profile::stop_t());

    return ret;
}

read_response_t apply_point_read(FDBTransaction *txn,
        cv_check_fut &&cvc,
        const namespace_id_t &table_id,
        const store_key_t &pkey,
        const profile_bool_t profile,
        const signal_t *interruptor) THROWS_ONLY(
            interrupted_exc_t, cannot_perform_query_exc_t,
            provisional_assumption_exception)
{
    // Some code duplication with apply_read w.r.t. trace logic.
    scoped_ptr_t<profile::trace_t> trace = ql::maybe_make_profile_trace(profile);
    read_response_t response;
    {
        PROFILE_STARTER_IF_ENABLED(
            profile == profile_bool_t::PROFILE, "Perform read on shard.", trace);

        fdb_read_visitor::do_point_read(txn, &cvc, table_id, pkey, &response, interruptor);
    }

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

