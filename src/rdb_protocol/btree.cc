// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/btree.hpp"

#include <algorithm>
#include <functional>
#include <iterator>
#include <set>
#include <string>
#include <vector>

#include "btree/concurrent_traversal.hpp"
#include "btree/operations.hpp"
#include "btree/reql_specific.hpp"
#include "btree/operations.hpp"
#include "concurrency/coro_pool.hpp"
#include "concurrency/new_mutex.hpp"
#include "concurrency/queue/unlimited_fifo.hpp"
#include "containers/archive/boost_types.hpp"
#include "containers/archive/buffer_group_stream.hpp"
#include "containers/archive/string_stream.hpp"
#include "containers/scoped.hpp"
#include "rdb_protocol/geo/exceptions.hpp"
#include "rdb_protocol/geo/indexing.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/geo_traversal.hpp"
#include "rdb_protocol/pseudo_geometry.hpp"
#include "rdb_protocol/serialize_datum_onto_blob.hpp"
#include "rdb_protocol/shards.hpp"
#include "rdb_protocol/table_common.hpp"
#include "rockstore/store.hpp"
#include "rockstore/rockshard.hpp"

#include "debug.hpp"


ql::serialization_result_t datum_serialize_to_string(const ql::datum_t &datum, std::string *out) {
    // TODO: We can avoid double-copying or something, because the write_message_t does
    // get preallocated to one buffer, we can use a rocksdb::Slice.
    write_message_t wm;
    ql::serialization_result_t res =
        datum_serialize(&wm, datum, ql::check_datum_serialization_errors_t::YES);
    if (bad(res)) {
        return res;
    }
    string_stream_t stream;
    int write_res = send_write_message(&stream, &wm);
    guarantee(write_res == 0);
    *out = std::move(stream.str());
    return res;
}

ql::datum_t btree_batched_replacer_t::apply_write_hook(
    const datum_string_t &pkey,
    const ql::datum_t &d,
    const ql::datum_t &res_,
    const ql::datum_t &write_timestamp,
    const counted_t<const ql::func_t> &write_hook) const {
    ql::datum_t res = res_;
    if (write_hook.has()) {
        ql::datum_t primary_key;
        if (res.get_type() != ql::datum_t::type_t::R_NULL) {
            primary_key = res.get_field(pkey, ql::throw_bool_t::NOTHROW);
        } else if (d.get_type() != ql::datum_t::type_t::R_NULL) {
            primary_key = d.get_field(pkey, ql::throw_bool_t::NOTHROW);
        }
        if (!primary_key.has()) {
            primary_key = ql::datum_t::null();
        }
        ql::datum_t modified;
        try {
            cond_t non_interruptor;
            ql::env_t write_hook_env(&non_interruptor,
                                     ql::return_empty_normal_batches_t::NO,
                                     reql_version_t::LATEST);

            ql::datum_object_builder_t builder;
            builder.overwrite("primary_key", std::move(primary_key));
            builder.overwrite("timestamp", write_timestamp);

            modified = write_hook->call(&write_hook_env,
                                        std::vector<ql::datum_t>{
                                            std::move(builder).to_datum(),
                                                d,
                                                res})->as_datum(&write_hook_env);
        } catch (ql::exc_t &e) {
            throw ql::exc_t(e.get_type(),
                            strprintf("Error in write hook: %s", e.what()),
                            e.backtrace(),
                            e.dummy_frames());
        } catch (ql::datum_exc_t &e) {
            throw ql::datum_exc_t(e.get_type(),
                                  strprintf("Error in write hook: %s", e.what()));
        }

        rcheck_toplevel(!(res.get_type() == ql::datum_t::type_t::R_NULL &&
                          modified.get_type() != ql::datum_t::type_t::R_NULL),
                        ql::base_exc_t::OP_FAILED,
                        "A write hook function must not turn a deletion into a "
                        "replace/insert.");
        rcheck_toplevel(!(res.get_type() != ql::datum_t::type_t::R_NULL &&
                          modified.get_type() == ql::datum_t::type_t::R_NULL),
                        ql::base_exc_t::OP_FAILED,
                        "A write hook function must not turn a replace/insert "
                        "into a deletion.");
        res = modified;
    }
    return res;
}

typedef ql::transform_variant_t transform_variant_t;
typedef ql::terminal_variant_t terminal_variant_t;

class rget_sindex_data_t {
public:
    rget_sindex_data_t(key_range_t _pkey_range,
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
    friend class rocks_rget_secondary_cb;
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
    friend class rocks_rget_cb;
    friend class rocks_rget_secondary_cb;
    ql::env_t *const env;
    scoped_ptr_t<ql::batcher_t> batcher;
    std::vector<scoped_ptr_t<ql::op_t> > transformers;
    sorting_t sorting;
    scoped_ptr_t<ql::accumulator_t> accumulator;
};

class rget_io_data_t {
public:
    rget_io_data_t(rget_read_response_t *_response, btree_slice_t *_slice)
        : response(_response), slice(_slice) { }
private:
    friend class rocks_rget_cb;
    friend class rocks_rget_secondary_cb;
    rget_read_response_t *const response;
    btree_slice_t *const slice;
};

class rocks_rget_cb {
public:
    rocks_rget_cb(
        rget_io_data_t &&_io,
        job_data_t &&_job);

    continue_bool_t handle_pair(
        std::pair<const char *, size_t> key, std::pair<const char *, size_t> value,
        size_t default_copies)
        THROWS_ONLY(interrupted_exc_t);
    void finish(continue_bool_t last_cb) THROWS_ONLY(interrupted_exc_t);
private:
    const rget_io_data_t io; // How do get data in/out.
    job_data_t job; // What to do next (stateful).

    // State for internal bookkeeping.
    bool bad_init;
    scoped_ptr_t<profile::disabler_t> disabler;
    scoped_ptr_t<profile::sampler_t> sampler;
};

// This is the interface the btree code expects, but our actual callback needs a
// little bit more so we use this wrapper to hold the extra information.
class rocks_rget_cb_wrapper : public rocks_traversal_cb {
public:
    rocks_rget_cb_wrapper(
            rocks_rget_cb *_cb,
            size_t _copies)
        : cb(_cb), copies(_copies) { }
    virtual continue_bool_t handle_pair(
        std::pair<const char *, size_t> key, std::pair<const char *, size_t> value)
        THROWS_ONLY(interrupted_exc_t) {
        return cb->handle_pair(
            key, value,
            copies);
    }
private:
    rocks_rget_cb *cb;
    size_t copies;
    DISABLE_COPYING(rocks_rget_cb_wrapper);
};

rocks_rget_cb::rocks_rget_cb(rget_io_data_t &&_io,
        job_data_t &&_job)
    : io(std::move(_io)),
      job(std::move(_job)),
      bad_init(false) {

    // We must disable profiler events for subtasks, because multiple instances
    // of `handle_pair`are going to run in parallel which  would otherwise corrupt
    // the sequence of events in the profiler trace.
    disabler.init(new profile::disabler_t(job.env->trace));
    sampler.init(new profile::sampler_t("Range traversal doc evaluation.",
                                        job.env->trace));
}

void rocks_rget_cb::finish(continue_bool_t last_cb) THROWS_ONLY(interrupted_exc_t) {
    job.accumulator->finish(last_cb, &io.response->result);
}

// Handle a keyvalue pair.  Returns whether or not we're done early.
continue_bool_t rocks_rget_cb::handle_pair(
        std::pair<const char *, size_t> keyslice, std::pair<const char *, size_t> value,
        size_t default_copies)
    THROWS_ONLY(interrupted_exc_t) {

    //////////////////////////////////////////////////
    // STUFF THAT CAN HAPPEN OUT OF ORDER GOES HERE //
    //////////////////////////////////////////////////
    sampler->new_sample();
    if (bad_init || boost::get<ql::exc_t>(&io.response->result) != nullptr) {
        return continue_bool_t::ABORT;
    }
    // Load the key and value.
    store_key_t key(keyslice.second, reinterpret_cast<const uint8_t *>(keyslice.first));
    ql::datum_t val;
    // Count stats whether or not we deserialize the value
    io.slice->stats.pm_keys_read.record();
    io.slice->stats.pm_total_keys_read += 1;
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
            (**it)(job.env, &data, lazy_sindex_val);
        }
        // We need lots of extra data for the accumulation because we might be
        // accumulating `rget_item_t`s for a batch.
        continue_bool_t cont = (*job.accumulator)(job.env, &data, key, lazy_sindex_val);
        return cont;
    } catch (const ql::exc_t &e) {
        io.response->result = e;
        return continue_bool_t::ABORT;
    } catch (const ql::datum_exc_t &e) {
#ifndef NDEBUG
        unreachable();
#else
        io.response->result = ql::exc_t(e, ql::backtrace_id_t::empty());
        return continue_bool_t::ABORT;
#endif // NDEBUG
    }
}


class rocks_rget_secondary_cb {
public:
    rocks_rget_secondary_cb(
        rget_io_data_t &&_io,
        job_data_t &&_job,
        rget_sindex_data_t &&_sindex_data);

    continue_bool_t handle_pair(
        std::pair<const char *, size_t> key, std::pair<const char *, size_t> value,
        size_t default_copies,
        const std::string &skey_left)
        THROWS_ONLY(interrupted_exc_t);
    void finish(continue_bool_t last_cb) THROWS_ONLY(interrupted_exc_t);
private:
    const rget_io_data_t io; // How do get data in/out.
    job_data_t job; // What to do next (stateful).
    const rget_sindex_data_t sindex_data; // Optional sindex information.

    scoped_ptr_t<ql::env_t> sindex_env;

    // State for internal bookkeeping.
    bool bad_init;
    optional<std::string> last_truncated_secondary_for_abort;
    scoped_ptr_t<profile::disabler_t> disabler;
    scoped_ptr_t<profile::sampler_t> sampler;
};

// This is the interface the btree code expects, but our actual callback needs a
// little bit more so we use this wrapper to hold the extra information.
class rocks_rget_secondary_cb_wrapper : public rocks_traversal_cb {
public:
    rocks_rget_secondary_cb_wrapper(
            rocks_rget_secondary_cb *_cb,
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
    rocks_rget_secondary_cb *cb;
    size_t copies;
    std::string skey_left;
};

rocks_rget_secondary_cb::rocks_rget_secondary_cb(rget_io_data_t &&_io,
        job_data_t &&_job,
        rget_sindex_data_t &&_sindex_data)
    : io(std::move(_io)),
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

void rocks_rget_secondary_cb::finish(continue_bool_t last_cb) THROWS_ONLY(interrupted_exc_t) {
    job.accumulator->finish(last_cb, &io.response->result);
}

// Handle a keyvalue pair.  Returns whether or not we're done early.
continue_bool_t rocks_rget_secondary_cb::handle_pair(
        std::pair<const char *, size_t> keyslice, std::pair<const char *, size_t> value,
        size_t default_copies,
        const std::string &skey_left)
    THROWS_ONLY(interrupted_exc_t) {

    //////////////////////////////////////////////////
    // STUFF THAT CAN HAPPEN OUT OF ORDER GOES HERE //
    //////////////////////////////////////////////////
    sampler->new_sample();
    if (bad_init || boost::get<ql::exc_t>(&io.response->result) != nullptr) {
        return continue_bool_t::ABORT;
    }
    // Load the key and value.
    store_key_t key(keyslice.second, reinterpret_cast<const uint8_t *>(keyslice.first));
    if (!sindex_data.pkey_range.contains_key(ql::datum_t::extract_primary(key))) {
        return continue_bool_t::CONTINUE;
    }
    ql::datum_t val;
    // Count stats whether or not we deserialize the value
    io.slice->stats.pm_keys_read.record();
    io.slice->stats.pm_total_keys_read += 1;
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
    if (last_truncated_secondary_for_abort) {
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
                sindex_data.active_region_range_inout->left.increment();
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
            (**it)(job.env, &data, lazy_sindex_val);
        }
        // We need lots of extra data for the accumulation because we might be
        // accumulating `rget_item_t`s for a batch.
        continue_bool_t cont = (*job.accumulator)(job.env, &data, key, lazy_sindex_val);
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
        io.response->result = e;
        return continue_bool_t::ABORT;
    } catch (const ql::datum_exc_t &e) {
#ifndef NDEBUG
        unreachable();
#else
        io.response->result = ql::exc_t(e, ql::backtrace_id_t::empty());
        return continue_bool_t::ABORT;
#endif // NDEBUG
    }
}

// TODO: Having two functions which are 99% the same sucks.
void rdb_rget_slice(
        rockshard rocksh,
        btree_slice_t *slice,
        const key_range_t &range,
        const optional<std::map<store_key_t, uint64_t> > &primary_keys,
        real_superblock_lock *superblock,
        ql::env_t *ql_env,
        const ql::batchspec_t &batchspec,
        const std::vector<transform_variant_t> &transforms,
        const optional<terminal_variant_t> &terminal,
        sorting_t sorting,
        rget_read_response_t *response,
        release_superblock_t release_superblock) {
    r_sanity_check(boost::get<ql::exc_t>(&response->result) == nullptr);
    PROFILE_STARTER_IF_ENABLED(
        ql_env->profile() == profile_bool_t::PROFILE,
        "Do range scan on primary index.",
        ql_env->trace);

    rocks_rget_cb callback(
        rget_io_data_t(response, slice),
        job_data_t(ql_env,
                   batchspec,
                   transforms,
                   terminal,
                   !reversed(sorting)
                       ? ql::limit_read_last_key(range.left)
                       : ql::limit_read_last_key(key_or_max::from_right_bound(range.right)),
                   sorting,
                   require_sindexes_t::NO));

    direction_t direction = reversed(sorting) ? direction_t::backward : direction_t::forward;
    continue_bool_t cont = continue_bool_t::CONTINUE;
    std::string rocks_kv_prefix = rockstore::table_primary_prefix(rocksh.table_id);
    if (primary_keys.has_value()) {
        // TODO: Instead of holding onto the superblock, we could make an iterator once,
        // or hold a rocksdb snapshot once, out here.
        auto cb = [&](const std::pair<store_key_t, uint64_t> &pair, bool is_last) {
            rocks_rget_cb_wrapper wrapper(&callback, pair.second);
            // Acquire read lock on superblock and make a snapshot.
            superblock->read_acq_signal()->wait_lazily_ordered();
            rockstore::snapshot snap = make_snapshot(rocksh.rocks);
            if (is_last && release_superblock == release_superblock_t::RELEASE) {
                superblock->reset_superblock();
            }

            return rocks_traversal(
                rocksh.rocks,
                snap.snap,
                rocks_kv_prefix,
                key_range_t::one_key(pair.first),
                direction,
                &wrapper);
        };
        if (!reversed(sorting)) {
            for (auto it = primary_keys->begin(); it != primary_keys->end();) {
                auto this_it = it++;
                if (cb(*this_it, it == primary_keys->end()) == continue_bool_t::ABORT) {
                    // If required the superblock will get released further up the stack.
                    cont = continue_bool_t::ABORT;
                    break;
                }
            }
        } else {
            for (auto it = primary_keys->rbegin(); it != primary_keys->rend();) {
                auto this_it = it++;
                if (cb(*this_it, it == primary_keys->rend()) == continue_bool_t::ABORT) {
                    // If required the superblock will get released further up the stack.
                    cont = continue_bool_t::ABORT;
                    break;
                }
            }
        }
    } else {
        rocks_rget_cb_wrapper wrapper(&callback, 1);
        superblock->read_acq_signal()->wait_lazily_ordered();
        rockstore::snapshot snap = make_snapshot(rocksh.rocks);
        if (release_superblock == release_superblock_t::RELEASE) {
            superblock->reset_superblock();
        }
        cont = rocks_traversal(
            rocksh.rocks, snap.snap, rocks_kv_prefix, range, direction, &wrapper);
    }
    callback.finish(cont);
}


// TODO: Remove this?  And header.
void rdb_rget_secondary_slice(
        rockshard rocksh,
        uuid_u sindex_uuid,
        btree_slice_t *slice,
        const ql::datumspec_t &datumspec,
        const key_range_t &sindex_region_range,
        real_superblock_lock *superblock,
        ql::env_t *ql_env,
        const ql::batchspec_t &batchspec,
        const std::vector<transform_variant_t> &transforms,
        const optional<terminal_variant_t> &terminal,
        const key_range_t &pk_range,
        sorting_t sorting,
        require_sindexes_t require_sindex_val,
        const sindex_disk_info_t &sindex_info,
        rget_read_response_t *response,
        release_superblock_t release_superblock) {
    r_sanity_check(boost::get<ql::exc_t>(&response->result) == nullptr);
    guarantee(sindex_info.geo == sindex_geo_bool_t::REGULAR);
    PROFILE_STARTER_IF_ENABLED(
        ql_env->profile() == profile_bool_t::PROFILE,
        "Do range scan on secondary index.",
        ql_env->trace);

    const reql_version_t sindex_func_reql_version =
        sindex_info.mapping_version_info.latest_compatible_reql_version;

    key_range_t active_region_range = sindex_region_range;
    rocks_rget_secondary_cb callback(
        rget_io_data_t(response, slice),
        job_data_t(ql_env,
                   batchspec,
                   transforms,
                   terminal,
                   !reversed(sorting)
                       ? ql::limit_read_last_key(sindex_region_range.left)
                       : ql::limit_read_last_key(key_or_max::from_right_bound(sindex_region_range.right)),
                   sorting,
                   require_sindex_val),
        rget_sindex_data_t(
            pk_range,
            datumspec,
            &active_region_range,
            sindex_func_reql_version,
            sindex_info.mapping,
            sindex_info.multi));

    std::string rocks_kv_prefix = rockstore::table_secondary_prefix(rocksh.table_id, sindex_uuid);

    // TODO: We could make a rocksdb snapshot here, and iterate through that,
    // instead of holding a superblock.
    // TODO: Or we could wait for the superblock here, or wait for it outside and don't even pass the superblock in.
    direction_t direction = reversed(sorting) ? direction_t::backward : direction_t::forward;
    auto cb = [&](const std::pair<ql::datum_range_t, uint64_t> &pair, bool is_last) {
        key_range_t sindex_keyrange =
            pair.first.to_sindex_keyrange();
        rocks_rget_secondary_cb_wrapper wrapper(
            &callback,
            pair.second,
            key_to_unescaped_str(sindex_keyrange.left));
        key_range_t active_range = active_region_range.intersection(sindex_keyrange);
        // This can happen sometimes with truncated keys.
        if (active_range.is_empty()) {
            return continue_bool_t::CONTINUE;
        }
        superblock->read_acq_signal()->wait_lazily_ordered();
        rockstore::snapshot snap = make_snapshot(rocksh.rocks);
        if (is_last && release_superblock == release_superblock_t::RELEASE) {
            superblock->reset_superblock();
        }
        return rocks_traversal(
            rocksh.rocks,
            snap.snap,
            rocks_kv_prefix,
            active_range,
            direction,
            &wrapper);
    };
    continue_bool_t cont = datumspec.iter(sorting, cb);
    callback.finish(cont);
}


static const int8_t HAS_VALUE = 0;
static const int8_t HAS_NO_VALUE = 1;

template <cluster_version_t W>
void serialize(write_message_t *wm, const rdb_modification_info_t &info) {
    if (!info.deleted.first.has()) {
        serialize<W>(wm, HAS_NO_VALUE);
    } else {
        serialize<W>(wm, HAS_VALUE);
        serialize<W>(wm, info.deleted.first);
    }

    if (!info.added.first.has()) {
        serialize<W>(wm, HAS_NO_VALUE);
    } else {
        serialize<W>(wm, HAS_VALUE);
        serialize<W>(wm, info.added.first);
    }
}

template <cluster_version_t W>
archive_result_t deserialize(read_stream_t *s, rdb_modification_info_t *info) {
    int8_t has_value;
    archive_result_t res = deserialize<W>(s, &has_value);
    if (bad(res)) { return res; }

    if (has_value == HAS_VALUE) {
        res = deserialize<W>(s, &info->deleted.first);
        if (bad(res)) { return res; }
    }

    res = deserialize<W>(s, &has_value);
    if (bad(res)) { return res; }

    if (has_value == HAS_VALUE) {
        res = deserialize<W>(s, &info->added.first);
        if (bad(res)) { return res; }
    }

    return archive_result_t::SUCCESS;
}

INSTANTIATE_SERIALIZABLE_SINCE_v1_13(rdb_modification_info_t);

RDB_IMPL_SERIALIZABLE_2_SINCE_v1_13(rdb_modification_report_t, primary_key, info);

std::vector<std::string> expand_geo_key(
        const ql::datum_t &key,
        const store_key_t &primary_key,
        optional<uint64_t> tag_num) {
    // Ignore non-geometry objects in geo indexes.
    // TODO (daniel): This needs to be changed once compound geo index
    // support gets added.
    if (!key.is_ptype(ql::pseudo::geometry_string)) {
        return std::vector<std::string>();
    }

    try {
        std::vector<std::string> grid_keys =
            compute_index_grid_keys(key, GEO_INDEX_GOAL_GRID_CELLS);

        std::vector<std::string> result;
        result.reserve(grid_keys.size());
        for (size_t i = 0; i < grid_keys.size(); ++i) {
            // TODO (daniel): Something else that needs change for compound index
            //   support: We must be able to truncate geo keys and handle such
            //   truncated keys.
            rassert(grid_keys[i].length() <= ql::datum_t::trunc_size(
                        key_to_unescaped_str(primary_key).length()));

            result.push_back(
                ql::datum_t::compose_secondary(
                    grid_keys[i], primary_key, tag_num));
        }

        return result;
    } catch (const geo_exception_t &e) {
        // As things are now, this exception is actually ignored in
        // `compute_keys()`. That's ok, though it would be nice if we could
        // pass on some kind of warning to the user.
        logWRN("Failed to compute grid keys for an index: %s", e.what());
        rfail_target(&key, ql::base_exc_t::LOGIC,
                "Failed to compute grid keys: %s", e.what());
    }
}

void compute_keys(const store_key_t &primary_key,
                  ql::datum_t doc,
                  const sindex_disk_info_t &index_info,
                  std::vector<std::pair<store_key_t, ql::datum_t> > *keys_out,
                  std::vector<index_pair_t> *cfeed_keys_out) {

    guarantee(keys_out->empty());

    const reql_version_t reql_version =
        index_info.mapping_version_info.latest_compatible_reql_version;

    // Secondary index functions are deterministic (so no need for an rdb_context_t)
    // and evaluated in a pristine environment (without global optargs).
    cond_t non_interruptor;
    ql::env_t sindex_env(&non_interruptor,
                         ql::return_empty_normal_batches_t::NO,
                         reql_version);

    ql::datum_t index =
        index_info.mapping.det_func.compile()->call(&sindex_env, doc)->as_datum(&sindex_env);

    if (index_info.multi == sindex_multi_bool_t::MULTI
        && index.get_type() == ql::datum_t::R_ARRAY) {
        for (uint64_t i = 0; i < index.arr_size(); ++i) {
            const ql::datum_t &skey = index.get(i, ql::THROW);
            if (index_info.geo == sindex_geo_bool_t::GEO) {
                std::vector<std::string> geo_keys = expand_geo_key(skey,
                                                                   primary_key,
                                                                   make_optional(i));
                for (auto it = geo_keys.begin(); it != geo_keys.end(); ++it) {
                    keys_out->push_back(std::make_pair(store_key_t(*it), skey));
                }
                if (cfeed_keys_out != nullptr) {
                    // For geospatial indexes, we generate multiple keys for the same
                    // index entry. We only pass the smallest one on in order to not get
                    // redundant results on the changefeed.
                    auto min_it = std::min_element(geo_keys.begin(), geo_keys.end());
                    if (min_it != geo_keys.end()) {
                        cfeed_keys_out->push_back(
                            std::make_pair(skey, std::move(*min_it)));
                    }
                }
            } else {
                try {
                    std::string store_key =
                        skey.print_secondary(primary_key,
                                             make_optional(i));
                    keys_out->push_back(
                        std::make_pair(store_key_t(store_key), skey));
                    if (cfeed_keys_out != nullptr) {
                        cfeed_keys_out->push_back(
                            std::make_pair(skey, std::move(store_key)));
                    }
                } catch (const ql::base_exc_t &e) {
                    // One of the values couldn't be converted to an index key.
                    // Ignore it and move on to the next one.
                }
            }
        }
    } else {
        if (index_info.geo == sindex_geo_bool_t::GEO) {
            std::vector<std::string> geo_keys = expand_geo_key(index,
                                                               primary_key,
                                                               r_nullopt);
            for (auto it = geo_keys.begin(); it != geo_keys.end(); ++it) {
                keys_out->push_back(std::make_pair(store_key_t(*it), index));
            }
            if (cfeed_keys_out != nullptr) {
                // For geospatial indexes, we generate multiple keys for the same
                // index entry. We only pass the smallest one on in order to not get
                // redundant results on the changefeed.
                auto min_it = std::min_element(geo_keys.begin(), geo_keys.end());
                if (min_it != geo_keys.end()) {
                    cfeed_keys_out->push_back(
                        std::make_pair(index, std::move(*min_it)));
                }
            }
        } else {
            std::string store_key =
                index.print_secondary(primary_key, r_nullopt);
            keys_out->push_back(
                std::make_pair(store_key_t(store_key), index));
            if (cfeed_keys_out != nullptr) {
                cfeed_keys_out->push_back(
                    std::make_pair(index, std::move(store_key)));
            }
        }
    }
}


