// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "btree/concurrent_traversal.hpp"

#include <stdint.h>

#include <algorithm>
#include <functional>

// TODO: Remove this include if we wrap the iterator stuff...
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"

#include "arch/runtime/coroutines.hpp"
#include "arch/runtime/thread_pool.hpp"
#include "btree/superblock.hpp"
#include "concurrency/auto_drainer.hpp"
#include "concurrency/semaphore.hpp"
#include "concurrency/fifo_enforcer.hpp"
#include "rockstore/store.hpp"


class incr_decr_t {
public:
    explicit incr_decr_t(size_t *ptr) : ptr_(ptr) {
        guarantee(*ptr_ < SIZE_MAX);
        ++*ptr_;
    }
    ~incr_decr_t() {
        --*ptr_;
    }

private:
    size_t *ptr_;

    DISABLE_COPYING(incr_decr_t);
};


namespace concurrent_traversal {
static const int initial_semaphore_capacity = 3;
static const int min_semaphore_capacity = 2;
static const int max_semaphore_capacity = 30;
static const int yield_interval = 100;
}  // namespace concurrent_traversal

class concurrent_traversal_adapter_t : public depth_first_traversal_callback_t {
public:

    explicit concurrent_traversal_adapter_t(concurrent_traversal_callback_t *cb,
                                            cond_t *failure_cond)
        : semaphore_(concurrent_traversal::initial_semaphore_capacity, 0.5),
          sink_waiters_(0),
          cb_(cb),
          failure_cond_(failure_cond),
          yield_counter(0) { }

    continue_bool_t filter_range(
            const btree_key_t *left_excl_or_null,
            const btree_key_t *right_incl,
            signal_t *,
            bool *skip_out) {
        cb_->filter_range(left_excl_or_null, right_incl, skip_out);
        return continue_bool_t::CONTINUE;
    }

    void handle_pair_coro(scoped_key_value_t *fragile_keyvalue,
                          semaphore_acq_t *fragile_acq,
                          fifo_enforcer_write_token_t token,
                          auto_drainer_t::lock_t) {
        // This is called by coro_t::spawn_now_dangerously. We need to get these
        // values before the caller's stack frame is destroyed.
        scoped_key_value_t keyvalue = std::move(*fragile_keyvalue);

        semaphore_acq_t semaphore_acq(std::move(*fragile_acq));

        fifo_enforcer_sink_t::exit_write_t exit_write(&sink_, token);

        continue_bool_t done;
        try {
            done = cb_->handle_pair(
                std::move(keyvalue),
                concurrent_traversal_fifo_enforcer_signal_t(&exit_write, this));
        } catch (const interrupted_exc_t &) {
            done = continue_bool_t::ABORT;
        }

        if (done == continue_bool_t::ABORT) {
            failure_cond_->pulse_if_not_already_pulsed();
        }
    }

    virtual continue_bool_t handle_pair(scoped_key_value_t &&keyvalue, signal_t *) {
        // First thing first: Get in line with the token enforcer.

        fifo_enforcer_write_token_t token = source_.enter_write();
        // ... and wait for the semaphore, we don't want too many things loading
        // values at once.
        semaphore_acq_t acq(&semaphore_);

        // Yield occasionally so we don't hog the thread if everything is in
        // memory and `cb->handle_pair()` doesn't block.
        if (yield_counter == concurrent_traversal::yield_interval) {
            coro_t::yield();
            yield_counter = 0;
        } else {
            ++yield_counter;
        }

        coro_t::spawn_now_dangerously(
            std::bind(&concurrent_traversal_adapter_t::handle_pair_coro,
                      this, &keyvalue, &acq, token, auto_drainer_t::lock_t(&drainer_)));

        // Report if we've failed by the time this handle_pair call is called.
        return failure_cond_->is_pulsed()
            ? continue_bool_t::ABORT : continue_bool_t::CONTINUE;
    }

    virtual profile::trace_t *get_trace() THROWS_NOTHING {
        return cb_->get_trace();
    }

private:
    friend class concurrent_traversal_fifo_enforcer_signal_t;

    adjustable_semaphore_t semaphore_;

    fifo_enforcer_source_t source_;
    fifo_enforcer_sink_t sink_;

    // The number of coroutines waiting for their turn with sink_.
    size_t sink_waiters_;

    concurrent_traversal_callback_t *cb_;

    // Signals when the query has failed, when we should give up all hope in executing
    // the query.
    cond_t *failure_cond_;

    // Counted up every time handle_pair() runs so we can yield occasionally.
    int yield_counter;

    // We don't use the drainer's drain signal, we use failure_cond_
    auto_drainer_t drainer_;
    DISABLE_COPYING(concurrent_traversal_adapter_t);
};

concurrent_traversal_fifo_enforcer_signal_t::
concurrent_traversal_fifo_enforcer_signal_t(
        signal_t *eval_exclusivity_signal,
        concurrent_traversal_adapter_t *parent)
    : eval_exclusivity_signal_(eval_exclusivity_signal),
      parent_(parent) { }

void concurrent_traversal_fifo_enforcer_signal_t::wait() THROWS_NOTHING {
    cond_t non_interruptor;
    wait_with_interruptor(&non_interruptor);
}

void concurrent_traversal_fifo_enforcer_signal_t::wait_interruptible()
    THROWS_ONLY(interrupted_exc_t) {
    wait_with_interruptor(parent_->failure_cond_);
}

void concurrent_traversal_fifo_enforcer_signal_t::wait_with_interruptor(
        signal_t *interruptor) THROWS_ONLY(interrupted_exc_t) {
    incr_decr_t incr_decr(&parent_->sink_waiters_);

    if (parent_->sink_waiters_ >= 2) {
        // If we have two or more things waiting for the signal (including ourselves)
        // we're probably looking too far ahead.  Lower the capacity by 1.  This might
        // seem abrupt (especially considering that our semapoher_.get_capacity()
        // concurrent reads can finish in any order), but we have the trickle fraction
        // set to 0.5, so there's smoothing.
        parent_->semaphore_.set_capacity(
            std::max<int64_t>(concurrent_traversal::min_semaphore_capacity,
                              parent_->semaphore_.get_capacity() - 1));
    } else if (parent_->sink_waiters_ == 1) {
        // We're the only thing waiting for the signal?  We might not be looking far
        // ahead enough.
        parent_->semaphore_.set_capacity(
            std::min<int64_t>(concurrent_traversal::max_semaphore_capacity,
                              parent_->semaphore_.get_capacity() + 1));
    }

    ::wait_interruptible(eval_exclusivity_signal_, interruptor);
}

continue_bool_t btree_concurrent_traversal(
        superblock_t *superblock,
        const key_range_t &range,
        concurrent_traversal_callback_t *cb,
        direction_t direction,
        release_superblock_t release_superblock) {
    cond_t failure_cond;
    bool failure_seen;
    {
        concurrent_traversal_adapter_t adapter(cb, &failure_cond);
        cond_t non_interruptor;
        failure_seen = (continue_bool_t::ABORT == btree_depth_first_traversal(
            superblock, range, &adapter, access_t::read, direction, release_superblock,
            &non_interruptor));
    }
    // Now that adapter is destroyed, the operations that might have failed have all
    // drained.  (If we fail, we try to report it to btree_depth_first_traversal (to
    // kill the traversal), but it's possible for us to fail after
    // btree_depth_first_traversal returns.)
    guarantee(!(failure_seen && !failure_cond.is_pulsed()));
    return failure_cond.is_pulsed() ? continue_bool_t::ABORT : continue_bool_t::CONTINUE;
}

continue_bool_t process_traversal_element(
        const std::string &rocks_kv_prefix,
        rocksdb::Slice key, rocksdb::Slice value, rocks_traversal_cb *cb) {
    rassert(key.starts_with(rocksdb::Slice(rocks_kv_prefix)));

    key.remove_prefix(rocks_kv_prefix.size());

    return cb->handle_pair(
        std::make_pair(key.data(), key.size()),
        std::make_pair(value.data(), value.size()));
}

continue_bool_t rocks_traversal(
        superblock_t *superblock,
        rockstore::store *rocks,
        const std::string &rocks_kv_prefix,
        const key_range_t &range,
        direction_t direction,
        release_superblock_t release_superblock,
        rocks_traversal_cb *cb) {
    // duh
    rocksdb::OptimisticTransactionDB *db = rocks->db();

    // linux_thread_pool_t::run_in_blocker_pool([&]() {
    // TODO: Release the superblock after we get a snapshot or something.
    // (Does making an iterator get us a snapshot?)

    // TODO: Check if we use switches for this type in other code.
    if (direction == direction_t::forward) {
        std::string prefixed_left_bound = rocks_kv_prefix + key_to_unescaped_str(range.left);
        std::string prefixed_upper_bound;
        rocksdb::Slice prefixed_upper_bound_slice;
        // TODO: Proper rocksdb::ReadOptions()
        rocksdb::ReadOptions opts;
        if (!range.right.unbounded) {
            prefixed_upper_bound = rocks_kv_prefix + key_to_unescaped_str(range.right.key());
            // Note: prefixed_upper_bound_slice doesn't copy the string, it points into it.
            prefixed_upper_bound_slice = rocksdb::Slice(prefixed_upper_bound);
            opts.iterate_upper_bound = &prefixed_upper_bound_slice;
        }
        // TODO: Check if we must call NewIterator on the thread pool thread.
        // TODO: Switching threads for every key/value pair is kind of lame.
        scoped_ptr_t<rocksdb::Iterator> iter(db->NewIterator(opts));
        // Release superblock after snapshotted iterator created.
        if (release_superblock == release_superblock_t::RELEASE) {
            superblock->release();
        }
        bool was_valid;
        rocksdb::Slice key_slice;
        rocksdb::Slice value_slice;
        linux_thread_pool_t::run_in_blocker_pool([&]() {
            iter->Seek(prefixed_left_bound);
            was_valid = iter->Valid();
            if (was_valid) {
                key_slice = iter->key();
                value_slice = iter->value();
            }
        });
        while (was_valid) {
            continue_bool_t contbool = process_traversal_element(rocks_kv_prefix, key_slice, value_slice, cb);
            if (contbool == continue_bool_t::ABORT) {
                return continue_bool_t::ABORT;
            }

            linux_thread_pool_t::run_in_blocker_pool([&]() {
                iter->Next();
                was_valid = iter->Valid();
                if (was_valid) {
                    key_slice = iter->key();
                    value_slice = iter->value();
                }
            });
        }
        return continue_bool_t::CONTINUE;
    } else {
        std::string prefixed_left_bound = rocks_kv_prefix + key_to_unescaped_str(range.left);
        // Note: Constructor here doesn't copy the string, it points to it.
        rocksdb::Slice prefixed_left_bound_slice(prefixed_left_bound);
        // TODO: Proper rocksdb::ReadOptions()
        rocksdb::ReadOptions opts;
        opts.iterate_lower_bound = &prefixed_left_bound_slice;
        // TODO: Check if we must call NewIterator on the thread pool thread.
        scoped_ptr_t<rocksdb::Iterator> iter(db->NewIterator(opts));
        // Release superblock after snapshotted iterator created.
        if (release_superblock == release_superblock_t::RELEASE) {
            superblock->release();
        }

        bool was_valid;
        rocksdb::Slice key_slice;
        rocksdb::Slice value_slice;
        linux_thread_pool_t::run_in_blocker_pool([&]() {
            if (range.right.unbounded) {
                iter->SeekToLast();
                was_valid = iter->Valid();
                if (was_valid) {
                    key_slice = iter->key();
                    value_slice = iter->value();
                }
            } else {
                std::string prefixed_right = rocks_kv_prefix + key_to_unescaped_str(range.right.key());
                iter->SeekForPrev(prefixed_right);
                was_valid = iter->Valid();
                if (was_valid) {
                    key_slice = iter->key();
                    if (key_slice.ToString() == prefixed_right) {
                        iter->Prev();
                        was_valid = iter->Valid();
                        if (was_valid) {
                            key_slice = iter->key();
                            value_slice = iter->value();
                        }
                    } else {
                        value_slice = iter->value();
                    }
                }
            }
        });

        while (was_valid) {
            continue_bool_t contbool = process_traversal_element(rocks_kv_prefix, key_slice, value_slice, cb);
            if (contbool == continue_bool_t::ABORT) {
                return continue_bool_t::ABORT;
            }

            linux_thread_pool_t::run_in_blocker_pool([&]() {
                iter->Prev();
                was_valid = iter->Valid();
                if (was_valid) {
                    key_slice = iter->key();
                    value_slice = iter->value();
                }
            });
        }
        return continue_bool_t::CONTINUE;
    }
}
