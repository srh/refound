// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/env.hpp"

#include "extproc/js_runner.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/minidriver.hpp"
#include "rdb_protocol/term_walker.hpp"
#include "rdb_protocol/val.hpp"

#include "debug.hpp"

// This is a totally arbitrary constant limiting the size of the regex cache.  1000
// was chosen out of a hat; if you have a good argument for it being something else
// (apart from cache line concerns, which are irrelevant due to the implementation)
// you're probably right.
const size_t LRU_CACHE_SIZE = 1000;

namespace ql {

void env_t::set_eval_callback(eval_callback_t *callback) {
    eval_callback_ = callback;
}

void env_t::do_eval_callback() {
    if (eval_callback_ != NULL) {
        eval_callback_->eval_callback();
    }
}

// TODO: Should this really take a val_t?
// TODO: Check that the val_t belongs to this env_t.
configured_limits_t env_t::limits_with_changefeed_queue_size(
    scoped_ptr_t<val_t> changefeed_queue_size) {
    if (changefeed_queue_size.has()) {
        return configured_limits_t(
            check_limit("changefeed queue size",
                        changefeed_queue_size->as_int(this)),
            limits_.array_size_limit());
    } else {
        return limits_;
    }
}

profile_bool_t env_t::profile() const {
    return trace != nullptr ? profile_bool_t::PROFILE : profile_bool_t::DONT_PROFILE;
}

std::string env_t::get_reql_http_proxy() {
    r_sanity_check(rdb_ctx_ != NULL);
    return rdb_ctx_->reql_http_proxy;
}

extproc_pool_t *env_t::get_extproc_pool() {
    assert_thread();
    r_sanity_check(rdb_ctx_ != NULL);
    r_sanity_check(rdb_ctx_->extproc_pool != NULL);
    return rdb_ctx_->extproc_pool;
}

js_runner_t *env_t::get_js_runner() {
    assert_thread();
    extproc_pool_t *extproc_pool = get_extproc_pool();
    if (!js_runner_.connected()) {
        js_runner_.begin(extproc_pool, interruptor, limits());
    }
    return &js_runner_;
}

scoped_ptr_t<profile::trace_t> maybe_make_profile_trace(profile_bool_t profile) {
    return profile == profile_bool_t::PROFILE
        ? make_scoped<profile::trace_t>()
        : scoped_ptr_t<profile::trace_t>();
}

env_t::env_t(rdb_context_t *ctx,
             return_empty_normal_batches_t _return_empty_normal_batches,
             const signal_t *_interruptor,
             serializable_env_t s_env,
             profile::trace_t *_trace)
    : serializable_(std::move(s_env)),
      limits_(from_optargs(ctx, _interruptor, &serializable_.global_optargs,
                           serializable_.deterministic_time)),
      reql_version_(reql_version_t::LATEST),
      regex_cache_(LRU_CACHE_SIZE),
      return_empty_normal_batches(_return_empty_normal_batches),
      interruptor(_interruptor),
      trace(_trace),
      evals_since_yield_(0),
      rdb_ctx_(ctx),
      eval_callback_(NULL) {
    // OOO: The reasoning that we can comment this out, and that rdb_ctx_ can be nullptr, is based on some spooky global reasoning.  (More precisely, we crash if the global optargs have nondeterministic functions.)  I'm not a fan.
    // rassert(ctx != NULL);
    rassert(interruptor != NULL);
}

env_t::env_t(rdb_context_t *ctx,
             return_empty_normal_batches_t _return_empty_normal_batches,
             const signal_t *_interruptor,
             global_optargs_t _global_optargs,
             auth::user_context_t _user_context,
             datum_t _deterministic_time,
             profile::trace_t *_trace) :
    env_t(ctx,
          _return_empty_normal_batches,
          _interruptor,
          serializable_env_t{
                  std::move(_global_optargs),
                  std::move(_user_context),
                  std::move(_deterministic_time)},
          _trace) { }

// Used in constructing the env for rdb_update_single_sindex and many unit tests.
env_t::env_t(const signal_t *_interruptor,
             return_empty_normal_batches_t _return_empty_normal_batches,
             reql_version_t _reql_version)
    : serializable_{
        global_optargs_t(),
        auth::user_context_t(auth::permissions_t(tribool::False, tribool::False, tribool::False, tribool::False)),
        datum_t()},
      reql_version_(_reql_version),
      regex_cache_(LRU_CACHE_SIZE),
      return_empty_normal_batches(_return_empty_normal_batches),
      interruptor(_interruptor),
      trace(NULL),
      evals_since_yield_(0),
      rdb_ctx_(NULL),
      eval_callback_(NULL) {
    rassert(interruptor != NULL);
}

env_t::~env_t() { }

void env_t::maybe_yield() {
    if (++evals_since_yield_ > EVALS_BEFORE_YIELD) {
        evals_since_yield_ = 0;
        coro_t::yield();
    }
}

} // namespace ql
