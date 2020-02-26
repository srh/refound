// Copyright 2010-2016 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_MAIN_MEMORY_CHECKER_HPP_
#define CLUSTERING_ADMINISTRATION_MAIN_MEMORY_CHECKER_HPP_

#include <functional>

#include "arch/runtime/coroutines.hpp"
#include "arch/timing.hpp"
#include "clustering/main/cache_size.hpp"
#include "concurrency/auto_drainer.hpp"
#include "rdb_protocol/context.hpp"

// memory_checker_t is created in serve.cc, and calls a repeating timer to
// Periodically check if we're using swap by looking at the proc file or system calls.
// If we're using swap, it creates an issue in a local issue tracker, and logs an error.
class memory_checker_t : private repeating_timer_callback_t {
public:
    memory_checker_t();

private:
    void do_check(auto_drainer_t::lock_t keepalive);
    void on_ring() final {
        coro_t::spawn_sometime(std::bind(&memory_checker_t::do_check,
                                         this,
                                         drainer.lock()));
    }

    uint64_t checks_until_reset;
    uint64_t swap_usage;

    bool print_log_message;

    int practice_runs_remaining;

    // Timer must be destructed before drainer, because on_ring acquires a lock on drainer.
    auto_drainer_t drainer;
    repeating_timer_t timer;
};

#endif // CLUSTERING_ADMINISTRATION_MAIN_MEMORY_CHECKER_HPP_
