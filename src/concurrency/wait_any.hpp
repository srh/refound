// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CONCURRENCY_WAIT_ANY_HPP_
#define CONCURRENCY_WAIT_ANY_HPP_

#include "concurrency/signal.hpp"
#include "containers/intrusive_list.hpp"
#include "containers/object_buffer.hpp"

/* Monitors multiple signals; becomes pulsed if any individual signal becomes
pulsed. */

class wait_any_subscription_t;

class wait_any_t : public signal_t {
public:
    template <typename... Args>
    explicit wait_any_t(Args... args) {
        UNUSED int arr[] = { 0, (add(args), 1)... };
    }

    ~wait_any_t();

    void add(const signal_t *s);

private:
    class wait_any_subscription_t : public signal_t::subscription_t, public intrusive_list_node_t<wait_any_subscription_t> {
    public:
        wait_any_subscription_t(wait_any_t *_parent, bool _alloc_on_heap) : parent(_parent), alloc_on_heap(_alloc_on_heap) { }
        virtual void run();
        bool on_heap() const { return alloc_on_heap; }
    private:
        wait_any_t *parent;
        bool alloc_on_heap;
        DISABLE_COPYING(wait_any_subscription_t);
    };

    static const size_t default_preallocated_subs = 4;

    void pulse_if_not_already_pulsed();

    intrusive_list_t<wait_any_subscription_t> subs;

    object_buffer_t<wait_any_subscription_t> sub_storage[default_preallocated_subs];

    DISABLE_COPYING(wait_any_t);
};

// These two don't use variadic templates because (a) there are only three of them, and (b)
// default_preallocated_subs is 4.
inline void wait_any(const signal_t *arg1, const signal_t *arg2) {
    wait_any_t waiter{arg1, arg2};
    waiter.wait();
}

inline void wait_any(const signal_t *arg1, const signal_t *arg2, const signal_t *arg3) {
    wait_any_t waiter{arg1, arg2, arg3};
    waiter.wait();
}

inline void wait_any(const signal_t *arg1, const signal_t *arg2, const signal_t *arg3, const signal_t *arg4) {
    wait_any_t waiter{arg1, arg2, arg3, arg4};
    waiter.wait();
}

#endif /* CONCURRENCY_WAIT_ANY_HPP_ */
