#include "concurrency/exponential_backoff.hpp"

#include "arch/runtime/coroutines.hpp"

void exponential_backoff_t::failure(const signal_t *interruptor) {
    if (backoff_ms == 0) {
        coro_t::yield();
        backoff_ms = min_backoff_ms;
    } else {
        nap(backoff_ms, interruptor);
        guarantee(static_cast<uint64_t>(backoff_ms * fail_factor) > backoff_ms,
            "rounding screwed it up");
        backoff_ms *= fail_factor;
        if (backoff_ms > max_backoff_ms) {
            backoff_ms = max_backoff_ms;
        }
    }
}
