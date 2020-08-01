#include "concurrency/interruptor.hpp"

#include "concurrency/wait_any.hpp"

void wait_interruptible(const signal_t *signal, const signal_t *interruptor)
        THROWS_ONLY(interrupted_exc_t) {
    wait_any(signal, interruptor);
    if (interruptor->is_pulsed()) {
        throw interrupted_exc_t();
    }
}
