#ifndef RETHINKDB_FDB_STARTUP_SHUTDOWN_HPP_
#define RETHINKDB_FDB_STARTUP_SHUTDOWN_HPP_

#include <pthread.h>

#include "errors.hpp"
#include "fdb/fdb.hpp"

void *fdb_thread(void *ctx);

bool setup_fdb(pthread_t *thread);

bool join_fdb(pthread_t thread);

struct fdb_startup_shutdown {
    fdb_startup_shutdown() {
        guarantee(setup_fdb(&fdb_network_thread),
            "Failed to setup FoundationDB");
    }
    ~fdb_startup_shutdown() {
        guarantee(join_fdb(fdb_network_thread),
            "Failed to join FoundationDB network thread");
    }
    pthread_t fdb_network_thread;

    DISABLE_COPYING(fdb_startup_shutdown);
};

#endif  // RETHINKDB_FDB_STARTUP_SHUTDOWN_HPP_
