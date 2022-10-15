#include "fdb/startup_shutdown.hpp"

void *fdb_thread(void *ctx) {
    (void)ctx;
    fdb_error_t err = fdb_run_network();
    if (err != 0) {
        const char *msg = fdb_get_error(err);
        printf("ERROR: fdb_run_network failed: %s\n", msg);
        return (void *)1;
    }

    printf("fdb_run_network completed.\n");
    return nullptr;
}

bool setup_fdb(pthread_t *thread) {
    fdb_error_t err = fdb_select_api_version(FDB_API_VERSION);
    if (err != 0) {
        const char *msg = fdb_get_error(err);
        printf("ERROR: Could not initialize FoundationDB client library: %s\n", msg);
        return false;
    }
    err = fdb_setup_network();
    if (err != 0) {
        const char *msg = fdb_get_error(err);
        printf("ERROR: fdb_setup_network failed: %s\n", msg);
        return false;
    }

    int result = pthread_create(thread, nullptr, fdb_thread, nullptr);
    guarantee_xerr(result == 0, result, "Could not create thread: %d", result);

    return true;
}

bool join_fdb(pthread_t thread) {
    fdb_error_t err = fdb_stop_network();
    if (err != 0) {
        const char *msg = fdb_get_error(err);
        printf("ERROR: fdb_stop_network failed: %s\n", msg);
        return false;
    }

    void *thread_return;
    int res = pthread_join(thread, &thread_return);
    guarantee_xerr(res == 0, res, "Could not join thread.");
    printf("fdb network thread has been joined.\n");
    return true;
}