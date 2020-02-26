// Copyright 2010-2013 RethinkDB, all rights reserved.

#ifndef _WIN32
#include <sys/resource.h>
#endif

#include <set>

#include "clustering/main/command_line.hpp"
#include "crypto/initialization_guard.hpp"
#include "fdb/reql_fdb.hpp"
#include "utils.hpp"
#include "config/args.hpp"
#include "extproc/extproc_spawner.hpp"

void *fdb_thread(void *ctx) {
    (void)ctx;
    fdb_error_t err = fdb_run_network();
    if (err != 0) {
        const char *msg = fdb_get_error(err);
        printf("ERROR: fdb_run_network failed: %s", msg);
        return (void *)1;
    }

    printf("fdb_run_network completed.\n");
    return nullptr;
}

bool setup_fdb(pthread_t *thread) {
    fdb_error_t err = fdb_select_api_version(FDB_API_VERSION);
    if (err != 0) {
        const char *msg = fdb_get_error(err);
        printf("ERROR: Could not initialize FoundationDB client library: %s", msg);
        return false;
    }
    err = fdb_setup_network();
    if (err != 0) {
        const char *msg = fdb_get_error(err);
        printf("ERROR: fdb_setup_network failed: %s", msg);
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
        printf("ERROR: fdb_stop_network failed: %s", msg);
        return false;
    }

    void *thread_return;
    int res = pthread_join(thread, &thread_return);
    guarantee_xerr(res == 0, res, "Could not join thread.");
    printf("fdb network thread has been joined.\n");
    return true;
}

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

int main(int argc, char *argv[]) {

    startup_shutdown_t startup_shutdown;
    crypto::initialization_guard_t crypto_initialization_guard;

#ifdef _WIN32
    extproc_maybe_run_worker(argc, argv);
#endif

    std::set<std::string> subcommands_that_look_like_flags;
    subcommands_that_look_like_flags.insert("--version");
    subcommands_that_look_like_flags.insert("-v");
    subcommands_that_look_like_flags.insert("--help");
    subcommands_that_look_like_flags.insert("-h");

    // TODO: Make sure all commands work.
    // TODO: For each command, make sure all options work.

    if (argc == 1 || (argv[1][0] == '-' && subcommands_that_look_like_flags.count(argv[1]) == 0)) {
        fdb_startup_shutdown fdb_startup_shutdown;
        fdb_database db;

        return main_rethinkdb_serve(db.db, argc - 1, argv + 1);

    } else {
        std::string subcommand = argv[1];

        if (subcommand == "--version" || subcommand == "-v") {
            if (argc != 2) {
                printf("WARNING: Ignoring extra parameters after '%s'.", subcommand.c_str());
            }
            print_version_message();
            return 0;

        } else if (subcommand == "help" || subcommand == "-h" || subcommand == "--help") {
            if (argc == 2) {
                help_rethinkdb_porcelain();
                return 0;
            } else if (argc == 3) {
                std::string subcommand2 = argv[2];
                if (subcommand2 == "create") {
                    help_rethinkdb_create();
                    return 0;
                } else if (subcommand2 == "serve") {
                    help_rethinkdb_serve();
                    return 0;
                } else if (subcommand2 == "proxy") {
                    help_rethinkdb_proxy();
                } else if (subcommand2 == "export") {
                    help_rethinkdb_export();
                } else if (subcommand2 == "import") {
                    help_rethinkdb_import();
                } else if (subcommand2 == "dump") {
                    help_rethinkdb_dump();
                } else if (subcommand2 == "restore") {
                    help_rethinkdb_restore();
                } else if (subcommand2 == "index-rebuild") {
                    help_rethinkdb_index_rebuild();
                } else if (subcommand2 == "repl") {
                    help_rethinkdb_repl();
#ifdef _WIN32
                } else if (subcommand2 == "install-service") {
                    help_rethinkdb_install_service();
                } else if (subcommand2 == "remove-service") {
                    help_rethinkdb_remove_service();
#endif
                } else {
                    printf("ERROR: No help for '%s'\n", subcommand2.c_str());
                    return 1;
                }

            } else {
                puts("ERROR: Too many parameters to 'rethinkdb help'.  Try 'rethinkdb help [subcommand]'.");
                return 1;
            }

        } else {
            fdb_startup_shutdown fdb_startup_shutdown;
            fdb_database db;

            // TODO: Update --version/--help for reqlfdb.
            if (subcommand == "create") {
                return main_rethinkdb_create(db.db, argc - 2, argv + 2);
            } else if (subcommand == "serve") {
                return main_rethinkdb_serve(db.db, argc - 2, argv + 2);
            } else if (subcommand == "proxy") {
                printf("ERROR: Subcommand 'proxy' is no longer supported in reql-on-fdb.\n");  // TODO: Product name
                return 1;
            } else if (subcommand == "export") {
                return main_rethinkdb_export(argc, argv);
            } else if (subcommand == "import") {
                return main_rethinkdb_import(argc, argv);
            } else if (subcommand == "dump") {
                return main_rethinkdb_dump(argc, argv);
            } else if (subcommand == "restore") {
                return main_rethinkdb_restore(argc, argv);
            } else if (subcommand == "index-rebuild") {
                return main_rethinkdb_index_rebuild(argc, argv);
            } else if (subcommand == "repl") {
                return main_rethinkdb_repl(argc, argv);
    #ifdef _WIN32
            } else if (subcommand == "run-service") {
                return main_rethinkdb_run_service(argc, argv);
            } else if (subcommand == "install-service") {
                return main_rethinkdb_install_service(argc, argv);
            } else if (subcommand == "remove-service") {
                return main_rethinkdb_remove_service(argc, argv);
    #endif /* _WIN32 */
            } else {
                printf("ERROR: Unrecognized subcommand '%s'. Try 'rethinkdb help'.\n", subcommand.c_str());
                return 1;
            }
        }
    }
}
